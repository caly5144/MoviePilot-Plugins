import base64
import json
import re
import threading
import time
import pickle
from datetime import datetime, timedelta
from typing import Optional, Any, List, Dict, Tuple
from app.core.cache import FileCache
from pathlib import Path

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dateutil.parser import isoparse
from requests import RequestException
from sqlalchemy.orm import Session
from zhconv import zhconv

from app import schemas
from app.chain.tmdb import TmdbChain
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.db import db_query
from app.db.models import Subscribe
from app.db.models.subscribehistory import SubscribeHistory
from app.db.transferhistory_oper import TransferHistoryOper
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.modules.themoviedb import TmdbApi
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType
from app.utils.common import retry
from app.utils.http import RequestUtils
from app.utils.string import StringUtils


class EmbyMetaRefreshCustom(_PluginBase):
    # 插件名称
    plugin_name = "Emby元数据刷新(修改版)"
    # 插件描述
    plugin_desc = "定时刷新Emby媒体库元数据，演职人员中文。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/caly5144/MoviePilot-Plugins/main/icons/Emby_A.png"
    # 插件版本
    plugin_version = "2.4.4"
    # 插件作者
    plugin_author = "caly5144"
    # 作者主页
    author_url = "https://github.com/caly5144"
    # 插件配置项ID前缀
    plugin_config_prefix = "embymetarefresh_custom_"
    # 加载顺序
    plugin_order = 15
    # 可使用的用户级别
    auth_level = 1

# 退出事件
    _event = threading.Event()
    # 私有属性
    _enabled = False
    tmdbchain = None
    tmdbapi = None
    _onlyonce = False
    _exclusiveExtract = False
    _cron = None
    _actor_chi = False
    _num = None
    _refresh_type = None
    _ReplaceAllMetadata = "true"
    _ReplaceAllImages = "true"
    _actor_path = None
    _mediaservers = None
    _interval = None
    mediaserver_helper = None
    _EMBY_HOST = None
    _EMBY_USER = None
    _EMBY_APIKEY = None
    _scheduler: Optional[BackgroundScheduler] = None
    _tmdb_cache = None
    _episodes_images = []
    _region_name = "embymetarefresh_custom_cache"

    def init_plugin(self, config: dict = None):
        # 停止现有任务
        self.stop_service()
        self.tmdbchain = TmdbChain()
        self.tmdbapi = TmdbApi()
        self.mediaserver_helper = MediaServerHelper()
        # 创建缓存实例
        self._tmdb_cache = FileCache(
            base=Path(f"/tmp/{self._region_name}"),
            ttl=604800
        )

        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            self._actor_chi = config.get("actor_chi")
            self._exclusiveExtract = config.get("exclusiveExtract")
            self._num = config.get("num") or 5
            self._actor_path = config.get("actor_path")
            self._refresh_type = config.get("refresh_type") or "历史记录"
            self._ReplaceAllMetadata = config.get("ReplaceAllMetadata") or "true"
            self._ReplaceAllImages = config.get("ReplaceAllImages") or "true"
            self._mediaservers = config.get("mediaservers") or []
            self._interval = config.get("interval") or 5

            self._episodes_images = self.get_data("episodes_images") or []

            # 加载模块
            if self._enabled or self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)

                if self._onlyonce:
                    logger.info(f"媒体库元数据刷新服务(修改版)启动，立即运行一次")
                    self._scheduler.add_job(self.refresh, 'date',
                                            run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                            name="媒体库元数据")
                    self._onlyonce = False
                    self.__update_config()

                if self._cron:
                    try:
                        self._scheduler.add_job(func=self.refresh,
                                                trigger=CronTrigger.from_crontab(self._cron),
                                                name="媒体库元数据")
                    except Exception as err:
                        logger.error(f"定时任务配置错误：{str(err)}")
                        self.systemmessage.put(f"执行周期配置错误：{err}")

                if self._scheduler.get_jobs():
                    self._scheduler.print_jobs()
                    self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    def __update_config(self):
        self.update_config(
            {
                "onlyonce": self._onlyonce,
                "cron": self._cron,
                "enabled": self._enabled,
                "actor_chi": self._actor_chi,
                "num": self._num,
                "refresh_type": self._refresh_type,
                "ReplaceAllMetadata": self._ReplaceAllMetadata,
                "ReplaceAllImages": self._ReplaceAllImages,
                "actor_path": self._actor_path,
                "mediaservers": self._mediaservers,
                "interval": self._interval,
                "exclusiveExtract": self._exclusiveExtract,
            }
        )

    def refresh(self):
        """
        刷新媒体库元数据
        """
        emby_servers = self.mediaserver_helper.get_services(name_filters=self._mediaservers, type_filter="emby")
        if not emby_servers:
            logger.error("未配置Emby媒体服务器")
            return

        for emby_name, emby_server in emby_servers.items():
            logger.info(f"开始刷新媒体服务器 {emby_name} 的媒体库元数据")
            emby = emby_server.instance
            if not emby:
                logger.error(f"Emby媒体服务器 {emby_name} 未连接")
                continue
            self._EMBY_USER = emby_server.instance.get_user()
            self._EMBY_APIKEY = emby_server.config.config.get("apikey")
            self._EMBY_HOST = emby_server.config.config.get("host")
            if not self._EMBY_HOST.endswith("/"):
                self._EMBY_HOST += "/"
            if not self._EMBY_HOST.startswith("http"):
                self._EMBY_HOST = "http://" + self._EMBY_HOST

            plugin_config, plugin_id = None, None
            if self._exclusiveExtract == "true":
                try:
                    plugin_config, plugin_id = self.__get_strm_assistant_config()
                    if plugin_id:
                        flag = self.__set_strm_assistant_exclusive_mode(emby, plugin_config, plugin_id, True)
                        if flag:
                            logger.info(f"神医助手 独占模式已打开")
                except Exception as e:
                    logger.error(f"获取 神医助手 配置失败：{str(e)}")

            if str(self._refresh_type) == "历史记录":
                self._ReplaceAllMetadata = "true"
                self._ReplaceAllImages = "true"
                current_date = datetime.now()
                target_date = current_date - timedelta(days=int(self._num))
                transferhistorys = TransferHistoryOper().list_by_date(target_date.strftime('%Y-%m-%d'))
                if not transferhistorys:
                    logger.error(f"{self._num}天内没有媒体库入库记录")
                    self.__close_exclusive_mode(emby, plugin_config, plugin_id)
                    return

                logger.info(f"开始刷新媒体库元数据，最近 {self._num} 天内入库媒体：{len(transferhistorys)}个")
                
                # 历史记录的豆瓣演员缓存字典
                history_cache = {}
                for transferinfo in transferhistorys:
                    self.__refresh_emby(transferinfo, emby, cache_dict=history_cache)
                    if self._interval:
                        time.sleep(int(self._interval))
            else:
                latest = self.__get_latest_media()
                if not latest:
                    logger.error(f"Emby中没有最新媒体")
                    self.__close_exclusive_mode(emby, plugin_config, plugin_id)
                    return

                logger.info(f"开始刷新媒体库元数据，{self._num} 天内最新媒体：{len(latest)} 个")

                # 最新入库的豆瓣演员缓存字典（防止单集重复请求）
                handle_items_cache = {}
                processed_series = set()

                for item in latest:
                    try:
                        refresh_meta = self._ReplaceAllMetadata
                        refresh_image = self._ReplaceAllImages
                        
                        is_episode = str(item.get('Type')) == 'Episode'
                        item_name_display = '%s S%02dE%02d %s' % (item.get('SeriesName'), item.get('ParentIndexNumber'), item.get('IndexNumber'), item.get('Name')) if is_episode else item.get('Name')
                        
                        if self._ReplaceAllMetadata == "auto":
                            refresh_meta = "false"
                            if (is_episode and (self.__contains_episode(item.get("Name")) or not item.get("Overview") or not self.__contains_chinese(item.get("Overview")))):
                                refresh_meta = "true"
                                
                        if self._ReplaceAllImages == "auto":
                            refresh_image = "false"
                            if is_episode:
                                if item.get("Id") in self._episodes_images:
                                    logger.info(f"最新媒体：电视剧 {item_name_display} {item.get('Id')} 封面无需更新")
                                else:
                                    key = f"{item.get('Type')}-{item.get('SeriesName')}-{str(item.get('ProductionYear'))}"
                                    tv_info = self._tmdb_cache.get(key, region=self._region_name)

                                    if not tv_info:
                                        tmdb_id = self.__get_subscribe_by_name(db=None, name=item.get('SeriesName'))
                                        if tmdb_id:
                                            tv_info = self.tmdbapi.get_info(tmdbid=tmdb_id, mtype=MediaType.TV)
                                        if not tv_info:
                                            tv_info = self.tmdbapi.match(name=item.get('SeriesName'), mtype=MediaType.TV, year=str(item.get('ProductionYear')))
                                    
                                    if tv_info:
                                        if isinstance(tv_info, bytes):
                                            tv_info = pickle.loads(tv_info)
                                        self._tmdb_cache.set(key, pickle.dumps(tv_info), region=self._region_name)
                                        episode_info = TmdbApi().get_tv_episode_detail(tv_info["id"], item.get('ParentIndexNumber'), item.get('IndexNumber'))
                                        if episode_info and episode_info.get("still_path"):
                                            flag = self.__update_item_image(item_id=item.get("Id"), image_url=f"https://image.tmdb.org/t/p/original{episode_info.get('still_path')}")
                                            if flag:
                                                refresh_image = "false"
                                                self._episodes_images.append(item.get("Id"))
                                                self.save_data("episodes_images", self._episodes_images)
                                            logger.info(f"最新媒体：电视剧 {item_name_display} {item.get('Id')} 封面更新 {flag}")

                        if refresh_meta == "true" or refresh_image == "true":
                            logger.info(f"开始刷新媒体库元数据，最新媒体：{'电视剧' if is_episode else '电影'} {item_name_display} {item.get('Id')}")
                            self.__refresh_emby_library_by_id(item_id=item.get("Id"), refresh_meta=refresh_meta, refresh_image=refresh_image)
                            if self._interval:
                                time.sleep(int(self._interval))
                        else:
                            logger.info(f"最新媒体：{'电视剧' if is_episode else '电影'} {item_name_display} {item.get('Id')} 元数据完整，跳过处理")

                        # 刮演员中文 (完全精准处理单集，不再串台)
                        if self._actor_chi:
                            logger.info(f"最新媒体：{'电视剧' if is_episode else '电影'} {item_name_display} {item.get('Id')} 开始处理演员中文名")
                            
                            mtype = MediaType.TV if is_episode else MediaType.MOVIE
                            season = item.get("ParentIndexNumber") if is_episode else None
                            title = item.get('SeriesName') if is_episode else item.get('Name')
                            item_id = item.get("Id")
                            
                            # 👇 核心修改 1：如果是电视剧，必须把“剧集主干（Series）”也顺带处理一遍
                            if is_episode:
                                series_id = item.get("SeriesId")
                                # 如果这个剧集主干还没处理过，就处理一下
                                if series_id and series_id not in processed_series:
                                    logger.info(f"附加任务：顺带处理剧集《{title}》主页的演员信息 ({series_id})")
                                    self.__update_people_chi(
                                        item_id=series_id,
                                        title=title,
                                        type=mtype,
                                        season=None, # 剧集主干不传 season
                                        emby=emby,
                                        cache_dict=handle_items_cache
                                    )
                                    processed_series.add(series_id) # 标记为已处理，防止后续集数重复处理主干
                            
                            # 👇 核心修改 2：继续处理当前的单集（Episode）或电影（Movie）
                            self.__update_people_chi(
                                item_id=item_id,
                                title=title,
                                type=mtype,
                                season=season,
                                emby=emby,
                                cache_dict=handle_items_cache
                            )
                    except Exception as e:
                        logger.error(f"刷新媒体库元数据失败：{str(e)}")
                        continue

            self.__close_exclusive_mode(emby, plugin_config, plugin_id)
            logger.info(f"刷新 {emby_name} 媒体库元数据完成")

    def __close_exclusive_mode(self, emby, plugin_config, plugin_id):
        if self._exclusiveExtract == "true":
            try:
                if plugin_id:
                    flag = self.__set_strm_assistant_exclusive_mode(emby, plugin_config, plugin_id, False)
                    if flag:
                        logger.info(f"神医助手 独占模式已关闭")
            except Exception as e:
                logger.error(f"关闭 神医助手 独占模式失败：{str(e)}")

    @staticmethod
    def __contains_chinese(text: str) -> bool:
        pattern = re.compile(r'[\u4e00-\u9fa5]')
        return bool(pattern.search(text))

    @staticmethod
    def __contains_episode(text: str) -> bool:
        pattern = re.compile(r'第\s*([0-9]|[十|一|二|三|四|五|六|七|八|九|零])+\s*集')
        return bool(pattern.search(text))

    def __get_latest_media(self) -> List[dict]:
        refresh_date = datetime.utcnow() - timedelta(days=int(self._num))
        refresh_date = refresh_date.replace(tzinfo=pytz.utc)
        try:
            latest_medias = self.__get_latest(limit=1000)
            if not latest_medias:
                return []

            _latest_medias = []
            for media in latest_medias:
                media_date = isoparse(media.get("DateCreated"))
                if media_date > refresh_date:
                    _latest_medias.append(media)
                else:
                    break
            return _latest_medias
        except Exception as err:
            logger.error(f"获取Emby中最新媒体失败：{str(err)}")
            return []

    def __update_people_chi(self, item_id, title, type, season=None, emby=None, cache_dict=None):
        """
        刮削演员中文名 (带缓存版本)
        """
        item_info = self.__get_item_info(item_id)
        if item_info:
            if self._actor_path and not any(str(actor_path) in item_info.get("Path", "") for actor_path in self._actor_path.split(",")):
                return None

            if self.__need_trans_actor(item_info):
                # 以 媒体类型-剧名 作为缓存Key，确保同剧集只请求一次豆瓣
                cache_key = f"{type.value}-{title}"
                douban_actors = []
                
                if cache_dict is not None and cache_key in cache_dict:
                    douban_actors = cache_dict[cache_key]
                else:
                    imdb_id = item_info.get("ProviderIds", {}).get("Imdb")
                    logger.info(f"开始获取 {title} ({item_info.get('ProductionYear')}) 的豆瓣/TMDB演员信息 ...")
                    douban_actors = self.__get_douban_actors(title=title, imdb_id=imdb_id, type=type, year=item_info.get("ProductionYear"), season=season)
                    if cache_dict is not None:
                        cache_dict[cache_key] = douban_actors or []

                peoples = self.__update_peoples(itemid=item_id, iteminfo=item_info, douban_actors=douban_actors, emby=emby)
                return peoples
            else:
                logger.info(f"媒体 {title} ({item_info.get('ProductionYear', '')}) 演员信息无需更新")
        return None

    def __update_peoples(self, itemid: str, iteminfo: dict, douban_actors, emby):
        peoples = []
        need_update_people = False
        
        for people in iteminfo.get("People", []) or []:
            if self._event.is_set():
                logger.info(f"演职人员刮削服务停止")
                return
            if not people.get("Name"):
                continue
                
            name = people.get("Name") or ""
            role = people.get("Role") or ""
            has_space = " " in name or "　" in name or "・" in name
            has_kana = bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF]', name))

            # 精准跳过：姓名角色是中文 + 无空格中圆点 + 无日文假名 + 已有头像
            if not has_space and not has_kana \
                    and StringUtils.is_chinese(name) \
                    and StringUtils.is_chinese(role) \
                    and people.get("PrimaryImageTag"):
                peoples.append(people)
                continue
                
            info = self.__update_people(people=people, douban_actors=douban_actors, emby=emby)
            if info:
                logger.info(f"更新演职人员 {people.get('Name')} ({people.get('Role')}) 信息：{info.get('Name')} ({info.get('Role')})")
                need_update_people = True
                peoples.append(info)
            else:
                peoples.append(people)

        item_name = f"{iteminfo.get('Name')} ({iteminfo.get('ProductionYear')})" if iteminfo.get('Type') in ['Series', 'Movie'] else f"{iteminfo.get('SeriesName')} ({iteminfo.get('ProductionYear')}) {iteminfo.get('SeasonName')} {iteminfo.get('Name')}"
        
        # 保存媒体项信息
        if peoples and need_update_people:
            iteminfo["People"] = peoples
            if "LockedFields" not in iteminfo:
                iteminfo["LockedFields"] = []
            if "Cast" not in iteminfo["LockedFields"]:
                iteminfo["LockedFields"].append("Cast")
            flag = self.set_iteminfo(itemid=itemid, iteminfo=iteminfo, emby=emby)
            logger.info(f"更新媒体 {item_name} 演员信息完成 {flag}")
        else:
            logger.info(f"媒体 {item_name} 演员信息已完成处理或无需修改文本属性")

        return iteminfo.get("People", [])

    def __update_people(self, people: dict, douban_actors: list = None, emby=None) -> Optional[dict]:
        def __get_emby_iteminfo() -> dict:
            try:
                url = f'[HOST]emby/Users/[USER]/Items/{people.get("Id")}?Fields=ChannelMappingInfo&api_key=[APIKEY]'
                res = emby.get_data(url=url)
                if res:
                    return res.json()
            except Exception as err:
                logger.error(f"获取Emby媒体项详情失败：{str(err)}")
            return {}

        def __get_peopleid(p: dict) -> Tuple[Optional[str], Optional[str]]:
            if not p.get("ProviderIds"):
                return None, None
            peopletmdbid, peopleimdbid = None, None
            if "Tmdb" in p["ProviderIds"]: peopletmdbid = p["ProviderIds"]["Tmdb"]
            if "tmdb" in p["ProviderIds"]: peopletmdbid = p["ProviderIds"]["tmdb"]
            if "Imdb" in p["ProviderIds"]: peopleimdbid = p["ProviderIds"]["Imdb"]
            if "imdb" in p["ProviderIds"]: peopleimdbid = p["ProviderIds"]["imdb"]
            return peopletmdbid, peopleimdbid

        # 返回的人物信息 - 使用浅拷贝替代深拷贝以减少内存使用
        ret_people = people.copy()
        for key, value in people.items():
            if isinstance(value, dict):
                ret_people[key] = value.copy()
            elif isinstance(value, list):
                ret_people[key] = value.copy()

        # 核心预处理工具：清理所有空白符、中圆点，并转为简体
        def clean_name(name_str):
            if not name_str: return ""
            s = re.sub(r'[\s・·]', '', name_str).lower()
            return zhconv.convert(s, "zh-hans")

        try:
            # 查询媒体库人物详情
            personinfo = __get_emby_iteminfo()
            if not personinfo:
                logger.warn(f"未找到人物 {people.get('Name')} 的信息")
                return None

            updated_name = False
            updated_overview = False
            update_character = False
            profile_path = None

            emby_name = people.get("Name") or ""
            emby_name_clean = clean_name(emby_name)
            # 提取名字中的汉字，用于模糊重叠匹配
            emby_kanji = "".join(re.findall(r'[\u4e00-\u9fa5]', emby_name))
            
            tmdb_name_clean = ""

            # 1. 从TMDB信息中更新人物信息
            person_tmdbid, person_imdbid = __get_peopleid(personinfo)
            if person_tmdbid:
                person_detail = self.tmdbchain.person_detail(int(person_tmdbid))
                if person_detail:
                    tmdb_name_clean = clean_name(person_detail.name)
                    cn_name = self.__get_chinese_name(person_detail)
                    profile_path = person_detail.profile_path
                    if profile_path:
                        profile_path = f"https://{settings.TMDB_IMAGE_DOMAIN}/t/p/original{profile_path}"
                    if cn_name:
                        personinfo["Name"] = cn_name
                        ret_people["Name"] = cn_name
                        updated_name = True
                    biography = person_detail.biography
                    if biography and StringUtils.is_chinese(biography):
                        personinfo["Overview"] = biography
                        updated_overview = True

            # 2. 从豆瓣信息中更新人物信息
            if douban_actors:
                matched_douban_actor = None
                
                for douban_actor in douban_actors:
                    db_name = douban_actor.get("name") or ""
                    db_latin = douban_actor.get("latin_name") or ""
                    db_name_clean = clean_name(db_name)
                    db_latin_clean = clean_name(db_latin)
                    
                    # 规则A：强力清洗后相等 (解决所有空格和 亜/亚 繁简问题)
                    if (emby_name_clean and emby_name_clean in [db_name_clean, db_latin_clean]) or \
                       (tmdb_name_clean and tmdb_name_clean in [db_name_clean, db_latin_clean]):
                        matched_douban_actor = douban_actor
                        break
                        
                    # 规则B：汉字重叠模糊匹配 (解决 高石あかり vs 高石明里)
                    if len(emby_kanji) >= 2 and emby_kanji in db_name:
                        matched_douban_actor = douban_actor
                        break

                if matched_douban_actor:
                    db_name = matched_douban_actor.get("name")
                    current_name = personinfo.get("Name") or ""
                    
                    # 关键修改：如果现在的名字包含空格、中圆点或【日文假名】，强制用豆瓣纯中文名覆盖！
                    has_dirty_chars = bool(re.search(r'[\s・·\u3040-\u309F\u30A0-\u30FF]', current_name))
                    
                    if not updated_name or has_dirty_chars:
                        logger.info(f"{emby_name} 匹配成功，从豆瓣获取到纯净中文名：{db_name}")
                        personinfo["Name"] = db_name
                        ret_people["Name"] = db_name
                        updated_name = True
                    
                    if not updated_overview and matched_douban_actor.get("title"):
                        personinfo["Overview"] = matched_douban_actor.get("title")
                        updated_overview = True
                        
                    if not update_character and matched_douban_actor.get("character"):
                        character = re.sub(r"饰\s+", "", matched_douban_actor.get("character"))
                        character = re.sub("演员|voice|Director|配音|导演", "", character)
                        if character:
                            ret_people["Role"] = character
                            update_character = True
                            
                    if not profile_path:
                        avatar = matched_douban_actor.get("avatar") or {}
                        if avatar.get("large"):
                            profile_path = avatar.get("large")

            # 3. 终极保底：如果全网没匹配上，但名字里有空格/中圆点，强制本地清理！
            if not updated_name and re.search(r'[\s・·]', emby_name):
                cleaned_fallback = re.sub(r'[\s・·]', '', emby_name)
                personinfo["Name"] = cleaned_fallback
                ret_people["Name"] = cleaned_fallback
                updated_name = True
                logger.info(f"API未匹配，执行本地强力去空格：{emby_name} -> {cleaned_fallback}")

            # 4. 更新人物图片
            if profile_path and not people.get("PrimaryImageTag"):
                self.set_item_image(itemid=people.get("Id"), imageurl=profile_path, emby=emby)

            # 5. 锁定人物信息
            if updated_name:
                if "LockedFields" not in personinfo: personinfo["LockedFields"] = []
                if "Name" not in personinfo["LockedFields"]: personinfo["LockedFields"].append("Name")
            if updated_overview:
                if "LockedFields" not in personinfo: personinfo["LockedFields"] = []
                if "Overview" not in personinfo["LockedFields"]: personinfo["LockedFields"].append("Overview")

            # 6. 保存到 Emby
            if updated_name or updated_overview or update_character:
                ret = self.set_iteminfo(itemid=people.get("Id"), iteminfo=personinfo, emby=emby)
                if ret:
                    return ret_people
            return None
        except Exception as err:
            logger.error(f"更新人物信息失败：{str(err)}")
            return None

    def __get_strm_assistant_config(self):
        list_plugins = self.__get_plugins()
        if not list_plugins: return None, None
        plugin_id = None
        for plugin in list_plugins:
            if plugin.get("DisplayName") == "神医助手":
                plugin_id = plugin.get("PluginId")
                break
        if not plugin_id: return None, None
        plugin_id = f"{plugin_id[:6]}:MediaInfoExtractPageView"
        plugin_info = self.__get_plugin_info(plugin_id)
        if not plugin_info: return None, None
        return plugin_info.get("EditObjectContainer", {}).get("Object"), plugin_id

    def __set_strm_assistant_exclusive_mode(self, emby, plugin_config, plugin_id, exclusive_mode: bool):
        plugin_config["exclusiveExtract"] = exclusive_mode
        plugin_config["ExclusiveControlList"] = [
            {"Value": "IgnoreFileChange", "Name": "忽略文件变更", "IsEnabled": False},
            {"Value": "CatchAllAllow", "Name": "尽可能全放行", "IsEnabled": False},
            {"Value": "CatchAllBlock", "Name": "尽可能全阻止", "IsEnabled": True}
        ]
        data = {
            "ClientLocale": "zh-cn", "CommandId": "PageSave", "ItemId": "undefined",
            "PageId": plugin_id, "Data": json.dumps(plugin_config, ensure_ascii=False)
        }
        try:
            res = emby.post_data(url=f"[HOST]emby/UI/Command?reqformat=json&api_key=[APIKEY]", data=json.dumps(data), headers={"Content-Type": "text/plain"})
            if res and res.status_code in [200, 204]: return True
        except Exception as err:
            logger.error(f"设置神医助手独占模式失败：{str(err)}")
        return False

    def __get_plugins(self) -> list:
        if not self._EMBY_HOST or not self._EMBY_APIKEY: return []
        req_url = f"%semby/web/configurationpages?PageType=PluginConfiguration&EnableInMainMenu=true&api_key=%s" % (self._EMBY_HOST, self._EMBY_APIKEY)
        with RequestUtils().get_res(req_url) as res:
            return res.json() if res else []

    def __get_plugin_info(self, plugin_id) -> dict:
        if not self._EMBY_HOST or not self._EMBY_APIKEY: return {}
        req_url = f"%semby/UI/View?PageId=%s&api_key=%s" % (self._EMBY_HOST, plugin_id, self._EMBY_APIKEY)
        with RequestUtils().get_res(req_url) as res:
            return res.json() if res else {}

    def __update_item_image(self, item_id, image_url) -> bool:
        if not self._EMBY_HOST or not self._EMBY_APIKEY: return False
        req_url = f"%semby/Items/%s/Images/Primary/0/Url?reqformat=json&api_key=%s" % (self._EMBY_HOST, item_id, self._EMBY_APIKEY)
        try:
            with RequestUtils().post_res(url=req_url, data={"Url": image_url}) as res:
                if res and res.status_code in [200, 204]: return True
        except Exception as err:
            logger.error(f"更新媒体项图片失败：{str(err)}")
        return False

    @staticmethod
    def set_iteminfo(itemid: str, iteminfo: dict, emby):
        try:
            res = emby.post_data(url=f'[HOST]emby/Items/{itemid}?api_key=[APIKEY]&reqformat=json', data=json.dumps(iteminfo), headers={"Content-Type": "application/json"})
            if res and res.status_code in [200, 204]: return True
        except Exception as err:
            logger.error(f"更新Emby媒体项详情失败：{str(err)}")
        return False

    @staticmethod
    @retry(RequestException, logger=logger)
    def set_item_image(itemid: str, imageurl: str, emby):
        def __download_image():
            try:
                if "doubanio.com" in imageurl:
                    r = RequestUtils(headers={'Referer': "https://movie.douban.com/"}, ua=settings.USER_AGENT).get_res(url=imageurl, raise_exception=True)
                else:
                    r = RequestUtils(proxies=settings.PROXY).get_res(url=imageurl, raise_exception=True)
                if r: return base64.b64encode(r.content).decode()
            except Exception as err:
                logger.error(f"下载图片失败：{str(err)}")
            return None

        def __set_emby_item_image(_base64: str):
            try:
                res = emby.post_data(url=f'[HOST]emby/Items/{itemid}/Images/Primary?api_key=[APIKEY]', data=_base64, headers={"Content-Type": "image/png"})
                if res and res.status_code in [200, 204]: return True
            except Exception as result:
                logger.error(f"更新Emby媒体项图片失败：{result}")
            return False

        image_base64 = __download_image()
        if image_base64: return __set_emby_item_image(image_base64)
        return None

    @staticmethod
    def __get_chinese_name(personinfo: schemas.MediaPerson) -> str:
        try:
            for name in personinfo.also_known_as or []:
                if name and StringUtils.is_chinese(name):
                    return zhconv.convert(name, "zh-hans")
        except Exception as err:
            logger.error(f"获取人物中文名失败：{err}")
        return ""

    def __get_douban_actors(self, title, imdb_id, type, year, season: int = None) -> List[dict]:
        sleep_time = 3 + int(time.time()) % 7
        time.sleep(sleep_time)
        doubaninfo = self.chain.match_doubaninfo(name=title, imdbid=imdb_id, mtype=type, year=year, season=season)
        if doubaninfo:
            doubanitem = self.chain.douban_info(doubaninfo.get("id")) or {}
            return (doubanitem.get("actors") or []) + (doubanitem.get("directors") or [])
        return []

    @staticmethod
    def __need_trans_actor(item):
        """
        含非中文名、非中文角色、无头像、含空格/中圆点、含日文假名，均返回 True
        """
        peoples = item.get("People") or []
        if not peoples:
            return True
        for x in peoples:
            name = x.get("Name") or ""
            role = x.get("Role") or ""
            
            # 检测是否包含空格(半角/全角)、中圆点
            has_space = " " in name or "　" in name or "・" in name
            # 检测是否包含日文平假名、片假名
            has_kana = bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF]', name))
            
            if has_space or has_kana \
               or (name and not StringUtils.is_chinese(name)) \
               or (role and not StringUtils.is_chinese(role)) \
               or not x.get("PrimaryImageTag"):
                return True
        return False

    def __get_item_info(self, item_id):
        res = RequestUtils().get_res(f"{self._EMBY_HOST}/emby/Users/{self._EMBY_USER}/Items/{item_id}?api_key={self._EMBY_APIKEY}")
        if res and res.status_code == 200:
            return res.json()
        return {}

    @eventmanager.register(EventType.PluginAction)
    def remote_sync(self, event: Event):
        if event:
            event_data = event.event_data
            if not event_data or event_data.get("action") != "emby_meta_refresh_custom":
                return
            self.post_message(channel=event.event_data.get("channel"), title="开始刷新Emby元数据 ...", userid=event.event_data.get("user"))
        self.refresh()
        if event:
            self.post_message(channel=event.event_data.get("channel"), title="刷新Emby元数据完成！", userid=event.event_data.get("user"))

    def __refresh_emby(self, transferinfo, emby, cache_dict=None):
        try:
            if transferinfo.type == "电影":
                movies = emby.get_movies(title=transferinfo.title, year=transferinfo.year)
                if not movies: return
                for movie in movies:
                    self.__refresh_emby_library_by_id(item_id=movie.item_id)
                    if self._actor_chi:
                        self.__update_people_chi(item_id=movie.item_id, title=movie.title, type=MediaType.MOVIE, emby=emby, cache_dict=cache_dict)
            else:
                item_id = self.__get_emby_series_id_by_name(name=transferinfo.title, year=transferinfo.year)
                if not item_id: return

                item_info = emby.get_iteminfo(item_id)
                if item_info and transferinfo.tmdbid and item_info.tmdbid:
                    if str(transferinfo.tmdbid) != str(item_info.tmdbid): return

                season = int(transferinfo.seasons.replace("S", ""))
                episode = int(transferinfo.episodes.replace("E", "")) if "-" not in transferinfo.episodes else int(transferinfo.episodes.replace("E", "").split("-")[0])
                episode_item_id = self.__get_emby_episode_item_id(item_id=item_id, season=season, episode=episode)
                if not episode_item_id: return

                self.__refresh_emby_library_by_id(item_id=episode_item_id)
                if self._actor_chi:
                    self.__update_people_chi(item_id=item_id, title=transferinfo.title, type=MediaType.TV, season=None, emby=emby, cache_dict=cache_dict)
                    self.__update_people_chi(item_id=episode_item_id, title=transferinfo.title, type=MediaType.TV, season=season, emby=emby, cache_dict=cache_dict)
        except Exception as e:
            logger.error(f"刷新Emby出错：{e}")

    def __get_emby_episode_item_id(self, item_id: str, season: int, episode: int) -> Optional[str]:
        if not self._EMBY_HOST or not self._EMBY_APIKEY: return None
        req_url = "%semby/Shows/%s/Episodes?Season=%s&IsMissing=false&api_key=%s" % (self._EMBY_HOST, item_id, season, self._EMBY_APIKEY)
        try:
            with RequestUtils().get_res(req_url) as res_json:
                if res_json:
                    for res_item in res_json.json().get("Items", []):
                        if season and season != res_item.get("ParentIndexNumber"): continue
                        if episode and episode != res_item.get("IndexNumber"): continue
                        return res_item.get("Id")
        except Exception as e:
            logger.error(f"连接Shows/Id/Episodes出错：" + str(e))
        return None

    def __refresh_emby_library_by_id(self, item_id: str, refresh_meta: str = None, refresh_image: str = None) -> bool:
        if not self._EMBY_HOST or not self._EMBY_APIKEY: return False
        req_url = "%semby/Items/%s/Refresh?Recursive=true&MetadataRefreshMode=FullRefresh&ImageRefreshMode=FullRefresh&ReplaceAllMetadata=%s&ReplaceAllImages=%s&api_key=%s" % (
            self._EMBY_HOST, item_id, refresh_meta or self._ReplaceAllMetadata, refresh_image or self._ReplaceAllImages, self._EMBY_APIKEY)
        try:
            with RequestUtils().post_res(req_url) as res:
                if res: return True
        except Exception as e:
            logger.error(f"连接Items/Id/Refresh出错：" + str(e))
        return False

    def __get_latest(self, limit) -> list:
        if not self._EMBY_HOST or not self._EMBY_APIKEY: return []
        req_url = "%semby/Users/%s/Items?Limit=%s&api_key=%s&SortBy=DateCreated,SortName&SortOrder=Descending&IncludeItemTypes=Episode,Movie&Recursive=true&Fields=DateCreated,Overview,PrimaryImageAspectRatio,ProductionYear" % (
            self._EMBY_HOST, self._EMBY_USER, limit, self._EMBY_APIKEY)
        try:
            with RequestUtils().get_res(req_url) as res:
                return res.json().get("Items") if res else []
        except Exception as e:
            logger.error(f"连接Items出错：" + str(e))
        return []

    def __get_emby_series_id_by_name(self, name: str, year: str) -> Optional[str]:
        if not self._EMBY_HOST or not self._EMBY_APIKEY: return None
        req_url = ("%semby/Items?IncludeItemTypes=Series&Fields=ProductionYear&StartIndex=0&Recursive=true&SearchTerm=%s&Limit=10&IncludeSearchTypes=false&api_key=%s") % (self._EMBY_HOST, name, self._EMBY_APIKEY)
        try:
            with RequestUtils().get_res(req_url) as res:
                if res:
                    for res_item in res.json().get("Items", []):
                        if res_item.get('Name') == name and (not year or str(res_item.get('ProductionYear')) == str(year)):
                            return res_item.get('Id')
        except Exception as e:
            logger.error(f"连接Items出错：" + str(e))
        return ""

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{
            "cmd": "/emby_meta_refresh_custom",
            "event": EventType.PluginAction,
            "desc": "Emby媒体库刷新(修改版)",
            "category": "",
            "data": {
                "action": "emby_meta_refresh_custom"
            }
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'actor_chi', 'label': '刮削演员中文'}}]
                            },
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VCronField', 'props': {'model': 'cron', 'label': '执行周期', 'placeholder': '5位cron表达式，留空自动'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSelect', 'props': {'model': 'refresh_type', 'label': '刷新方式', 'items': [{'title': '历史记录', 'value': '历史记录'}, {'title': '最新入库', 'value': '最新入库'}]}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'num', 'label': '最新入库天数/历史记录天数'}}]
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSelect', 'props': {'model': 'ReplaceAllImages', 'label': '覆盖图片', 'items': [{'title': 'true', 'value': "true"}, {'title': 'false', 'value': "false"}, {'title': 'auto', 'value': "auto"}]}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSelect', 'props': {'model': 'ReplaceAllMetadata', 'label': '覆盖元数据', 'items': [{'title': 'true', 'value': "true"}, {'title': 'false', 'value': "false"}, {'title': 'auto', 'value': "auto"}]}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'actor_path', 'label': '演员刮削生效路径关键词', 'placeholder': '留空则全部处理'}}]
                            }
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'interval', 'label': '刷新间隔(秒)', 'placeholder': '留空默认0秒'}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSelect', 'props': {'multiple': True, 'chips': True, 'clearable': True, 'model': 'mediaservers', 'label': '媒体服务器', 'items': [{"title": config.name, "value": config.name} for config in self.mediaserver_helper.get_configs().values() if config.type == "emby"]}}]
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSelect', 'props': {'multiple': False, 'chips': True, 'clearable': True, 'model': 'exclusiveExtract', 'label': '联动独占模式', 'items': [{'title': 'true', 'value': "true"}, {'title': 'false', 'value': "false"}]}}]
                            }
                        ]
                    }
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "actor_chi": False,
            "exclusiveExtract": False,
            "ReplaceAllMetadata": "true",
            "ReplaceAllImages": "true",
            "cron": "5 1 * * *",
            "refresh_type": "历史记录",
            "actor_path": "",
            "mediaservers": [],
            "num": 5,
            "interval": 0,
        }

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))

    @staticmethod
    @db_query
    def __get_subscribe_by_name(db: Optional[Session], name: str) -> int:
        tmdb_id = None
        subscribe = db.query(Subscribe).filter(Subscribe.name == name, Subscribe.type == MediaType.TV.value).first()
        if subscribe:
            tmdb_id = subscribe.tmdbid
        else:
            subscribe_history = db.query(SubscribeHistory).filter(SubscribeHistory.name == name, SubscribeHistory.type == MediaType.TV.value).first()
            if subscribe_history:
                tmdb_id = subscribe_history.tmdbid
        return tmdb_id