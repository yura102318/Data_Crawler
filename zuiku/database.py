"""
数据库模型和操作
已添加字段修改追踪功能
✨ V2: 新增 year/month/day, province/city/county, event_type, is_online等字段
✨ V3: 切换到MySQL数据库
✨ V4: 修复TEXT字段长度问题，改为MEDIUMTEXT
"""
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey, JSON, Float, Boolean
from sqlalchemy.dialects.mysql import MEDIUMTEXT  # ⭐ 新增：MySQL专用类型
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class RaceEvent(Base):
    """赛事主表"""
    __tablename__ = 'race_events'

    id = Column(Integer, primary_key=True)
    event_id = Column(String(50), unique=True, nullable=False, index=True)
    event_url = Column(String(500))
    name = Column(String(200))
    event_date = Column(String(50))

    # ✨ 新增：日期拆分字段
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)

    event_level = Column(String(50))

    # ✨ 新增：赛事类型字段
    event_type = Column(String(50))
    is_online = Column(Boolean, default=False)

    location = Column(String(200))

    # ✨ 新增：地点拆分字段
    province = Column(String(100))
    city = Column(String(100))
    county = Column(String(100))
    is_domestic = Column(Boolean, default=True)

    detailed_address = Column(Text)
    total_scale = Column(String(100))
    registration_fee = Column(String(200))
    organizer = Column(Text)
    host_units = Column(Text)
    co_organizers = Column(Text)
    supporters = Column(Text)
    contact_phone = Column(String(200))
    contact_email = Column(String(200))
    contact_person = Column(String(100))
    registration_deadline = Column(String(100))
    status = Column(String(50))
    description = Column(MEDIUMTEXT)  # ⭐ 修复：改为MEDIUMTEXT

    # ✨ 新增：赛事详情字段
    event_detail = Column(MEDIUMTEXT)  # ⭐ 修复：改为MEDIUMTEXT

    news_content = Column(MEDIUMTEXT)  # ⭐ 修复：改为MEDIUMTEXT
    news_url = Column(String(500))
    raw_html = Column(MEDIUMTEXT)  # ⭐ 修复：改为MEDIUMTEXT

    # 统计字段
    avg_price_per_km = Column(Float)
    min_price_per_km = Column(Float)
    max_price_per_km = Column(Float)

    # 字段修改追踪
    manually_modified_fields = Column(Text, default='[]')
    crawler_version = Column(Integer, default=1)
    manual_version = Column(Integer, default=0)
    crawler_updated_at = Column(DateTime)
    manual_updated_at = Column(DateTime)

    # 时间戳
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 关系
    categories = relationship("RaceCategory", back_populates="race_event", cascade="all, delete-orphan")
    images_analysis = relationship("ImageAnalysis", back_populates="race_event", cascade="all, delete-orphan")
class RaceCategory(Base):
    """赛事组别表"""
    __tablename__ = 'race_categories'

    id = Column(Integer, primary_key=True)
    race_event_id = Column(Integer, ForeignKey('race_events.id'), nullable=False)
    name = Column(String(200))
    distance = Column(String(100))
    distance_numeric = Column(Float)
    fee = Column(String(100))
    price_per_km = Column(Float)
    zaoniao_fee = Column(String(100))
    total_quota = Column(Integer)
    registered_count = Column(Integer)
    start_time = Column(String(50))
    cutoff_time = Column(String(50))
    status = Column(String(50))
    status_text = Column(String(100))

    # ✨ V4: 新增组别报名状态和链接
    registration_status = Column(String(50))  # 组别报名状态：报名中/已截止/NULL
    registration_url = Column(String(500))    # 组别报名链接

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # 关系
    race_event = relationship("RaceEvent", back_populates="categories")


class ImageAnalysis(Base):
    """图片分析表"""
    __tablename__ = 'images_analysis'

    id = Column(Integer, primary_key=True)
    race_event_id = Column(Integer, ForeignKey('race_events.id'), nullable=False)
    image_url = Column(String(500))
    analysis_text = Column(Text)

    created_at = Column(DateTime, default=datetime.now)

    # 关系
    race_event = relationship("RaceEvent", back_populates="images_analysis")


class Database:
    """数据库操作类 - MySQL版本"""

    def __init__(self, db_config: dict = None):
        """
        初始化MySQL数据库连接

        Args:
            db_config: 数据库配置字典
                {
                    'host': 'localhost',
                    'port': 3306,
                    'user': 'root',
                    'password': 'lxzq102318',
                    'database': 'longjing',
                    'charset': 'utf8mb4'
                }
        """
        if db_config is None:
            # 默认MySQL配置
            db_config = {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': 'lxzq102318',  # ⭐⭐⭐ 修改为您的MySQL密码
                'database': 'longjing',
                'charset': 'utf8mb4'
            }

        # 构建MySQL连接URL
        db_url = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            f"?charset={db_config['charset']}"
        )

        logger.info(f"连接MySQL数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")

        self.engine = create_engine(
            db_url,
            pool_pre_ping=True,      # 自动检测连接是否有效
            pool_recycle=3600,       # 1小时回收连接
            pool_size=10,            # 连接池大小
            max_overflow=20,         # 超过pool_size后最多再创建的连接数
            echo=False               # 不打印SQL（调试时可设为True）
        )
        self.Session = sessionmaker(bind=self.engine)

    def init_db(self):
        """初始化数据库表"""
        Base.metadata.create_all(self.engine)
        logger.info("✓ 数据库表初始化完成")

    def save_race_event(self, event_data: dict) -> bool:
        """保存赛事数据"""
        session = self.Session()

        try:
            # 查找是否已存在
            existing = session.query(RaceEvent).filter_by(
                event_id=event_data['event_id']
            ).first()

            if existing:
                # 更新
                self._update_event(session, existing, event_data)
                logger.info(f"✓ 更新赛事: {event_data.get('name')}")
            else:
                # 新增
                self._create_event(session, event_data)
                logger.info(f"✓ 新增赛事: {event_data.get('name')}")

            session.commit()
            return True

        except Exception as e:
            session.rollback()
            logger.error(f"✗ 保存失败: {e}")
            return False
        finally:
            session.close()

    def _create_event(self, session, event_data: dict):
        """创建新赛事"""
        race_event = RaceEvent(
            event_id=event_data['event_id'],
            event_url=event_data.get('event_url'),
            name=event_data.get('name'),
            event_date=event_data.get('event_date'),

            # ✨ 新增：日期拆分
            year=event_data.get('year'),
            month=event_data.get('month'),
            day=event_data.get('day'),

            event_level=event_data.get('event_level'),

            # ✨ 新增：类型字段
            event_type=event_data.get('event_type'),
            is_online=event_data.get('is_online', False),

            location=event_data.get('location'),

            # ✨ 新增：地点拆分
            province=event_data.get('province'),
            city=event_data.get('city'),
            county=event_data.get('county'),
            is_domestic=event_data.get('is_domestic', True),

            detailed_address=event_data.get('detailed_address'),
            total_scale=event_data.get('total_scale'),
            registration_fee=event_data.get('registration_fee'),
            organizer=event_data.get('organizer'),
            host_units=event_data.get('host_units'),
            co_organizers=event_data.get('co_organizers'),
            supporters=event_data.get('supporters'),
            contact_phone=event_data.get('contact_phone'),
            contact_email=event_data.get('contact_email'),
            contact_person=event_data.get('contact_person'),
            registration_deadline=event_data.get('registration_deadline'),
            status=event_data.get('status', 'active'),
            description=event_data.get('description'),

            # ✨ 新增：详情字段
            event_detail=event_data.get('event_detail'),

            news_content=event_data.get('news_content'),
            news_url=event_data.get('news_url'),
            raw_html=event_data.get('raw_html'),
            avg_price_per_km=event_data.get('avg_price_per_km'),
            min_price_per_km=event_data.get('min_price_per_km'),
            max_price_per_km=event_data.get('max_price_per_km'),
            crawler_version=1,
            crawler_updated_at=datetime.now()
        )

        session.add(race_event)
        session.flush()

        # 添加组别
        if event_data.get('race_categories'):
            for cat_data in event_data['race_categories']:
                category = RaceCategory(
                    race_event_id=race_event.id,
                    name=cat_data.get('name'),
                    distance=cat_data.get('distance'),
                    distance_numeric=cat_data.get('distance_numeric'),
                    fee=cat_data.get('fee'),
                    price_per_km=cat_data.get('price_per_km'),
                    zaoniao_fee=cat_data.get('zaoniao_fee'),
                    total_quota=cat_data.get('total_quota'),
                    registered_count=cat_data.get('registered_count'),
                    start_time=cat_data.get('start_time'),
                    cutoff_time=cat_data.get('cutoff_time'),
                    status=cat_data.get('status'),
                    status_text=cat_data.get('status_text'),

                    # ✨ V4: 组别报名状态和链接
                    registration_status=cat_data.get('registration_status'),
                    registration_url=cat_data.get('registration_url')
                )
                session.add(category)

        # 添加图片分析
        if event_data.get('images_analysis'):
            for img_data in event_data['images_analysis']:
                image_analysis = ImageAnalysis(
                    race_event_id=race_event.id,
                    image_url=img_data.get('image_url'),
                    analysis_text=img_data.get('analysis_text')
                )
                session.add(image_analysis)

    def _update_event(self, session, existing: RaceEvent, event_data: dict):
        """更新现有赛事"""
        # 更新基本字段
        existing.event_url = event_data.get('event_url')
        existing.name = event_data.get('name')
        existing.event_date = event_data.get('event_date')

        # ✨ 新增：日期拆分
        existing.year = event_data.get('year')
        existing.month = event_data.get('month')
        existing.day = event_data.get('day')

        existing.event_level = event_data.get('event_level')

        # ✨ 新增：类型字段
        existing.event_type = event_data.get('event_type')
        existing.is_online = event_data.get('is_online', False)

        existing.location = event_data.get('location')

        # ✨ 新增：地点拆分
        existing.province = event_data.get('province')
        existing.city = event_data.get('city')
        existing.county = event_data.get('county')
        existing.is_domestic = event_data.get('is_domestic', True)

        existing.detailed_address = event_data.get('detailed_address')
        existing.total_scale = event_data.get('total_scale')
        existing.registration_fee = event_data.get('registration_fee')
        existing.organizer = event_data.get('organizer')
        existing.host_units = event_data.get('host_units')
        existing.co_organizers = event_data.get('co_organizers')
        existing.supporters = event_data.get('supporters')
        existing.contact_phone = event_data.get('contact_phone')
        existing.contact_email = event_data.get('contact_email')
        existing.contact_person = event_data.get('contact_person')
        existing.registration_deadline = event_data.get('registration_deadline')
        existing.status = event_data.get('status', 'active')
        existing.description = event_data.get('description')

        # ✨ 新增：详情字段
        existing.event_detail = event_data.get('event_detail')

        existing.news_content = event_data.get('news_content')
        existing.news_url = event_data.get('news_url')
        existing.raw_html = event_data.get('raw_html')
        existing.avg_price_per_km = event_data.get('avg_price_per_km')
        existing.min_price_per_km = event_data.get('min_price_per_km')
        existing.max_price_per_km = event_data.get('max_price_per_km')
        existing.crawler_updated_at = datetime.now()

        # 修复：如果 crawler_version 是 None，先初始化为 0
        if existing.crawler_version is None:
            existing.crawler_version = 0
        existing.crawler_version += 1

        # 删除旧组别
        session.query(RaceCategory).filter_by(race_event_id=existing.id).delete()

        # 添加新组别
        if event_data.get('race_categories'):
            for cat_data in event_data['race_categories']:
                category = RaceCategory(
                    race_event_id=existing.id,
                    name=cat_data.get('name'),
                    distance=cat_data.get('distance'),
                    distance_numeric=cat_data.get('distance_numeric'),
                    fee=cat_data.get('fee'),
                    price_per_km=cat_data.get('price_per_km'),
                    zaoniao_fee=cat_data.get('zaoniao_fee'),
                    total_quota=cat_data.get('total_quota'),
                    registered_count=cat_data.get('registered_count'),
                    start_time=cat_data.get('start_time'),
                    cutoff_time=cat_data.get('cutoff_time'),
                    status=cat_data.get('status'),
                    status_text=cat_data.get('status_text'),

                    # ✨ V4: 组别报名状态和链接
                    registration_status=cat_data.get('registration_status'),
                    registration_url=cat_data.get('registration_url')
                )
                session.add(category)

        # 删除旧图片分析
        session.query(ImageAnalysis).filter_by(race_event_id=existing.id).delete()

        # 添加新图片分析
        if event_data.get('images_analysis'):
            for img_data in event_data['images_analysis']:
                image_analysis = ImageAnalysis(
                    race_event_id=existing.id,
                    image_url=img_data.get('image_url'),
                    analysis_text=img_data.get('analysis_text')
                )
                session.add(image_analysis)

    def get_event_by_id(self, event_id: str):
        """根据event_id查询赛事"""
        session = self.Session()
        try:
            return session.query(RaceEvent).filter_by(event_id=event_id).first()
        finally:
            session.close()

    def get_stats(self) -> dict:
        """获取数据库统计"""
        session = self.Session()
        try:
            total = session.query(RaceEvent).count()
            with_news = session.query(RaceEvent).filter(RaceEvent.news_content != None).count()
            with_contact = session.query(RaceEvent).filter(RaceEvent.contact_phone != None).count()

            return {
                'total': total,
                'with_news': with_news,
                'with_contact': with_contact
            }
        finally:
            session.close()