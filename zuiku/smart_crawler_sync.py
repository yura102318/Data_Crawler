"""
智能更新模块 - 保护人工编辑的数据 - 完整修复版
✨ 适配新表结构（race_events + race_categories）
✨ 分别检查两张表的 manually_modified_fields

核心修改：
1. 检查 race_events.manually_modified_fields - 保护赛事字段
2. 检查 race_categories.manually_modified_fields - 保护组别字段
3. 分别处理赛事级别和组别级别的字段保护
"""

import pymysql
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class SmartCrawlerSync:
    """智能同步 - 完整修复版 - 保护两张表的人工编辑"""

    def __init__(self, db_config):
        self.db_config = db_config

    def smart_sync_to_mysql(self, crawler_data: dict) -> bool:
        """
        智能同步到MySQL - 分别保护两张表的人工编辑字段

        核心逻辑：
        1. 查询 race_events 表，检查 race_events.manually_modified_fields
        2. 查询 race_categories 表，检查 race_categories.manually_modified_fields
        3. 只更新未被标记的字段

        Args:
            crawler_data: 爬虫数据（包含event_id, race_categories等）

        Returns:
            bool: 是否成功
        """
        try:
            conn = pymysql.connect(**self.db_config)
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            event_id = crawler_data.get('event_id')

            # ========== 第1步：更新赛事主表 ==========

            # 查询赛事记录
            cursor.execute(
                "SELECT * FROM race_events WHERE event_id = %s",
                (event_id,)
            )
            race_event_record = cursor.fetchone()

            if not race_event_record:
                logger.warning(f"赛事 {event_id} 不存在，跳过同步")
                conn.close()
                return False

            race_event_id = race_event_record['id']

            # ⭐ 更新赛事主表（保护人工修改）
            self._update_race_event_protected(
                cursor, race_event_record, crawler_data
            )

            # ========== 第2步：更新组别表 ==========

            # 获取爬虫数据中的组别
            crawler_categories = crawler_data.get('race_categories', [])

            for crawler_cat in crawler_categories:
                cat_name = crawler_cat.get('name')

                # 1. 查找MySQL中是否已存在组别
                if cat_name:
                    cursor.execute(
                        "SELECT * FROM race_categories WHERE race_event_id=%s AND name=%s",
                        (race_event_id, cat_name)
                    )
                else:
                    cursor.execute(
                        "SELECT * FROM race_categories WHERE race_event_id=%s AND (name IS NULL OR name='')",
                        (race_event_id,)
                    )

                category_record = cursor.fetchone()

                if category_record:
                    # 2. ⭐ 更新组别（保护人工修改）
                    self._update_race_category_protected(
                        cursor, category_record, crawler_cat, event_id, cat_name
                    )
                else:
                    # 3. 新增组别
                    self._insert_race_category(
                        cursor, race_event_id, crawler_data, crawler_cat
                    )
                    logger.info(f"  ✓ 新增组别: {event_id} - {cat_name}")

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"✗ 智能同步失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _update_race_event_protected(self, cursor, db_record, crawler_data):
        """
        更新赛事主表（保护人工修改）

        ⭐ 检查 race_events.manually_modified_fields
        """
        # 获取人工修改的字段列表
        manually_modified = json.loads(db_record.get('manually_modified_fields') or '[]')

        # ⭐ 赛事字段映射（爬虫字段 → 数据库字段）
        event_field_mapping = {
            'event_url': 'event_url',
            'name': 'name',
            'event_date': 'event_date',
            'event_level': 'event_level',
            'location': 'location',
            'detailed_address': 'detailed_address',
            'status': 'status',
            'total_scale': 'total_scale',
            'registration_fee': 'registration_fee',
            'organizer': 'organizer',
            'host_units': 'host_units',
            'co_organizers': 'co_organizers',
            'supporters': 'supporters',
            'contact_phone': 'contact_phone',
            'contact_email': 'contact_email',
            'contact_person': 'contact_person',
            'registration_deadline': 'registration_deadline',
            'description': 'description',
            'event_detail': 'event_detail',
            'news_content': 'news_content',
            'news_url': 'news_url',
            'raw_html': 'raw_html'
        }

        # 准备更新的字段（排除人工修改的）
        update_fields = {}
        protected_count = 0

        for crawler_field, db_field in event_field_mapping.items():
            # 检查是否被人工修改
            if db_field in manually_modified:
                protected_count += 1
                continue

            # 检查爬虫数据中是否有该字段
            if crawler_field in crawler_data and crawler_data[crawler_field]:
                update_fields[db_field] = crawler_data[crawler_field]

        # 执行更新
        if update_fields:
            set_clause = ', '.join([f"{k}=%s" for k in update_fields.keys()])
            set_clause += ", crawler_updated_at=%s"

            values = list(update_fields.values()) + [datetime.now()]

            cursor.execute(
                f"UPDATE race_events SET {set_clause} WHERE id=%s",
                values + [db_record['id']]
            )

            logger.info(f"  ✓ 智能更新赛事: {crawler_data.get('event_id')}")
            logger.debug(f"    更新字段: {list(update_fields.keys())[:5]}...")
            if protected_count > 0:
                logger.debug(f"    保护字段: {protected_count} 个（被人工修改过）")
        else:
            logger.info(f"  ○ 跳过赛事: {crawler_data.get('event_id')}（所有字段都被保护）")

    def _update_race_category_protected(self, cursor, db_record, crawler_cat, event_id, cat_name):
        """
        更新组别（保护人工修改）

        ⭐ 检查 race_categories.manually_modified_fields
        """
        # 获取人工修改的字段列表
        manually_modified = json.loads(db_record.get('manually_modified_fields') or '[]')

        # ⭐ 组别字段映射（爬虫字段 → 数据库字段）
        category_field_mapping = {
            'name': 'name',
            'distance': 'distance',
            'distance_numeric': 'distance_numeric',
            'fee': 'fee',
            'price_per_km': 'price_per_km',
            'zaoniao_fee': 'zaoniao_fee',
            'total_quota': 'total_quota',
            'registered_count': 'registered_count',
            'start_time': 'start_time',
            'cutoff_time': 'cutoff_time',
            'registration_status': 'registration_status',
            'registration_url': 'registration_url'
        }

        # 准备更新的字段（排除人工修改的）
        update_fields = {}
        protected_count = 0

        for crawler_field, db_field in category_field_mapping.items():
            # 检查是否被人工修改
            if db_field in manually_modified:
                protected_count += 1
                continue

            # 检查爬虫数据中是否有该字段
            if crawler_field in crawler_cat and crawler_cat[crawler_field]:
                update_fields[db_field] = crawler_cat[crawler_field]

        # 执行更新
        if update_fields:
            set_clause = ', '.join([f"{k}=%s" for k in update_fields.keys()])
            set_clause += ", updated_at=%s"

            values = list(update_fields.values()) + [datetime.now()]

            cursor.execute(
                f"UPDATE race_categories SET {set_clause} WHERE id=%s",
                values + [db_record['id']]
            )

            logger.info(f"  ✓ 智能更新组别: {event_id} - {cat_name}")
            logger.debug(f"    更新字段: {list(update_fields.keys())[:5]}...")
            if protected_count > 0:
                logger.debug(f"    保护字段: {protected_count} 个（被人工修改过）")
        else:
            logger.info(f"  ○ 跳过组别: {event_id} - {cat_name}（所有字段都被保护）")

    def _insert_race_category(self, cursor, race_event_id, crawler_data, crawler_cat):
        """新增组别"""
        insert_data = {
            'race_event_id': race_event_id,
            'name': crawler_cat.get('name'),
            'distance': crawler_cat.get('distance'),
            'distance_numeric': crawler_cat.get('distance_numeric'),
            'fee': crawler_cat.get('fee'),
            'price_per_km': crawler_cat.get('price_per_km'),
            'zaoniao_fee': crawler_cat.get('zaoniao_fee'),
            'total_quota': crawler_cat.get('total_quota'),
            'registered_count': crawler_cat.get('registered_count'),
            'start_time': crawler_cat.get('start_time'),
            'cutoff_time': crawler_cat.get('cutoff_time'),
            'registration_status': crawler_cat.get('registration_status'),
            'registration_url': crawler_cat.get('registration_url'),
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }

        # 移除None值
        insert_data = {k: v for k, v in insert_data.items() if v is not None}

        fields = ', '.join(insert_data.keys())
        placeholders = ', '.join(['%s'] * len(insert_data))

        cursor.execute(
            f"INSERT INTO race_categories ({fields}) VALUES ({placeholders})",
            list(insert_data.values())
        )