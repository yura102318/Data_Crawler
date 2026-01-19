"""
赛事数据智能管理脚本 - 完整修复版
✨ 适配新表结构（race_events + race_categories）
✨ 分别标记两张表的人工修改字段

核心修改：
1. race_events.manually_modified_fields - 标记赛事字段的人工修改
2. race_categories.manually_modified_fields - 标记组别字段的人工修改
3. 导入时自动标记
4. 爬虫同步时检查标记，保护人工数据
"""

import pymysql
import json
import pandas as pd
from datetime import datetime
import sys
import os
import logging

logger = logging.getLogger(__name__)


# ⭐⭐⭐ 数据库配置（请修改为您的实际配置）⭐⭐⭐
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'lxzq102318',
    'database': 'longjing',
    'charset': 'utf8mb4'
}


class RaceEventsManager:
    """赛事管理器 - 完整修复版"""

    def __init__(self):
        self.db_config = DB_CONFIG

    def _get_connection(self):
        """获取数据库连接"""
        return pymysql.connect(**self.db_config)

    def import_manual_edits(self, excel_file: str, enable_delete: bool = False):
        """
        智能导入 - 分别标记赛事和组别的人工修改

        ⭐ 关键修改：
        1. 赛事字段 → 更新 race_events + 标记到 race_events.manually_modified_fields
        2. 组别字段 → 更新 race_categories + 标记到 race_categories.manually_modified_fields
        """
        print(f"\n{'='*80}")
        print(f"智能导入: {excel_file}")
        print(f"删除同步: {'启用' if enable_delete else '禁用'}")
        print(f"{'='*80}\n")

        if not os.path.exists(excel_file):
            print(f"❌ 文件不存在: {excel_file}")
            return

        df = pd.read_excel(excel_file)

        # 数据清洗
        print("数据清洗...")
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].replace(['null', 'NULL', 'Null', 'None', 'NONE'], '')
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

        if '组别' in df.columns:
            null_count = (df['组别'].astype(str).str.lower() == 'null').sum()
            if null_count > 0:
                print(f"  发现 {null_count} 条组别为'null'的脏数据，已清理")
                df['组别'] = df['组别'].replace(['null', 'NULL', 'Null'], '')

        print(f"数据清洗完成\n")
        print(f"📊 Excel: {len(df)} 行\n")

        conn = self._get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 去重
        df_original = len(df)
        df = self._deduplicate_excel(df)
        if len(df) < df_original:
            print(f"去重: {df_original} → {len(df)} 行\n")

        updated_events = 0
        updated_categories = 0
        inserted = 0
        deleted = 0

        # ⭐⭐⭐ Excel字段映射 ⭐⭐⭐

        # 赛事字段（对应 race_events 表）
        event_field_mapping = {
            '赛事id': 'event_id',
            '赛事名称': 'name',
            '赛事日期': 'event_date',
            '赛事级别': 'event_level',
            '赛事地址(省市县)': 'location',
            '详细地点': 'detailed_address',
            '赛事规模(人数)': 'total_scale',
            '报名费区间': 'registration_fee',
            '运营单位/运营公司': 'organizer',
            '主办单位': 'host_units',
            '承办单位': 'co_organizers',
            '协办单位/支持单位': 'supporters',
            '联系电话/组委会电话': 'contact_phone',
            '组委会邮箱/联系邮箱': 'contact_email',
            '联系人': 'contact_person',
            '报名截止时间': 'registration_deadline'
        }

        # 组别字段（对应 race_categories 表）
        category_field_mapping = {
            '组别': 'name',
            '公里数': 'distance_numeric',
            '报名费用': 'fee',
            '每公里单价': 'price_per_km',
            '早鸟价': 'zaoniao_fee',
            '组别名额': 'total_quota',
            '组别起跑时间': 'start_time',
            '组别关门时间': 'cutoff_time'
        }

        # 按赛事ID分组处理
        for event_id_raw, group in df.groupby('赛事id'):
            event_id = str(event_id_raw).strip()

            # 1. 查找赛事主表记录
            cursor.execute(
                "SELECT * FROM race_events WHERE event_id = %s",
                (event_id,)
            )
            event_record = cursor.fetchone()

            if not event_record:
                print(f"⚠️  赛事不存在: {event_id}，跳过")
                continue

            race_event_id = event_record['id']

            # 2. ⭐ 更新赛事主表（带标记）
            first_row = group.iloc[0]
            event_updated = self._update_race_event_with_mark(
                cursor, event_record, first_row, event_field_mapping
            )
            if event_updated:
                updated_events += 1

            # 3. ⭐ 处理组别（带标记）
            for _, row in group.iterrows():
                category_name = row.get('组别', '')
                if pd.isna(category_name):
                    category_name = ''
                category_name = str(category_name).strip()

                # 查找组别
                if category_name:
                    cursor.execute(
                        "SELECT * FROM race_categories WHERE race_event_id = %s AND name = %s",
                        (race_event_id, category_name)
                    )
                else:
                    cursor.execute(
                        "SELECT * FROM race_categories WHERE race_event_id = %s AND (name IS NULL OR name = '')",
                        (race_event_id,)
                    )

                category_record = cursor.fetchone()

                if category_record:
                    # 更新组别
                    if self._update_race_category_with_mark(
                        cursor, category_record, row, category_field_mapping
                    ):
                        updated_categories += 1
                        if updated_categories <= 10:
                            print(f"  ✓ 更新组别: {event_id} - {category_name}")
                else:
                    # 新增组别
                    if self._insert_race_category(
                        cursor, race_event_id, row, category_field_mapping
                    ):
                        inserted += 1
                        if inserted <= 10:
                            print(f"  ✓ 新增组别: {event_id} - {category_name}")

        # 删除同步（如果启用）
        if enable_delete:
            excel_event_ids = set()
            excel_keys = set()
            for _, row in df.iterrows():
                eid = str(row.get('赛事id', '')).strip()
                cname = str(row.get('组别', '')).strip()
                excel_event_ids.add(eid)
                excel_keys.add((eid, cname))

            if excel_event_ids:
                placeholders = ','.join(['%s'] * len(excel_event_ids))
                cursor.execute(f"""
                    SELECT e.event_id, c.name, c.id
                    FROM race_categories c
                    JOIN race_events e ON c.race_event_id = e.id
                    WHERE e.event_id IN ({placeholders})
                """, list(excel_event_ids))

                for db_record in cursor.fetchall():
                    db_key = (db_record['event_id'], db_record['name'] or '')
                    if db_key not in excel_keys:
                        cursor.execute("DELETE FROM race_categories WHERE id = %s", (db_record['id'],))
                        deleted += 1
                        if deleted <= 10:
                            print(f"  ✗ 删除: {db_key[0]} - {db_key[1]}")

                print(f"  删除范围: 仅限Excel中的 {len(excel_event_ids)} 个赛事")

        conn.commit()
        conn.close()

        print(f"\n{'='*80}")
        print(f"完成: 更新赛事{updated_events} 更新组别{updated_categories} 新增{inserted} 删除{deleted}")
        print(f"{'='*80}\n")

    def _update_race_event_with_mark(self, cursor, db_record, excel_row, field_mapping):
        """
        更新赛事主表 + 标记人工修改字段

        ⭐ 关键：标记到 race_events.manually_modified_fields
        """
        update_values = {}

        for excel_col, db_field in field_mapping.items():
            if excel_col not in excel_row.index:
                continue

            excel_val = excel_row[excel_col]
            db_val = db_record.get(db_field)

            # 字符串字段
            excel_str = str(excel_val).strip() if not pd.isna(excel_val) else ''
            db_str = str(db_val).strip() if db_val is not None else ''

            if excel_str and excel_str != 'nan' and excel_str != 'None':
                if excel_str != db_str:
                    update_values[db_field] = excel_str

        if not update_values:
            return False

        # ⭐⭐⭐ 标记人工修改的字段 ⭐⭐⭐
        manually_modified = json.loads(db_record.get('manually_modified_fields') or '[]')

        for field in update_values.keys():
            if field not in manually_modified and field not in ['updated_at', 'created_at']:
                manually_modified.append(field)

        update_values['manually_modified_fields'] = json.dumps(manually_modified)

        # 执行更新
        set_clause = ', '.join([f"{f}=%s" for f in update_values.keys()])
        set_clause += ", updated_at=%s"
        values = list(update_values.values()) + [datetime.now()]

        cursor.execute(
            f"UPDATE race_events SET {set_clause} WHERE id=%s",
            values + [db_record['id']]
        )
        return True

    def _update_race_category_with_mark(self, cursor, db_record, excel_row, field_mapping):
        """
        更新组别 + 标记人工修改字段

        ⭐ 关键：标记到 race_categories.manually_modified_fields
        """
        update_values = {}

        for excel_col, db_field in field_mapping.items():
            if excel_col not in excel_row.index:
                continue

            excel_val = excel_row[excel_col]
            db_val = db_record.get(db_field)

            # 数字字段
            if db_field in ['distance_numeric', 'fee', 'price_per_km', 'zaoniao_fee', 'total_quota', 'registered_count']:
                try:
                    excel_num = float(excel_val) if not pd.isna(excel_val) and excel_val not in ['', 'None', 'nan'] else None
                    db_num = float(db_val) if db_val is not None else None

                    if excel_num is not None and db_num is not None:
                        if abs(excel_num - db_num) < 0.01:
                            continue
                    elif excel_num is None and db_num is None:
                        continue

                    if excel_num is not None:
                        update_values[db_field] = excel_num
                    continue
                except:
                    pass

            # 字符串字段
            excel_str = str(excel_val).strip() if not pd.isna(excel_val) else ''
            db_str = str(db_val).strip() if db_val is not None else ''

            if excel_str and excel_str != 'nan' and excel_str != 'None':
                if excel_str != db_str:
                    update_values[db_field] = excel_str

        if not update_values:
            return False

        # ⭐⭐⭐ 标记人工修改的字段 ⭐⭐⭐
        manually_modified = json.loads(db_record.get('manually_modified_fields') or '[]')

        for field in update_values.keys():
            if field not in manually_modified and field not in ['updated_at', 'created_at', 'price_per_km']:
                manually_modified.append(field)

        update_values['manually_modified_fields'] = json.dumps(manually_modified)

        # 强制重新计算每公里单价
        distance = update_values.get('distance_numeric')
        fee = update_values.get('fee')

        if distance is None:
            distance = db_record.get('distance_numeric')
        if fee is None:
            fee = db_record.get('fee')

        if distance and fee:
            try:
                calculated_price = round(float(fee) / float(distance), 2)
                update_values['price_per_km'] = calculated_price
            except Exception as e:
                print(f"⚠️  计算每公里单价失败: {e}")

        # 执行更新
        set_clause = ', '.join([f"{f}=%s" for f in update_values.keys()])
        set_clause += ", updated_at=%s"
        values = list(update_values.values()) + [datetime.now()]

        cursor.execute(
            f"UPDATE race_categories SET {set_clause} WHERE id=%s",
            values + [db_record['id']]
        )
        return True

    def _insert_race_category(self, cursor, race_event_id, excel_row, field_mapping):
        """插入新组别"""
        data = {'race_event_id': race_event_id}

        for excel_col, db_field in field_mapping.items():
            if excel_col in excel_row.index:
                val = excel_row[excel_col]
                if not pd.isna(val):
                    val_str = str(val).strip()
                    if val_str and val_str != 'nan' and val_str != 'None':
                        data[db_field] = val_str

        # 自动计算每公里单价
        if 'distance_numeric' in data and 'fee' in data:
            try:
                distance = float(data['distance_numeric'])
                fee = float(data['fee'])
                if distance > 0:
                    data['price_per_km'] = round(fee / distance, 2)
            except:
                pass

        data['created_at'] = datetime.now()
        data['updated_at'] = datetime.now()

        fields = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))

        cursor.execute(
            f"INSERT INTO race_categories ({fields}) VALUES ({placeholders})",
            list(data.values())
        )
        return True

    def _deduplicate_excel(self, df):
        """Excel去重"""
        df['_key'] = df.apply(
            lambda r: f"{r.get('赛事id', '')}__{r.get('组别', '')}",
            axis=1
        )
        df = df.drop_duplicates(subset=['_key'], keep='first').drop(columns=['_key'])
        return df

    def export_to_excel(self, excel_file: str):
        """导出到Excel - 使用JOIN关联两个表"""
        print(f"\n{'='*80}")
        print(f"导出: {excel_file}")
        print(f"{'='*80}\n")

        conn = self._get_connection()

        # ✨ 使用JOIN关联race_events和race_categories
        sql = """
        SELECT 
            e.event_id as "赛事id",
            e.event_url as "赛事链接",
            e.name as "赛事名称",
            e.event_date as "赛事日期",
            e.event_level as "赛事级别",
            e.location as "赛事地址(省市县)",
            e.detailed_address as "详细地点",
            c.name as "组别",
            c.distance_numeric as "公里数",
            c.fee as "报名费用",
            c.price_per_km as "每公里单价",
            c.zaoniao_fee as "早鸟价",
            c.total_quota as "组别名额",
            e.total_scale as "赛事规模(人数)",
            e.registration_fee as "报名费区间",
            c.start_time as "组别起跑时间",
            c.cutoff_time as "组别关门时间",
            e.organizer as "运营单位/运营公司",
            e.host_units as "主办单位",
            e.co_organizers as "承办单位",
            e.supporters as "协办单位/支持单位",
            e.contact_phone as "联系电话/组委会电话",
            e.contact_email as "组委会邮箱/联系邮箱",
            e.contact_person as "联系人",
            c.registration_status as "赛事状态",
            e.registration_deadline as "报名截止时间",
            e.created_at as "爬取日期",
            e.updated_at as "更新日期"
        FROM race_events e
        LEFT JOIN race_categories c ON e.id = c.race_event_id
        WHERE e.event_date >= '2019-12-28'
        ORDER BY 
            e.event_date DESC,
            e.event_id,
            CASE WHEN c.name IS NULL OR c.name = '' THEN 1 ELSE 0 END,
            c.distance_numeric ASC
        """

        df = pd.read_sql_query(sql, conn)
        conn.close()

        df.to_excel(excel_file, index=False, engine='openpyxl')

        print(f"✓ 完成: {len(df)} 行")
        print(f"{'='*80}\n")


def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python race_events_manager.py import <文件.xlsx>")
        print("  python race_events_manager.py export <文件.xlsx>")
        return

    manager = RaceEventsManager()

    if sys.argv[1] == 'import' and len(sys.argv) >= 3:
        manager.import_manual_edits(sys.argv[2], '--no-delete' not in sys.argv)
    elif sys.argv[1] == 'export' and len(sys.argv) >= 3:
        manager.export_to_excel(sys.argv[2])
    else:
        print("❌ 无效命令")


if __name__ == '__main__':
    main()