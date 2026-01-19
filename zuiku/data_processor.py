"""
数据处理工具模块
功能：
1. 提取公里数纯数字（去掉km单位）
2. 计算公里数单价（报名费用/公里数）
3. 数据标准化和验证
"""
import re
import logging
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class DataProcessor:
    """数据处理器"""

    def __init__(self):
        # 公里数的各种表示方式
        self.distance_patterns = [
            r'(\d+\.?\d*)\s*公里',
            r'(\d+\.?\d*)\s*km',
            r'(\d+\.?\d*)\s*k',
            r'(\d+\.?\d*)\s*千米',
        ]

    def extract_distance_number(self, distance_str: Union[str, int, float]) -> Optional[float]:
        """
        提取公里数的纯数字

        Args:
            distance_str: 公里数字符串，如 "42.195km"、"21.0975公里"、"10k" 等

        Returns:
            float: 提取的数字，如 42.195
            None: 无法提取时返回None

        Examples:
            "42.195km" -> 42.195
            "21.0975公里" -> 21.0975
            "10k" -> 10.0
            "5" -> 5.0
            "null" -> None
        """
        if not distance_str or distance_str == 'null':
            return None

        # 如果已经是数字，直接返回
        if isinstance(distance_str, (int, float)):
            return float(distance_str)

        # 转换为字符串处理
        distance_str = str(distance_str).strip().lower()

        # 尝试各种模式匹配
        for pattern in self.distance_patterns:
            match = re.search(pattern, distance_str)
            if match:
                try:
                    number = float(match.group(1))
                    logger.debug(f"提取公里数: '{distance_str}' -> {number}")
                    return number
                except ValueError:
                    continue

        # 如果没有匹配到，尝试直接转换为数字
        try:
            # 移除所有非数字字符（除了小数点）
            clean_str = re.sub(r'[^\d.]', '', distance_str)
            if clean_str:
                number = float(clean_str)
                logger.debug(f"直接提取公里数: '{distance_str}' -> {number}")
                return number
        except ValueError:
            pass

        logger.warning(f"无法提取公里数: '{distance_str}'")
        return None

    def extract_fee_number(self, fee_str: Union[str, int, float]) -> Optional[float]:
        """
        提取费用的纯数字

        Args:
            fee_str: 费用字符串，如 "120.00"、"80元"、"100" 等

        Returns:
            float: 提取的数字，如 120.00
            None: 无法提取时返回None
        """
        if not fee_str or fee_str == 'null':
            return None

        # 如果已经是数字，直接返回
        if isinstance(fee_str, (int, float)):
            return float(fee_str)

        # 转换为字符串处理
        fee_str = str(fee_str).strip()

        # 移除所有非数字字符（除了小数点）
        try:
            clean_str = re.sub(r'[^\d.]', '', fee_str)
            if clean_str:
                number = float(clean_str)
                logger.debug(f"提取费用: '{fee_str}' -> {number}")
                return number
        except ValueError:
            pass

        logger.warning(f"无法提取费用: '{fee_str}'")
        return None

    def calculate_price_per_km(self, fee: Union[str, int, float],
                               distance: Union[str, int, float]) -> Optional[float]:
        """
        计算公里数单价（每公里费用）

        Args:
            fee: 报名费用
            distance: 公里数

        Returns:
            float: 每公里费用，保留2位小数
            None: 计算失败时返回None

        Examples:
            fee="120.00", distance="42.195km" -> 2.84
            fee="80", distance="21.0975" -> 3.79
            fee="60", distance="10km" -> 6.00
        """
        # 提取纯数字
        fee_num = self.extract_fee_number(fee)
        distance_num = self.extract_distance_number(distance)

        # 验证数据有效性
        if fee_num is None or distance_num is None:
            logger.debug(f"无法计算单价: fee={fee}, distance={distance}")
            return None

        if distance_num <= 0:
            logger.warning(f"公里数无效: {distance_num}")
            return None

        if fee_num < 0:
            logger.warning(f"费用无效: {fee_num}")
            return None

        # 计算单价
        try:
            price_per_km = fee_num / distance_num
            result = round(price_per_km, 2)  # 保留2位小数
            logger.debug(f"计算单价: {fee_num} / {distance_num} = {result}")
            return result
        except Exception as e:
            logger.error(f"计算单价失败: {e}")
            return None

    def process_race_category(self, category: Dict) -> Dict:
        """
        处理单个组别数据

        功能：
        1. 提取公里数纯数字（新增 distance_numeric 字段）
        2. 计算公里数单价（新增 price_per_km 字段）
        3. 保留原始数据

        Args:
            category: 组别数据字典

        Returns:
            处理后的组别数据（包含新字段）
        """
        processed = category.copy()

        # 1. 提取公里数纯数字
        if category.get('distance'):
            distance_numeric = self.extract_distance_number(category['distance'])
            if distance_numeric is not None:
                processed['distance_numeric'] = distance_numeric
                logger.debug(f"组别 {category.get('name')}: 公里数 {category['distance']} -> {distance_numeric}")
            else:
                processed['distance_numeric'] = None
                logger.warning(f"组别 {category.get('name')}: 无法提取公里数 '{category['distance']}'")
        else:
            processed['distance_numeric'] = None

        # 2. 计算公里数单价
        if category.get('fee') and category.get('distance'):
            price_per_km = self.calculate_price_per_km(
                category['fee'],
                category['distance']
            )
            if price_per_km is not None:
                processed['price_per_km'] = price_per_km
                logger.info(f"组别 {category.get('name')}: 单价 = {price_per_km} 元/公里")
            else:
                processed['price_per_km'] = None
                logger.warning(f"组别 {category.get('name')}: 无法计算单价")
        else:
            processed['price_per_km'] = None

        return processed

    def process_event_data(self, event_data: Dict) -> Dict:
        """
        处理整个赛事数据

        功能：
        1. 处理所有组别的公里数和单价
        2. 添加统计信息

        Args:
            event_data: 赛事数据字典

        Returns:
            处理后的赛事数据
        """
        processed = event_data.copy()

        logger.info(f"\n处理赛事数据: {event_data.get('name', 'Unknown')}")
        logger.info("-" * 80)

        # 处理每个组别
        if processed.get('race_categories'):
            processed_categories = []

            for idx, category in enumerate(processed['race_categories'], 1):
                logger.info(f"\n处理组别 {idx}/{len(processed['race_categories'])}: {category.get('name', 'Unknown')}")

                processed_cat = self.process_race_category(category)
                processed_categories.append(processed_cat)

                # 输出处理结果
                logger.info(f"  原始公里数: {category.get('distance')}")
                logger.info(f"  纯数字公里数: {processed_cat.get('distance_numeric')}")
                logger.info(f"  报名费用: {category.get('fee')}")
                logger.info(f"  公里单价: {processed_cat.get('price_per_km')} 元/公里")

            processed['race_categories'] = processed_categories

            # 添加统计信息
            self._add_statistics(processed)

        logger.info("-" * 80)

        return processed

    def _add_statistics(self, event_data: Dict):
        """添加统计信息"""
        categories = event_data.get('race_categories', [])

        if not categories:
            return

        # 统计有效数据
        valid_price_per_km = [
            cat['price_per_km'] for cat in categories
            if cat.get('price_per_km') is not None
        ]

        if valid_price_per_km:
            event_data['avg_price_per_km'] = round(
                sum(valid_price_per_km) / len(valid_price_per_km),
                2
            )
            event_data['min_price_per_km'] = round(min(valid_price_per_km), 2)
            event_data['max_price_per_km'] = round(max(valid_price_per_km), 2)

            logger.info(f"\n统计信息:")
            logger.info(f"  平均单价: {event_data['avg_price_per_km']} 元/公里")
            logger.info(f"  最低单价: {event_data['min_price_per_km']} 元/公里")
            logger.info(f"  最高单价: {event_data['max_price_per_km']} 元/公里")

    def batch_process_events(self, events: List[Dict]) -> List[Dict]:
        """
        批量处理赛事数据

        Args:
            events: 赛事数据列表

        Returns:
            处理后的赛事数据列表
        """
        logger.info(f"\n{'=' * 80}")
        logger.info(f"批量处理 {len(events)} 个赛事数据")
        logger.info(f"{'=' * 80}")

        processed_events = []

        for idx, event in enumerate(events, 1):
            logger.info(f"\n[{idx}/{len(events)}] 处理赛事: {event.get('name', 'Unknown')}")

            try:
                processed = self.process_event_data(event)
                processed_events.append(processed)
                logger.info(f"✓ 处理成功")
            except Exception as e:
                logger.error(f"✗ 处理失败: {e}", exc_info=True)
                # 出错时保留原始数据
                processed_events.append(event)

        logger.info(f"\n{'=' * 80}")
        logger.info(f"批量处理完成: 成功 {len(processed_events)}/{len(events)}")
        logger.info(f"{'=' * 80}\n")

        return processed_events

    def validate_processed_data(self, event_data: Dict) -> Dict:
        """
        验证处理后的数据

        Returns:
            验证结果字典
        """
        validation = {
            'is_valid': True,
            'warnings': [],
            'errors': []
        }

        categories = event_data.get('race_categories', [])

        for idx, cat in enumerate(categories, 1):
            cat_name = cat.get('name', f'组别{idx}')

            # 检查公里数提取
            if cat.get('distance') and not cat.get('distance_numeric'):
                validation['warnings'].append(
                    f"{cat_name}: 公里数 '{cat['distance']}' 无法提取为数字"
                )

            # 检查单价计算
            if cat.get('fee') and cat.get('distance') and not cat.get('price_per_km'):
                validation['warnings'].append(
                    f"{cat_name}: 无法计算单价 (费用: {cat['fee']}, 公里数: {cat['distance']})"
                )

            # 检查单价合理性（每公里1-50元）
            if cat.get('price_per_km'):
                price = cat['price_per_km']
                if price < 1 or price > 50:
                    validation['warnings'].append(
                        f"{cat_name}: 单价 {price} 元/公里 可能不合理"
                    )

        if validation['warnings']:
            validation['is_valid'] = False

        return validation


# ==================== 便捷函数 ====================

def process_event(event_data: Dict) -> Dict:
    """
    便捷函数：处理单个赛事数据

    Args:
        event_data: 赛事数据

    Returns:
        处理后的数据（包含 distance_numeric 和 price_per_km）
    """
    processor = DataProcessor()
    return processor.process_event_data(event_data)


def extract_distance(distance_str: str) -> Optional[float]:
    """
    便捷函数：提取公里数纯数字

    Args:
        distance_str: 公里数字符串，如 "42.195km"

    Returns:
        纯数字，如 42.195
    """
    processor = DataProcessor()
    return processor.extract_distance_number(distance_str)


def calculate_price_per_km(fee: Union[str, float], distance: Union[str, float]) -> Optional[float]:
    """
    便捷函数：计算公里数单价

    Args:
        fee: 报名费用
        distance: 公里数

    Returns:
        每公里费用
    """
    processor = DataProcessor()
    return processor.calculate_price_per_km(fee, distance)


# ==================== 使用示例 ====================

if __name__ == '__main__':
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # 示例数据
    test_event = {
        'name': '2025厦门马拉松',
        'event_date': '2025-01-05',
        'location': '福建省-厦门市',
        'total_scale': '30000',
        'race_categories': [
            {
                'name': '全程马拉松',
                'distance': '42.195km',
                'fee': '120.00',
                'total_quota': '15000'
            },
            {
                'name': '半程马拉松',
                'distance': '21.0975公里',
                'fee': '100',
                'total_quota': '10000'
            },
            {
                'name': '10公里跑',
                'distance': '10k',
                'fee': '80元',
                'total_quota': '5000'
            }
        ]
    }

    print("\n" + "=" * 80)
    print("数据处理工具示例")
    print("=" * 80)

    # 处理数据
    processor = DataProcessor()
    processed = processor.process_event_data(test_event)

    # 显示结果
    print("\n" + "=" * 80)
    print("处理结果")
    print("=" * 80)

    for cat in processed['race_categories']:
        print(f"\n组别: {cat['name']}")
        print(f"  原始公里数: {cat['distance']}")
        print(f"  纯数字公里数: {cat.get('distance_numeric')}")
        print(f"  报名费用: {cat['fee']}")
        print(f"  公里单价: {cat.get('price_per_km')} 元/公里")

    if processed.get('avg_price_per_km'):
        print(f"\n赛事统计:")
        print(f"  平均单价: {processed['avg_price_per_km']} 元/公里")
        print(f"  单价范围: {processed['min_price_per_km']} - {processed['max_price_per_km']} 元/公里")

    # 验证数据
    validation = processor.validate_processed_data(processed)

    print(f"\n数据验证:")
    print(f"  有效性: {'✓ 通过' if validation['is_valid'] else '✗ 有警告'}")
    if validation['warnings']:
        print(f"  警告:")
        for warning in validation['warnings']:
            print(f"    - {warning}")

# ==================== 增强数据处理器 ====================

class EnhancedDataProcessor:
    """
    增强数据处理器
    处理新增字段：year, month, day, province, city, county, event_type, is_online
    """

    @staticmethod
    def parse_date(event_date: str) -> dict:
        """
        拆分日期为年月日

        Args:
            event_date: 日期字符串 "YYYY-MM-DD"

        Returns:
            dict: {'year': 2026, 'month': 5, 'day': 16}
        """
        if not event_date:
            return {'year': None, 'month': None, 'day': None}

        try:
            # 支持多种日期格式
            date_str = str(event_date).strip()

            # 格式1: 2026-05-16
            if '-' in date_str:
                parts = date_str.split('-')
                if len(parts) == 3:
                    return {
                        'year': int(parts[0]),
                        'month': int(parts[1]),
                        'day': int(parts[2])
                    }

            # 格式2: 2026/05/16
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) == 3:
                    return {
                        'year': int(parts[0]),
                        'month': int(parts[1]),
                        'day': int(parts[2])
                    }

            logger.warning(f"无法解析日期: {event_date}")
            return {'year': None, 'month': None, 'day': None}

        except Exception as e:
            logger.error(f"日期解析失败: {e}")
            return {'year': None, 'month': None, 'day': None}

    @staticmethod
    def parse_location(location: str) -> dict:
        """
        拆分地点为省市县

        Args:
            location: 地点字符串 "湖北-神农架林区-松柏镇" 或 "黑龙江-哈尔滨市"

        Returns:
            dict: {'province': '湖北', 'city': '神农架林区', 'county': '松柏镇', 'is_domestic': 1}
        """
        if not location:
            return {
                'province': None,
                'city': None,
                'county': None,
                'is_domestic': 1
            }

        try:
            location = str(location).strip()

            # 判断是否境外
            if location == '境外' or '境外' in location:
                return {
                    'province': '境外',
                    'city': None,
                    'county': None,
                    'is_domestic': 0
                }

            # 拆分地点（支持"-"或"·"分隔）
            parts = location.replace('·', '-').split('-')
            parts = [p.strip() for p in parts if p.strip()]

            result = {
                'province': parts[0] if len(parts) > 0 else None,
                'city': parts[1] if len(parts) > 1 else None,
                'county': parts[2] if len(parts) > 2 else None,
                'is_domestic': 1
            }

            logger.debug(f"地点解析: {location} → {result}")
            return result

        except Exception as e:
            logger.error(f"地点解析失败: {e}")
            return {
                'province': None,
                'city': None,
                'county': None,
                'is_domestic': 1
            }

    @staticmethod
    def detect_event_type(name: str, description: str = '') -> str:
        """
        检测赛事类型

        Args:
            name: 赛事名称
            description: 赛事描述

        Returns:
            str: 赛事类型（马拉松/越野赛/铁三/游泳/自行车/欢乐跑/路跑/其他）
        """
        text = (name or '') + ' ' + (description or '')
        text = text.lower()

        # 按优先级匹配
        if '铁三' in text or '铁人三项' in text:
            return '铁人三项'
        if '越野' in text or '山地' in text or '山径' in text or '越野跑' in text:
            return '越野赛'
        if '马拉松' in text or 'marathon' in text:
            return '马拉松'
        if '游泳' in text or '公开水域' in text:
            return '游泳'
        if '自行车' in text or '骑行' in text:
            return '自行车'
        if '欢乐跑' in text or '迷你跑' in text or '亲子跑' in text:
            return '欢乐跑'
        if '路跑' in text or '健康跑' in text:
            return '路跑'

        return '其他'

    @staticmethod
    def is_online_event(location: str, name: str = '') -> int:
        """
        判断是否线上赛事

        Args:
            location: 地点
            name: 赛事名称

        Returns:
            int: 1=线上, 0=线下
        """
        text = (location or '') + ' ' + (name or '')
        text = text.lower()

        online_keywords = ['线上', '云跑', '云上', '网络', '虚拟', '居家', '在线']

        for keyword in online_keywords:
            if keyword in text:
                return 1

        return 0

    @staticmethod
    def extract_distance_numeric(distance_str):
        """
        从距离字符串中提取数字

        Args:
            distance_str: 距离字符串，如 "42.195km", "21公里", "10 km"

        Returns:
            float: 距离数字，如果提取失败返回None
        """
        if not distance_str:
            return None

        try:
            # 移除空格
            distance_str = str(distance_str).strip()

            # 匹配数字（包括小数）
            match = re.search(r'(\d+\.?\d*)', distance_str)
            if match:
                numeric = float(match.group(1))
                logger.debug(f"距离提取: {distance_str} → {numeric}")
                return numeric

            logger.warning(f"无法从距离中提取数字: {distance_str}")
            return None

        except Exception as e:
            logger.error(f"距离提取失败: {e}")
            return None

    @staticmethod
    def calculate_price_per_km(fee, distance_numeric):
        """
        计算每公里单价

        Args:
            fee: 费用（可以是字符串或数字）
            distance_numeric: 距离数字

        Returns:
            float: 每公里单价，保留2位小数
        """
        if not fee or not distance_numeric:
            return None

        try:
            # 转换费用（可能包含"元"、"¥"等）
            fee_str = str(fee).replace('元', '').replace('¥', '').replace(',', '').strip()
            fee_num = float(fee_str)

            # 计算单价
            if distance_numeric > 0:
                price = round(fee_num / distance_numeric, 2)
                logger.debug(f"单价计算: {fee} / {distance_numeric} = {price}")
                return price

            return None

        except Exception as e:
            logger.error(f"单价计算失败: {e}")
            return None

    @classmethod
    def process_event_data(cls, event_data):
        """
        处理赛事数据，添加新字段

        Args:
            event_data: 赛事数据字典

        Returns:
            dict: 处理后的赛事数据（添加了新字段）
        """
        try:
            # 1. 拆分日期
            if event_data.get('event_date'):
                date_info = cls.parse_date(event_data['event_date'])
                event_data.update(date_info)

            # 2. 拆分地点
            if event_data.get('location'):
                location_info = cls.parse_location(event_data['location'])
                event_data.update(location_info)

            # 3. 检测类型
            event_data['event_type'] = cls.detect_event_type(
                event_data.get('name', ''),
                event_data.get('description', '')
            )

            # 4. 判断线上/线下
            event_data['is_online'] = cls.is_online_event(
                event_data.get('location'),
                event_data.get('name')
            )

            # 5. 处理组别的distance_numeric和price_per_km
            if 'race_categories' in event_data and event_data['race_categories']:
                for category in event_data['race_categories']:
                    # 提取distance_numeric（从"42.195km"提取42.195）
                    if category.get('distance') and not category.get('distance_numeric'):
                        category['distance_numeric'] = cls.extract_distance_numeric(
                            category['distance']
                        )

                    # 计算price_per_km
                    if category.get('fee') and category.get('distance_numeric'):
                        category['price_per_km'] = cls.calculate_price_per_km(
                            category['fee'],
                            category['distance_numeric']
                        )

            logger.info(f"✓ 数据处理完成: {event_data.get('name')}")
            return event_data

        except Exception as e:
            logger.error(f"数据处理失败: {e}")
            return event_data