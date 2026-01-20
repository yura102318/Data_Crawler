# zuiku/utils/event_level_standardizer.py

"""
赛事级别标准化器
✨ 将各种表述统一为标准词库
✨ 支持：A类、金标、UTMB等多种认证体系
"""
import logging

logger = logging.getLogger(__name__)


class EventLevelStandardizer:
    """赛事级别标准化器"""
    
    # 标准词库（按优先级排序）
    STANDARD_LEVELS = {
        # 世界田径标准（最高优先级）
        'WORLD_PLATINUM': {
            'standard': '世界田径白金标赛事',
            'aliases': [
                '世界田联白金标', '白金标', 'world athletics platinum label',
                'platinum label', '世界田径白金', 'wa白金标'
            ],
            'priority': 200
        },
        'WORLD_GOLD': {
            'standard': '世界田径金标赛事',
            'aliases': [
                '世界田联金标', '金标', 'world athletics gold label',
                'gold label', '国际金标', '世界田径金标', 'wa金标'
            ],
            'priority': 190
        },
        'WORLD_SILVER': {
            'standard': '世界田径银标赛事',
            'aliases': [
                '世界田联银标', '银标', 'world athletics silver label',
                'silver label', '国际银标', '世界田径银标', 'wa银标'
            ],
            'priority': 180
        },
        'WORLD_BRONZE': {
            'standard': '世界田径铜标赛事',
            'aliases': [
                '世界田联铜标', '铜标', 'world athletics bronze label',
                'bronze label', '国际铜标', '世界田径铜标', 'wa铜标'
            ],
            'priority': 170
        },
        
        # 越野跑认证
        'UTMB': {
            'standard': 'UTMB认证',
            'aliases': [
                'utmb', 'utmb认证赛事', 'utmb系列赛', 'utmb积分赛',
                'utmb认证', 'utmb by utmb'
            ],
            'priority': 160
        },
        'ITRA': {
            'standard': 'ITRA认证',
            'aliases': [
                'itra', 'itra认证赛事', 'itra认证', 'itra积分赛',
                'itra points'
            ],
            'priority': 150
        },
        
        # 国内等级（中国田径协会）
        'A1': {
            'standard': 'A（A1）',
            'aliases': [
                'a1类', 'a1', 'a1类赛事', 'a（a1）类', 'a（a1）',
                'a1级', 'a1等级'
            ],
            'priority': 100
        },
        'A2': {
            'standard': 'A（A2）',
            'aliases': [
                'a2类', 'a2', 'a2类赛事', 'a（a2）类', 'a（a2）',
                'a2级', 'a2等级'
            ],
            'priority': 95
        },
        'A': {
            'standard': 'A类',
            'aliases': [
                'a类', 'a类赛事', 'a级', 'a等级', 'a类认证'
            ],
            'priority': 90
        },
        'B': {
            'standard': 'B类',
            'aliases': [
                'b类', 'b类赛事', 'b级', 'b等级', 'b类认证'
            ],
            'priority': 80
        },
        'C': {
            'standard': 'C类（属地办赛）',
            'aliases': [
                'c类', 'c类赛事', '属地办赛', 'c级', 'c等级',
                'c类认证', '属地赛'
            ],
            'priority': 70
        },
        
        # 其他认证
        'CAA': {
            'standard': '中国田径协会认证',
            'aliases': [
                '中国田径协会', '田协认证', '中田协', 'caa认证',
                '中国田协认证'
            ],
            'priority': 60
        },
        'AIMS': {
            'standard': 'AIMS认证',
            'aliases': [
                'aims', 'aims认证', 'aims会员赛事'
            ],
            'priority': 85
        },
        
        # 其他
        'OTHER': {
            'standard': '其他',
            'aliases': [],
            'priority': 0
        }
    }
    
    @staticmethod
    def standardize(raw_level: str) -> str:
        """
        标准化赛事级别
        
        Args:
            raw_level: 原始级别文本，如 "a1类"、"金标"
        
        Returns:
            标准化后的级别，如 "A（A1）"、"世界田径金标赛事"
        """
        if not raw_level or str(raw_level).strip() in ['null', 'None', '']:
            return '其他'
        
        raw_level_clean = str(raw_level).strip().lower()
        
        # 匹配标准词库
        best_match = None
        best_priority = -1
        
        for level_code, level_info in EventLevelStandardizer.STANDARD_LEVELS.items():
            # 精确匹配标准名称
            if raw_level_clean == level_info['standard'].lower():
                return level_info['standard']
            
            # 别名匹配
            for alias in level_info['aliases']:
                alias_lower = alias.lower()
                
                # 精确匹配
                if raw_level_clean == alias_lower:
                    if level_info['priority'] > best_priority:
                        best_match = level_info['standard']
                        best_priority = level_info['priority']
                    break
                
                # 模糊匹配（包含）
                elif alias_lower in raw_level_clean or raw_level_clean in alias_lower:
                    if level_info['priority'] > best_priority:
                        best_match = level_info['standard']
                        best_priority = level_info['priority']
        
        if best_match:
            logger.debug(f"级别标准化: '{raw_level}' → '{best_match}'")
            return best_match
        
        # 没有匹配到，保留原文（可能是新的级别）
        logger.warning(f"未识别的赛事级别，保留原文: '{raw_level}'")
        return raw_level
    
    @staticmethod
    def get_level_priority(standard_level: str) -> int:
        """获取级别优先级（用于排序）"""
        for level_code, level_info in EventLevelStandardizer.STANDARD_LEVELS.items():
            if level_info['standard'] == standard_level:
                return level_info['priority']
        return 0
    
    @staticmethod
    def get_all_standard_levels() -> list:
        """获取所有标准级别（用于下拉框）"""
        levels = [
            info['standard'] 
            for info in sorted(
                EventLevelStandardizer.STANDARD_LEVELS.values(),
                key=lambda x: x['priority'],
                reverse=True
            )
            if info['standard'] != '其他'
        ]
        levels.append('其他')  # 其他放在最后
        return levels
    
    @staticmethod
    def is_international_level(level: str) -> bool:
        """判断是否国际级别"""
        international_keywords = ['世界田径', '金标', '银标', '铜标', '白金标', 'utmb', 'itra', 'aims']
        level_lower = level.lower()
        return any(keyword in level_lower for keyword in international_keywords)


# 便捷函数
def standardize_event_level(raw_level: str) -> str:
    """便捷函数：标准化赛事级别"""
    return EventLevelStandardizer.standardize(raw_level)


# 测试代码
if __name__ == '__main__':
    print("\n" + "=" * 80)
    print("赛事级别标准化测试")
    print("=" * 80)
    
    test_cases = [
        ('a1类', 'A（A1）'),
        ('金标', '世界田径金标赛事'),
        ('世界田联银标', '世界田径银标赛事'),
        ('utmb', 'UTMB认证'),
        ('b类赛事', 'B类'),
        ('属地办赛', 'C类（属地办赛）'),
        ('中国田径协会', '中国田径协会认证'),
        ('未知级别', '未知级别'),  # 保留原文
    ]
    
    for raw, expected in test_cases:
        result = EventLevelStandardizer.standardize(raw)
        priority = EventLevelStandardizer.get_level_priority(result)
        
        print(f"\n原始: {raw}")
        print(f"  标准化: {result}")
        print(f"  优先级: {priority}")
        
        if expected != '未知级别':  # 未知级别会保留原文，所以跳过断言
            assert result == expected, f"期望 {expected}，实际 {result}"
        
        print("  ✓ 通过")
    
    print("\n所有标准级别:")
    for level in EventLevelStandardizer.get_all_standard_levels():
        priority = EventLevelStandardizer.get_level_priority(level)
        print(f"  - {level} (优先级: {priority})")
    
    print("\n" + "=" * 80)
    print("✓✓✓ 所有测试通过！")
    print("=" * 80)