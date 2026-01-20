# zuiku/utils/trail_distance_classifier.py

"""
越野赛距离分组器
✨ 自动将越野赛距离分组：10公里以下、10-19公里、...、100公里以上
"""
import logging

logger = logging.getLogger(__name__)


class TrailDistanceClassifier:
    """越野赛距离分组器"""
    
    # 距离分组标准
    DISTANCE_GROUPS = [
        {'name': '10公里以下', 'min': 0, 'max': 9.999, 'code': 'UNDER_10'},
        {'name': '10-19公里', 'min': 10, 'max': 19.999, 'code': 'KM_10_19'},
        {'name': '20-30公里', 'min': 20, 'max': 30, 'code': 'KM_20_30'},
        {'name': '31-40公里', 'min': 31, 'max': 40, 'code': 'KM_31_40'},
        {'name': '41-50公里', 'min': 41, 'max': 50, 'code': 'KM_41_50'},
        {'name': '51-60公里', 'min': 51, 'max': 60, 'code': 'KM_51_60'},
        {'name': '61-70公里', 'min': 61, 'max': 70, 'code': 'KM_61_70'},
        {'name': '71-80公里', 'min': 71, 'max': 80, 'code': 'KM_71_80'},
        {'name': '81-90公里', 'min': 81, 'max': 90, 'code': 'KM_81_90'},
        {'name': '91-99公里', 'min': 91, 'max': 99.999, 'code': 'KM_91_99'},
        {'name': '100公里', 'min': 100, 'max': 100, 'code': 'KM_100'},
        {'name': '100公里以上', 'min': 100.001, 'max': 9999, 'code': 'OVER_100'},
    ]
    
    # 需要分组的赛事类型
    TRAIL_EVENT_TYPES = ['越野赛', '山地马拉松', '越野跑', '山地越野', 'trail']
    
    @staticmethod
    def classify_distance(distance_km: float, event_type: str = None) -> str:
        """
        对距离进行分组
        
        Args:
            distance_km: 距离（公里）
            event_type: 赛事类型（马拉松/越野赛等）
        
        Returns:
            分组名称，如 "51-60公里"；非越野赛返回 None
        """
        # 只对越野赛分组
        if event_type:
            event_type_lower = event_type.lower()
            is_trail = any(
                trail_type.lower() in event_type_lower 
                for trail_type in TrailDistanceClassifier.TRAIL_EVENT_TYPES
            )
            
            if not is_trail:
                return None
        
        if not distance_km or distance_km <= 0:
            return None
        
        # 查找对应分组
        for group in TrailDistanceClassifier.DISTANCE_GROUPS:
            if group['min'] <= distance_km <= group['max']:
                return group['name']
        
        # 超出范围，返回"100公里以上"
        return '100公里以上'
    
    @staticmethod
    def get_group_code(distance_km: float, event_type: str = None) -> str:
        """获取分组代码（用于数据库存储）"""
        if event_type:
            event_type_lower = event_type.lower()
            is_trail = any(
                trail_type.lower() in event_type_lower 
                for trail_type in TrailDistanceClassifier.TRAIL_EVENT_TYPES
            )
            
            if not is_trail:
                return None
        
        if not distance_km or distance_km <= 0:
            return None
        
        for group in TrailDistanceClassifier.DISTANCE_GROUPS:
            if group['min'] <= distance_km <= group['max']:
                return group['code']
        
        return 'OVER_100'
    
    @staticmethod
    def get_all_groups() -> list:
        """获取所有分组（用于前端下拉框）"""
        return [group['name'] for group in TrailDistanceClassifier.DISTANCE_GROUPS]


# 便捷函数
def classify_trail_distance(distance_km: float, event_type: str = None) -> str:
    """便捷函数：越野赛距离分组"""
    return TrailDistanceClassifier.classify_distance(distance_km, event_type)


# 测试代码
if __name__ == '__main__':
    print("\n" + "=" * 80)
    print("越野赛距离分组测试")
    print("=" * 80)
    
    test_cases = [
        (5.5, '越野赛', '10公里以下'),
        (15, '越野跑', '10-19公里'),
        (42.195, '越野赛', '41-50公里'),
        (100, '山地马拉松', '100公里'),
        (168, '越野赛', '100公里以上'),
        (42.195, '马拉松', None),  # 非越野赛
    ]
    
    for distance, event_type, expected in test_cases:
        result = TrailDistanceClassifier.classify_distance(distance, event_type)
        code = TrailDistanceClassifier.get_group_code(distance, event_type)
        
        print(f"\n距离: {distance}km, 类型: {event_type}")
        print(f"  分组: {result}")
        print(f"  代码: {code}")
        
        assert result == expected, f"期望 {expected}，实际 {result}"
        print("  ✓ 通过")
    
    print("\n" + "=" * 80)
    print("✓✓✓ 所有测试通过！")
    print("=" * 80)