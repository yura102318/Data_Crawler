"""
赛事级别提取器 V2 - 从HTML标签中提取
✨ 从赛事列表的 tag 标签中提取赛事级别

使用场景：
在 crawler.py 的 parse_event_list 方法中调用
从赛事列表HTML中提取赛事级别标签
"""
import re
import logging
from bs4 import BeautifulSoup
from typing import List, Optional

logger = logging.getLogger(__name__)


class EventLevelExtractorV2:
    """赛事级别提取器 V2 - 从HTML标签提取"""

    # 赛事级别关键词（按优先级排序）
    LEVEL_KEYWORDS = {
        # 国际级别
        '大满贯': 100,
        '金标': 90,
        '银标': 80,
        '铜标': 70,

        # 国内级别
        'a1类': 60,
        'a类': 50,
        'b类': 40,
        'c类': 30,

        # 其他认证
        '中国田径协会': 20,
        '国际田联': 95,
        'iaaf': 95,
        'aims': 85,
    }

    @staticmethod
    def extract_event_level_from_html(event_html: str) -> str:
        """
        从赛事列表HTML中提取赛事级别

        提取逻辑：
        1. 找到 <h4 class="name"> 标签
        2. 找到所有 <a href="https://zuicool.com/tag/..."> 标签
        3. 提取每个 <img> 的 title 属性
        4. 根据优先级选择最高级别的标签

        Args:
            event_html: 赛事HTML片段（包含 <h4 class="name">）

        Returns:
            str: 赛事级别（如：'a类', '金标', '中国田径协会', 等）
        """
        try:
            soup = BeautifulSoup(event_html, 'html.parser')

            # 找到 <h4 class="name"> 标签
            name_tag = soup.find('h4', class_='name')
            if not name_tag:
                logger.debug("未找到 <h4 class='name'> 标签")
                return '其他'

            # 找到所有 tag 链接
            tag_links = name_tag.find_all('a', href=lambda h: h and '/tag/' in h)

            if not tag_links:
                logger.debug("未找到任何 tag 标签")
                return '其他'

            # 提取所有标签的 title
            tags = []
            for tag_link in tag_links:
                img_tag = tag_link.find('img')
                if img_tag and img_tag.get('title'):
                    title = img_tag.get('title').strip()
                    tags.append(title)
                    logger.debug(f"  发现标签: {title}")

            if not tags:
                logger.debug("未提取到任何标签 title")
                return '其他'

            # 根据优先级选择级别
            event_level = EventLevelExtractorV2._select_best_level(tags)

            logger.debug(f"✓ 赛事级别: {event_level} (来自标签: {', '.join(tags)})")
            return event_level

        except Exception as e:
            logger.error(f"提取赛事级别失败: {e}")
            return '其他'

    @staticmethod
    def _select_best_level(tags: List[str]) -> str:
        """
        从标签列表中选择最佳级别

        优先级：
        1. 精确匹配赛事级别关键词（如：'a类', '金标'）
        2. 模糊匹配（如：'a类赛事' 包含 'a类'）
        3. 返回第一个标签（如果都不匹配）

        Args:
            tags: 标签列表（如：['中国马拉松', 'a类', '中国田径协会']）

        Returns:
            str: 最佳级别
        """
        if not tags:
            return '其他'

        best_level = None
        best_priority = -1

        # 1. 精确匹配
        for tag in tags:
            tag_lower = tag.lower()
            for keyword, priority in EventLevelExtractorV2.LEVEL_KEYWORDS.items():
                if tag_lower == keyword.lower():
                    if priority > best_priority:
                        best_level = tag
                        best_priority = priority
                        logger.debug(f"    精确匹配: {tag} (优先级: {priority})")

        if best_level:
            return best_level

        # 2. 模糊匹配（包含关键词）
        for tag in tags:
            tag_lower = tag.lower()
            for keyword, priority in EventLevelExtractorV2.LEVEL_KEYWORDS.items():
                if keyword.lower() in tag_lower:
                    if priority > best_priority:
                        best_level = tag
                        best_priority = priority
                        logger.debug(f"    模糊匹配: {tag} 包含 {keyword} (优先级: {priority})")

        if best_level:
            return best_level

        # 3. 返回第一个标签
        logger.debug(f"    无匹配，使用第一个标签: {tags[0]}")
        return tags[0]

    @staticmethod
    def extract_all_tags(event_html: str) -> List[str]:
        """
        提取所有标签（用于调试或其他用途）

        Args:
            event_html: 赛事HTML片段

        Returns:
            list: 所有标签的 title 列表
        """
        try:
            soup = BeautifulSoup(event_html, 'html.parser')
            name_tag = soup.find('h4', class_='name')
            if not name_tag:
                return []

            tag_links = name_tag.find_all('a', href=lambda h: h and '/tag/' in h)

            tags = []
            for tag_link in tag_links:
                img_tag = tag_link.find('img')
                if img_tag and img_tag.get('title'):
                    tags.append(img_tag.get('title').strip())

            return tags

        except Exception as e:
            logger.error(f"提取标签列表失败: {e}")
            return []


# 便捷函数
def extract_event_level_from_html(event_html: str) -> str:
    """
    便捷函数：从HTML提取赛事级别

    Args:
        event_html: 赛事HTML片段

    Returns:
        str: 赛事级别
    """
    return EventLevelExtractorV2.extract_event_level_from_html(event_html)


# 测试代码
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                       format='%(levelname)s - %(message)s')

    print("\n" + "=" * 80)
    print("赛事级别提取测试 V2（从HTML标签）")
    print("=" * 80)

    # 测试HTML
    test_html = """
    <div class="event">
        <div class="event-body">
            <h4 class="name">
                <a href="https://zuicool.com/event/26426" class="event-a">
                    2026重庆铜梁龙马拉松 
                </a>
                
                <a href="https://zuicool.com/tag/760548792" style="display: inline">
                    <img src="..." alt="" title="中国马拉松" style="width: 17px;">
                </a>
                <a href="https://zuicool.com/tag/912121853" style="display: inline">
                    <img src="..." alt="" title="a类" style="width: 17px;">
                </a>
                <a href="https://zuicool.com/tag/1519813803" style="display: inline">
                    <img src="..." alt="" title="中国田径协会" style="width: 17px;">
                </a>
            </h4>
        </div>
    </div>
    """

    print("\n【测试1】提取赛事级别")
    print("-" * 80)
    level = EventLevelExtractorV2.extract_event_level_from_html(test_html)
    print(f"\n结果: {level}")
    assert level == "a类", f"期望 'a类'，实际 '{level}'"
    print("✓ 通过\n")

    print("【测试2】提取所有标签")
    print("-" * 80)
    tags = EventLevelExtractorV2.extract_all_tags(test_html)
    print(f"\n所有标签: {tags}")
    assert tags == ["中国马拉松", "a类", "中国田径协会"], f"标签提取错误: {tags}"
    print("✓ 通过\n")

    # 测试HTML2 - 金标赛事
    test_html2 = """
    <h4 class="name">
        <a href="https://zuicool.com/event/12345" class="event-a">
            2026厦门国际马拉松
        </a>
        <a href="https://zuicool.com/tag/111" style="display: inline">
            <img src="..." title="金标" style="width: 17px;">
        </a>
        <a href="https://zuicool.com/tag/222" style="display: inline">
            <img src="..." title="国际田联" style="width: 17px;">
        </a>
    </h4>
    """

    print("【测试3】金标赛事")
    print("-" * 80)
    level2 = EventLevelExtractorV2.extract_event_level_from_html(test_html2)
    print(f"\n结果: {level2}")
    assert level2 == "国际田联", f"期望 '国际田联'（优先级更高），实际 '{level2}'"
    print("✓ 通过\n")

    # 测试HTML3 - 没有标签
    test_html3 = """
    <h4 class="name">
        <a href="https://zuicool.com/event/99999" class="event-a">
            2026某地马拉松
        </a>
    </h4>
    """

    print("【测试4】没有标签的赛事")
    print("-" * 80)
    level3 = EventLevelExtractorV2.extract_event_level_from_html(test_html3)
    print(f"\n结果: {level3}")
    assert level3 == "其他", f"期望 '其他'，实际 '{level3}'"
    print("✓ 通过\n")

    print("=" * 80)
    print("✓✓✓ 所有测试通过！")
    print("=" * 80)