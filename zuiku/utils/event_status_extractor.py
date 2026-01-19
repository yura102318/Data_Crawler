"""
赛事状态提取器 V3 - 最终版
✨ 基于详情页组别HTML精确判断
✨ 支持赛事整体状态和组别状态提取

赛事整体状态逻辑：
1. 有"一键报名"（不管是否有"已截止"） → "报名中"
2. 全部是"已截止"（没有"一键报名"） → "已截止"
3. 既没有"一键报名"也没有"已截止" → "已结束"

组别状态逻辑：
1. 按钮文本是"一键报名" → "报名中"
2. 按钮文本是"已截止" → "已截止"
3. 什么都没有 → None
"""
import re
import logging
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


class EventStatusExtractor:
    """赛事状态提取器 V3 - 基于组别HTML判断"""

    @staticmethod
    def extract_event_status_from_categories(detail_html: str) -> str:
        """
        从详情页组别列表中提取赛事整体状态

        判断逻辑（按优先级）：
        1. 有"一键报名"（不管是否有"已截止"） → "报名中"
        2. 全部是"已截止"（没有"一键报名"） → "已截止"
        3. 既没有"一键报名"也没有"已截止" → "已结束"

        Args:
            detail_html: 详情页HTML（包含组别列表）

        Returns:
            str: 赛事整体状态（报名中/已截止/已结束）
        """
        if not detail_html:
            logger.warning("详情页HTML为空，无法提取状态")
            return "已结束"

        try:
            soup = BeautifulSoup(detail_html, 'html.parser')

            # 查找所有组别（pkg2）
            category_divs = soup.find_all('div', class_='pkg2')

            if not category_divs:
                logger.debug("未找到组别列表，状态 → 已结束")
                return "已结束"

            # 统计各按钮数量
            has_register = False  # 是否有"一键报名"
            has_closed = False    # 是否有"已截止"

            for pkg in category_divs:
                # 查找所有报名按钮
                buttons = pkg.find_all('a', class_='reg-btn')

                for button in buttons:
                    button_text = button.get_text(strip=True)

                    if '一键报名' in button_text or '报名' in button_text:
                        has_register = True
                        logger.debug(f"  发现'一键报名'按钮: {button_text}")
                    elif '已截止' in button_text or '截止' in button_text:
                        has_closed = True
                        logger.debug(f"  发现'已截止'按钮: {button_text}")

            # 判断赛事整体状态
            if has_register:
                # 有"一键报名"（优先级最高）
                logger.info(f"✓ 赛事状态: 报名中（有一键报名按钮）")
                return "报名中"
            elif has_closed:
                # 全部是"已截止"
                logger.info(f"✓ 赛事状态: 已截止（全部已截止）")
                return "已截止"
            else:
                # 都没有
                logger.info(f"✓ 赛事状态: 已结束（无报名按钮）")
                return "已结束"

        except Exception as e:
            logger.error(f"提取赛事状态失败: {e}")
            return "已结束"

    @staticmethod
    def extract_category_info(category_html: str) -> Dict:
        """
        从组别HTML中提取组别状态和报名链接

        Args:
            category_html: 单个组别的HTML片段（pkg2 div）

        Returns:
            dict: {
                'registration_status': '报名中' | '已截止' | None,
                'registration_url': 'https://reg.zuicool.com/...' | None
            }
        """
        result = {
            'registration_status': None,
            'registration_url': None
        }

        if not category_html:
            return result

        try:
            soup = BeautifulSoup(category_html, 'html.parser')

            # 查找报名按钮
            buttons = soup.find_all('a', class_='reg-btn')

            for button in buttons:
                button_text = button.get_text(strip=True)
                button_href = button.get('href', '')

                # 提取报名链接（优先）
                if button_href and 'reg.zuicool.com' in button_href:
                    result['registration_url'] = button_href
                    logger.debug(f"    提取报名链接: {button_href}")

                # 提取组别状态
                if '一键报名' in button_text or '报名' in button_text:
                    result['registration_status'] = '报名中'
                    logger.debug(f"    组别状态: 报名中")
                elif '已截止' in button_text or '截止' in button_text:
                    result['registration_status'] = '已截止'
                    logger.debug(f"    组别状态: 已截止")

            # 如果没找到按钮，状态为None
            if result['registration_status'] is None:
                logger.debug(f"    组别状态: 无（NULL）")

        except Exception as e:
            logger.warning(f"提取组别信息失败: {e}")

        return result

    @staticmethod
    def parse_all_categories_status(detail_html: str) -> Tuple[str, List[Dict]]:
        """
        解析所有组别，返回赛事整体状态和组别列表

        Args:
            detail_html: 详情页HTML

        Returns:
            tuple: (event_status, categories_info)
            - event_status: 赛事整体状态
            - categories_info: 组别信息列表，每项包含：
                {
                    'html': '组别HTML',
                    'registration_status': '报名中/已截止/None',
                    'registration_url': '报名链接'
                }
        """
        categories_info = []

        if not detail_html:
            return "已结束", categories_info

        try:
            soup = BeautifulSoup(detail_html, 'html.parser')

            # 查找所有组别
            category_divs = soup.find_all('div', class_='pkg2')

            logger.info(f"找到 {len(category_divs)} 个组别")

            for idx, pkg in enumerate(category_divs, 1):
                pkg_html = str(pkg)

                # 提取组别信息
                info = EventStatusExtractor.extract_category_info(pkg_html)

                categories_info.append({
                    'html': pkg_html,
                    'registration_status': info['registration_status'],
                    'registration_url': info['registration_url']
                })

                logger.debug(f"  组别{idx}: 状态={info['registration_status']}, "
                           f"链接={info['registration_url']}")

            # 提取赛事整体状态
            event_status = EventStatusExtractor.extract_event_status_from_categories(detail_html)

            return event_status, categories_info

        except Exception as e:
            logger.error(f"解析组别状态失败: {e}")
            return "已结束", categories_info


# 便捷函数
def extract_event_status(detail_html: str) -> str:
    """
    便捷函数：提取赛事整体状态

    Args:
        detail_html: 详情页HTML

    Returns:
        str: 赛事状态（报名中/已截止/已结束）
    """
    return EventStatusExtractor.extract_event_status_from_categories(detail_html)


def extract_category_status_and_url(category_html: str) -> Dict:
    """
    便捷函数：提取组别状态和报名链接

    Args:
        category_html: 组别HTML

    Returns:
        dict: {'registration_status': ..., 'registration_url': ...}
    """
    return EventStatusExtractor.extract_category_info(category_html)


# 测试代码
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                       format='%(levelname)s - %(message)s')

    print("\n" + "=" * 80)
    print("赛事状态提取测试 V3（最终版 - 基于组别HTML）")
    print("=" * 80)

    # 测试HTML（根据用户提供的实际HTML）
    test_html = """
    <div class="section event-pkgs">
        <div class="pkgs-list">
            <!-- 组别1: 报名中 -->
            <div class="pkg2">
                <div class="col-xs-9 col-sm-8">
                    <h4 class="name">马拉松（42.195公里）</h4>
                </div>
                <div class="col-xs-2 hidden-xs text-right">
                    <div class="price">150.00</div>
                </div>
                <div class="col-xs-2 hidden-xs text-center">
                    <a href="https://reg.zuicool.com/16628/242803" 
                        class="btn btn-md btn-block btn-primary reg-btn">
                        一键报名
                    </a>
                </div>
            </div>
            
            <!-- 组别2: 已截止 -->
            <div class="pkg2">
                <div class="col-xs-9 col-sm-8">
                    <h4 class="name">半程马拉松（21.0975公里）</h4>
                </div>
                <div class="col-xs-2 hidden-xs text-right">
                    <div class="price">120.00</div>
                </div>
                <div class="col-xs-2 hidden-xs text-center">
                    <a href="https://reg.zuicool.com/16628/209767" 
                        class="btn btn-md btn-block btn-default reg-btn">
                        已截止
                    </a>
                </div>
            </div>
        </div>
    </div>
    """

    # 测试1: 提取赛事整体状态
    print("\n【测试1】提取赛事整体状态")
    print("-" * 80)
    event_status = EventStatusExtractor.extract_event_status_from_categories(test_html)
    print(f"赛事整体状态: {event_status}")
    assert event_status == "报名中", f"期望'报名中'，实际'{event_status}'"
    print("✓ 通过（有一键报名 → 报名中）\n")

    # 测试2: 解析所有组别
    print("【测试2】解析所有组别信息")
    print("-" * 80)
    event_status, categories = EventStatusExtractor.parse_all_categories_status(test_html)
    print(f"赛事整体状态: {event_status}")
    print(f"组别数量: {len(categories)}")

    for idx, cat in enumerate(categories, 1):
        print(f"\n组别{idx}:")
        print(f"  报名状态: {cat['registration_status']}")
        print(f"  报名链接: {cat['registration_url']}")

    assert len(categories) == 2, "应该有2个组别"
    assert categories[0]['registration_status'] == "报名中", "组别1应该是报名中"
    assert categories[0]['registration_url'] == "https://reg.zuicool.com/16628/242803", "组别1链接错误"
    assert categories[1]['registration_status'] == "已截止", "组别2应该是已截止"
    assert categories[1]['registration_url'] == "https://reg.zuicool.com/16628/209767", "组别2链接错误"
    print("\n✓ 通过（组别信息提取正确）\n")

    # 测试3: 全部已截止
    print("【测试3】全部已截止的情况")
    print("-" * 80)
    test_html_closed = """
    <div class="pkgs-list">
        <div class="pkg2">
            <a href="https://reg.zuicool.com/111/111" class="reg-btn">已截止</a>
        </div>
        <div class="pkg2">
            <a href="https://reg.zuicool.com/222/222" class="reg-btn">已截止</a>
        </div>
    </div>
    """
    status = EventStatusExtractor.extract_event_status_from_categories(test_html_closed)
    print(f"赛事整体状态: {status}")
    assert status == "已截止", f"期望'已截止'，实际'{status}'"
    print("✓ 通过（全部已截止）\n")

    # 测试4: 都没有
    print("【测试4】都没有的情况")
    print("-" * 80)
    test_html_finished = """
    <div class="pkgs-list">
        <div class="pkg2">
            <h4>马拉松</h4>
            <div class="price">150</div>
        </div>
    </div>
    """
    status = EventStatusExtractor.extract_event_status_from_categories(test_html_finished)
    print(f"赛事整体状态: {status}")
    assert status == "已结束", f"期望'已结束'，实际'{status}'"
    print("✓ 通过（都没有 → 已结束）\n")

    print("=" * 80)
    print("✓✓✓ 所有测试通过！")
    print("=" * 80)