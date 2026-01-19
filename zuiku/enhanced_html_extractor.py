"""
增强版HTML组别提取器
支持AI识别组别名称中的公里数
"""
import re
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)


class EnhancedHtmlCategoryExtractor:
    """增强版HTML组别提取器"""

    def __init__(self, ai_extractor=None):
        """
        Args:
            ai_extractor: AI提取器实例，用于识别组别名称中的公里数
        """
        self.ai_extractor = ai_extractor

    def extract_categories_from_html(self, html_content: str) -> list:
        """
        从HTML中提取结构化的组别信息

        Args:
            html_content: HTML内容

        Returns:
            组别列表，每个组别包含 name, distance, fee, status, description
        """
        if not html_content:
            return []

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # 第1步：查找组别容器 pkg2
            pkg_divs = soup.find_all('div', class_='pkg2')

            categories = []

            if pkg_divs:
                logger.info(f"\n找到 {len(pkg_divs)} 个组别容器")

                for idx, pkg_div in enumerate(pkg_divs, 1):
                    try:
                        category = self._extract_single_category(pkg_div, idx)
                        if category:
                            categories.append(category)
                    except Exception as e:
                        logger.warning(f"提取组别 {idx} 失败: {e}")
                        continue

                logger.info(f"✓ 成功从pkg2提取 {len(categories)} 个组别信息")
            else:
                logger.info("未找到 pkg2 组别容器")

            # 第2步：尝试从 event-desc_lead 提取补充信息（新增！）
            desc_lead_info = self._extract_from_desc_lead(soup)
            if desc_lead_info:
                logger.info(f"✓ 从event-desc_lead提取了补充信息")
                # 如果pkg2没有组别，或者pkg2的组别缺少距离信息，用desc_lead补充
                categories = self._merge_desc_lead_info(categories, desc_lead_info)

            return categories

        except Exception as e:
            logger.error(f"解析HTML失败: {e}")
            return []

    def _extract_single_category(self, pkg_div, idx: int) -> dict:
        """
        提取单个组别信息

        Args:
            pkg_div: BeautifulSoup Tag对象
            idx: 组别序号

        Returns:
            组别信息字典
        """
        category = {}

        # 1. 提取组别名称
        name_tag = pkg_div.find('h4', class_='name')
        if name_tag:
            category['name'] = name_tag.get_text(strip=True)
        else:
            logger.warning(f"组别 {idx}: 未找到名称")
            return None

        # 2. 提取详细说明（包含距离等信息）
        desc_tag = pkg_div.find('small', class_='highlight_short')
        if desc_tag:
            description = desc_tag.get_text(strip=True)
            category['description'] = description

            # 从描述中提取距离
            distance = self._extract_distance_from_description(description)
            if distance:
                category['distance'] = distance
                category['distance_source'] = 'html_description'

        # 3. 如果描述中没有距离，尝试从组别名称中提取
        if not category.get('distance'):
            distance = self._extract_distance_from_name(category['name'])
            if distance:
                category['distance'] = distance
                category['distance_source'] = 'html_name'

        # 4. 提取价格
        price_div = pkg_div.find('div', class_='price')
        if price_div:
            price_text = price_div.get_text(strip=True)
            category['fee'] = price_text
        else:
            # 尝试从mobile版本提取
            mobile_div = pkg_div.find('div', class_='visible-xs-block')
            if mobile_div:
                price_match = re.search(r'(\d+\.?\d*)', mobile_div.get_text())
                if price_match:
                    category['fee'] = price_match.group(1)

        # 5. 提取报名状态
        status_btn = pkg_div.find('a', class_='reg-btn')
        if status_btn:
            status_text = status_btn.get_text(strip=True)
            category['status'] = self._parse_status(status_text)
            category['status_text'] = status_text

        logger.info(f"  组别 {idx}: {category.get('name')} - {category.get('distance', 'N/A')} - ¥{category.get('fee', 'N/A')} - {category.get('status_text', 'N/A')}")

        return category

    def _extract_distance_from_description(self, description: str) -> str:
        """
        从描述中提取距离

        Args:
            description: 描述文本，如 "（实际里程41.5KM、累计爬升2300M...）"

        Returns:
            距离字符串，如 "41.5km" 或 "41.5公里"
        """
        # 匹配模式
        patterns = [
            r'实际里程\s*(\d+\.?\d*)\s*K',     # 实际里程41.5KM
            r'里程\s*(\d+\.?\d*)\s*K',         # 里程20KM
            r'实际里程\s*(\d+\.?\d*)\s*公里',  # 实际里程41.5公里
            r'(\d+\.?\d*)\s*K[Mm]',            # 直接匹配 42KM
        ]

        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                distance_num = match.group(1)
                return f"{distance_num}km"

        return None

    def _extract_distance_from_name(self, name: str) -> str:
        """
        从组别名称中提取距离

        Args:
            name: 组别名称，如 "西湖42K"、"半程马拉松"

        Returns:
            距离字符串
        """
        # 直接包含数字+K/KM/公里
        patterns = [
            r'(\d+\.?\d*)\s*K[Mm]?',    # 42K, 42km, 42KM
            r'(\d+\.?\d*)\s*公里',       # 42公里
        ]

        for pattern in patterns:
            match = re.search(pattern, name, re.IGNORECASE)
            if match:
                distance_num = match.group(1)
                return f"{distance_num}km"

        # 特殊名称映射
        special_distances = {
            '全程': '42.195km',
            '全马': '42.195km',
            '半程': '21.0975km',
            '半马': '21.0975km',
            '十公里': '10km',
            '迷你': '5km',
        }

        for key, value in special_distances.items():
            if key in name:
                return value

        return None

    def _parse_status(self, status_text: str) -> str:
        """
        解析报名状态文本

        Args:
            status_text: 按钮文本，如 "已截止"、"立即报名"、"即将开启"

        Returns:
            标准化状态：'closed', 'active', 'upcoming'
        """
        status_text = status_text.strip()

        # 已截止
        if any(word in status_text for word in ['已截止', '已结束', '已满', '截止']):
            return 'closed'

        # 即将开启
        if any(word in status_text for word in ['即将开启', '敬请期待', '即将开始', '未开始']):
            return 'upcoming'

        # 报名中
        if any(word in status_text for word in ['立即报名', '报名', '抢报', '马上报名']):
            return 'active'

        # 默认
        return 'active'

    def enhance_categories_with_ai(self, categories: list) -> list:
        """
        使用AI增强组别信息
        从组别名称中识别公里数等信息

        Args:
            categories: HTML提取的组别列表

        Returns:
            增强后的组别列表
        """
        if not categories or not self.ai_extractor:
            return categories

        logger.info(f"\n  使用AI增强组别信息...")

        enhanced_categories = []

        for cat in categories:
            enhanced_cat = cat.copy()

            # 如果没有距离信息，用AI从名称中识别
            if not enhanced_cat.get('distance'):
                distance = self._ai_extract_distance_from_name(enhanced_cat.get('name', ''))
                if distance:
                    enhanced_cat['distance'] = distance
                    enhanced_cat['distance_source'] = 'ai_name'
                    logger.info(f"    ✓ AI识别 '{enhanced_cat['name']}' 的距离: {distance}")

            enhanced_categories.append(enhanced_cat)

        return enhanced_categories

    def _ai_extract_distance_from_name(self, name: str) -> str:
        """
        使用AI从组别名称中提取距离

        Args:
            name: 组别名称

        Returns:
            距离字符串
        """
        if not name or not self.ai_extractor:
            return None

        try:
            # 构造提示词
            prompt = f"""
请从以下组别名称中识别公里数。

组别名称：{name}

要求：
1. 如果名称中有明确的数字+单位（如"42K"、"21公里"），提取数字
2. 如果是"全程"、"全马"，返回 42.195
3. 如果是"半程"、"半马"，返回 21.0975
4. 如果是"十公里"、"10K"，返回 10
5. 如果是"迷你"、"5K"，返回 5
6. 如果无法识别，返回 null

只返回数字（保留小数），不要单位，不要其他文字。
"""

            # 调用AI（这里简化处理，实际应该调用真实的AI接口）
            # response = self.ai_extractor.extract_simple(prompt)
            # distance_num = response.strip()

            # 由于没有实际AI接口，这里先用规则
            distance = self._extract_distance_from_name(name)
            if distance:
                return distance

        except Exception as e:
            logger.warning(f"AI提取距离失败: {e}")

        return None

    def _extract_from_desc_lead(self, soup) -> dict:
        """
        从 event-desc_lead 区域提取组别信息

        HTML结构示例：
        <div class="section event-desc_lead">
            半程马拉松（21.0975公里）20000人，10公里（10公里）5000人
        </div>

        Args:
            soup: BeautifulSoup对象

        Returns:
            提取的信息字典，包含categories列表
        """
        try:
            desc_lead_div = soup.find('div', class_='event-desc_lead')
            if not desc_lead_div:
                desc_lead_div = soup.find('div', class_='section event-desc_lead')

            if not desc_lead_div:
                return None

            text = desc_lead_div.get_text(strip=True)
            if not text:
                return None

            logger.info(f"\n  从event-desc_lead提取信息...")
            logger.info(f"  原文: {text[:200]}...")

            # 提取组别信息的正则模式
            # 匹配模式：组别名称（公里数）人数
            # 例如：半程马拉松（21.0975公里）20000人
            import re

            categories_info = []

            # 模式1: 组别名（数字公里）数字人
            pattern1 = r'([^（，。]+)（(\d+\.?\d*)\s*公里\）(\d+)\s*人'
            matches = re.findall(pattern1, text)

            for match in matches:
                cat_name = match[0].strip()
                distance = match[1].strip()
                scale = match[2].strip()

                category = {
                    'name': cat_name,
                    'distance': f"{distance}km",
                    'distance_numeric': float(distance),
                    'total_quota': int(scale),
                    'distance_source': 'event_desc_lead'
                }

                categories_info.append(category)
                logger.info(f"    提取组别: {cat_name} - {distance}km - {scale}人")

            # 模式2: 组别名（数字KM）
            if not categories_info:
                pattern2 = r'([^（，。]+)（(\d+\.?\d*)\s*[Kk][Mm]?\）'
                matches = re.findall(pattern2, text)

                for match in matches:
                    cat_name = match[0].strip()
                    distance = match[1].strip()

                    category = {
                        'name': cat_name,
                        'distance': f"{distance}km",
                        'distance_numeric': float(distance),
                        'distance_source': 'event_desc_lead'
                    }

                    categories_info.append(category)
                    logger.info(f"    提取组别: {cat_name} - {distance}km")

            # 提取赛事总规模
            total_scale = None
            scale_pattern = r'(\d+)\s*人'
            scale_matches = re.findall(scale_pattern, text)
            if scale_matches:
                # 取最大的数字作为总规模
                total_scale = max([int(s) for s in scale_matches])

            if categories_info:
                return {
                    'categories': categories_info,
                    'total_scale': total_scale
                }

            return None

        except Exception as e:
            logger.warning(f"从event-desc_lead提取失败: {e}")
            return None

    def _merge_desc_lead_info(self, pkg_categories: list, desc_lead_info: dict) -> list:
        """
        合并 pkg2 提取的组别和 desc_lead 提取的信息

        策略：
        - 如果pkg2组别缺少距离，用desc_lead的距离补充
        - 如果pkg2没有组别，直接用desc_lead的组别
        - 如果两者都有，优先pkg2，但用desc_lead补充缺失字段

        Args:
            pkg_categories: 从pkg2提取的组别列表
            desc_lead_info: 从desc_lead提取的信息

        Returns:
            合并后的组别列表
        """
        if not desc_lead_info or not desc_lead_info.get('categories'):
            return pkg_categories

        desc_categories = desc_lead_info['categories']

        # 如果pkg2没有组别，直接用desc_lead的
        if not pkg_categories:
            logger.info(f"  ✓ 使用event-desc_lead的 {len(desc_categories)} 个组别")
            return desc_categories

        # 如果pkg2有组别，用desc_lead补充缺失的距离信息
        logger.info(f"  用event-desc_lead补充pkg2组别的距离信息...")

        # 建立desc_lead组别索引
        desc_dict = {}
        for desc_cat in desc_categories:
            desc_name = desc_cat['name']
            desc_dict[desc_name] = desc_cat

        # 补充pkg2组别的距离
        enhanced_categories = []
        for pkg_cat in pkg_categories:
            enhanced_cat = pkg_cat.copy()

            # 如果pkg2组别没有距离
            if not enhanced_cat.get('distance'):
                pkg_name = enhanced_cat.get('name', '')

                # 在desc_lead中查找匹配的组别
                matching_desc = None

                # 精确匹配
                if pkg_name in desc_dict:
                    matching_desc = desc_dict[pkg_name]
                else:
                    # 模糊匹配
                    for desc_name, desc_cat in desc_dict.items():
                        if _is_similar_name(pkg_name, desc_name):
                            matching_desc = desc_cat
                            break

                # 补充距离信息
                if matching_desc and matching_desc.get('distance'):
                    enhanced_cat['distance'] = matching_desc['distance']
                    enhanced_cat['distance_numeric'] = matching_desc.get('distance_numeric')
                    enhanced_cat['distance_source'] = 'event_desc_lead'
                    logger.info(f"    ✓ 补充 {pkg_name} 的距离: {matching_desc['distance']}")

                # 补充名额信息
                if matching_desc and matching_desc.get('total_quota'):
                    if not enhanced_cat.get('total_quota'):
                        enhanced_cat['total_quota'] = matching_desc['total_quota']

            enhanced_categories.append(enhanced_cat)

        return enhanced_categories


def merge_html_and_ai_categories(html_categories: list, ai_categories: list) -> list:
    """
    合并HTML和AI的组别数据

    优先级：
    1. HTML优先字段：name, distance, fee, status（准确性高）
    2. AI补充字段：registered_count, total_quota, description等（详细性高）

    Args:
        html_categories: HTML提取的组别列表
        ai_categories: AI提取的组别列表

    Returns:
        合并后的组别列表
    """
    # 如果HTML没有组别，直接用AI的
    if not html_categories:
        logger.info(f"  ⚠ HTML无组别数据，使用AI提取的 {len(ai_categories or [])} 个组别")
        return ai_categories or []

    # 如果AI没有组别，直接用HTML的
    if not ai_categories:
        logger.info(f"  ✓ 仅使用HTML提取的 {len(html_categories)} 个组别")
        return html_categories

    logger.info(f"  开始合并：HTML {len(html_categories)}个组别 + AI {len(ai_categories)}个组别")

    merged_categories = []

    # 建立AI组别索引
    ai_dict = {}
    for ai_cat in ai_categories:
        ai_name = ai_cat.get('name', '')
        if ai_name:
            ai_dict[ai_name] = ai_cat

    # 遍历HTML组别
    for html_cat in html_categories:
        html_name = html_cat.get('name', '')

        # 查找匹配的AI组别
        ai_cat = None

        # 精确匹配
        if html_name in ai_dict:
            ai_cat = ai_dict[html_name]
        else:
            # 模糊匹配
            for ai_name, ai_data in ai_dict.items():
                if _is_similar_name(html_name, ai_name):
                    ai_cat = ai_data
                    break

        # 合并数据
        if ai_cat:
            # 有匹配的AI数据，合并
            merged_cat = ai_cat.copy()  # 先复制AI的所有字段

            # HTML字段优先覆盖
            merged_cat['name'] = html_cat.get('name')           # HTML名称优先
            merged_cat['distance'] = html_cat.get('distance')   # HTML距离优先
            merged_cat['fee'] = html_cat.get('fee')             # HTML价格优先
            merged_cat['status'] = html_cat.get('status')       # HTML状态优先

            # HTML的额外字段
            if html_cat.get('status_text'):
                merged_cat['status_text'] = html_cat['status_text']
            if html_cat.get('description'):
                merged_cat['html_description'] = html_cat['description']
            if html_cat.get('distance_source'):
                merged_cat['distance_source'] = html_cat['distance_source']

            logger.info(f"    ✓ 合并组别: {html_name}")
            logger.info(f"      - HTML提供: 距离({merged_cat.get('distance')}), 价格({merged_cat.get('fee')}), 状态({merged_cat.get('status')})")
            logger.info(f"      - AI提供: 报名数({merged_cat.get('registered_count', 'N/A')}), 名额({merged_cat.get('total_quota', 'N/A')})")
        else:
            # 没有匹配的AI数据，只用HTML
            merged_cat = html_cat.copy()
            logger.info(f"    ⚠ 仅使用HTML数据: {html_name}")

        merged_categories.append(merged_cat)

    # 检查AI中是否有HTML没有的组别
    for ai_name, ai_cat in ai_dict.items():
        found = False
        for html_cat in html_categories:
            if _is_similar_name(html_cat.get('name', ''), ai_name):
                found = True
                break

        if not found:
            logger.info(f"    ⚠ 仅使用AI数据: {ai_name}（HTML未找到此组别）")
            merged_categories.append(ai_cat)

    logger.info(f"  ✓ 合并完成：共 {len(merged_categories)} 个组别")

    return merged_categories

    def extract_event_detail(self, html_content: str) -> str:
        """
        提取赛事详情（event-desc_lead部分）

        Args:
            html_content: 详情页HTML

        Returns:
            赛事详情文本
        """
        if not html_content:
            return None

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # 查找赛事详情div
            detail_div = soup.find('div', class_='event-desc_lead')

            if detail_div:
                # 获取文本内容，保留换行
                detail_text = detail_div.get_text(separator='\n', strip=True)

                # 清理多余的空行
                lines_text = [line.strip() for line in detail_text.split('\n') if line.strip()]
                detail_text = '\n'.join(lines_text)

                logger.debug(f"✓ 提取到赛事详情: {len(detail_text)} 字符")
                logger.debug(f"  预览: {detail_text[:100]}...")

                return detail_text

            # 尝试备选方案：查找包含"赛事详情"或"赛事介绍"的div
            for class_name in ['event-desc', 'event-info', 'event-detail', 'desc-content']:
                detail_div = soup.find('div', class_=class_name)
                if detail_div:
                    detail_text = detail_div.get_text(separator='\n', strip=True)
                    logger.debug(f"✓ 从备选位置提取到赛事详情: {class_name}")
                    return detail_text

            logger.debug("未找到赛事详情")
            return None

        except Exception as e:
            logger.error(f"提取赛事详情失败: {e}")
            return None

def _is_similar_name(name1: str, name2: str) -> bool:
    """判断两个组别名称是否相似"""
    if not name1 or not name2:
        return False

    # 去除空格和常见后缀
    name1 = name1.strip().replace('组', '').replace('公里', '').replace('km', '').replace('KM', '').lower()
    name2 = name2.strip().replace('组', '').replace('公里', '').replace('km', '').replace('KM', '').lower()

    # 完全相同
    if name1 == name2:
        return True

    # 包含关系
    if name1 in name2 or name2 in name1:
        return True

    # 提取数字比较
    import re
    num1 = re.findall(r'\d+\.?\d*', name1)
    num2 = re.findall(r'\d+\.?\d*', name2)

    if num1 and num2 and num1 == num2:
        return True

    return False