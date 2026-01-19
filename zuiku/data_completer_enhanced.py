import logging
from typing import Dict, List, Optional
from config import QWEN_API_KEY, QWEN_API_BASE
import httpx
import json
import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import quote

logger = logging.getLogger(__name__)


class EnhancedDataCompleter:
    """增强版数据补全器 - 使用多层策略补全缺失的赛事信息"""

    def __init__(self):
        self.api_key = QWEN_API_KEY
        self.api_base = QWEN_API_BASE
        self.search_model = "qwen-plus"
        self.max_retries = 3  # 每层策略的最大重试次数

    def check_missing_fields(self, structured_data: Dict) -> Dict:
        """检查哪些关键字段缺失或不完整"""
        missing = {
            'has_missing': False,
            'missing_fields': [],
            'incomplete_categories': [],
            'missing_total_scale': False
        }

        # 检查赛事总规模
        total_scale = structured_data.get('total_scale')
        if not total_scale or total_scale == 'null' or str(total_scale).strip() == '':
            missing['has_missing'] = True
            missing['missing_total_scale'] = True
            missing['missing_fields'].append('total_scale')
            logger.warning("⚠ 缺失：赛事总规模")

        # 检查组别信息
        race_categories = structured_data.get('race_categories', [])

        if not race_categories or len(race_categories) == 0:
            missing['has_missing'] = True
            missing['missing_fields'].append('race_categories')
            logger.warning("⚠ 缺失：组别信息完全为空")
        else:
            # 检查每个组别的完整性
            for idx, category in enumerate(race_categories):
                incomplete_fields = []

                if not category.get('name') or category.get('name') == 'null':
                    incomplete_fields.append('name')

                if not category.get('distance') or category.get('distance') == 'null':
                    incomplete_fields.append('distance')

                if not category.get('fee') or category.get('fee') == 'null':
                    incomplete_fields.append('fee')

                if not category.get('total_quota') or category.get('total_quota') == 'null':
                    incomplete_fields.append('total_quota')

                if incomplete_fields:
                    missing['has_missing'] = True
                    missing['incomplete_categories'].append({
                        'index': idx,
                        'category': category,
                        'missing_fields': incomplete_fields
                    })
                    logger.warning(f"⚠ 组别 {idx + 1} ({category.get('name', '未知')}) 缺失字段: {', '.join(incomplete_fields)}")

        return missing

    # ==================== 第一层：优化的AI联网搜索 ====================

    def layer1_ai_search(self, event_name: str, missing_info: Dict, retry: int = 0) -> Optional[Dict]:
        """第一层：使用AI联网搜索补全数据（带重试机制）"""
        try:
            logger.info(f"🔍 [第1层] AI联网搜索 (尝试 {retry + 1}/{self.max_retries})")

            from openai import OpenAI

            client = OpenAI(
                api_key=self.api_key,
                base_url=self.api_base,
                http_client=httpx.Client(
                    base_url=self.api_base,
                    follow_redirects=True,
                )
            )

            # 构建更详细的搜索提示词
            prompt = self._build_enhanced_search_prompt(event_name, missing_info, retry)

            response = client.chat.completions.create(
                model=self.search_model,
                messages=[
                    {
                        "role": "system",
                        "content": "你是一个专业的马拉松赛事信息搜集助手。你必须通过搜索引擎查找赛事信息，并提取准确的组别、距离、报名费用和名额信息。请确保信息准确可靠，优先从官方公众号、官网等权威来源获取。"
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )

            result = response.choices[0].message.content
            completed_data = json.loads(result)

            # 验证返回的数据质量
            if self._validate_completed_data(completed_data, missing_info):
                logger.info(f"✓ [第1层] AI搜索成功")
                return completed_data
            else:
                logger.warning(f"⚠ [第1层] AI返回数据质量不足")
                return None

        except Exception as e:
            logger.error(f"✗ [第1层] AI搜索失败: {e}")
            return None

    def _build_enhanced_search_prompt(self, event_name: str, missing_info: Dict, retry: int) -> str:
        """构建增强版搜索提示词"""

        # 根据重试次数调整搜索策略
        search_tips = [
            "请搜索官方公众号文章、官网信息",
            "请尝试搜索赛事报名平台（如：最酷、马拉马拉、爱燃烧等）",
            "请搜索赛事新闻报道、跑友分享等多个来源"
        ]

        prompt_parts = [
            f"【重要任务】请帮我搜索「{event_name}」这个马拉松赛事的详细信息。",
            f"【搜索建议】{search_tips[min(retry, len(search_tips) - 1)]}",
            "",
            "【必须补全的信息】"
        ]

        # 列出缺失的字段
        if missing_info.get('missing_total_scale'):
            prompt_parts.append("✓ 赛事总规模/总人数")

        if missing_info.get('incomplete_categories'):
            prompt_parts.append("✓ 以下组别的详细信息：")
            for item in missing_info['incomplete_categories']:
                cat = item['category']
                missing_fields = item['missing_fields']
                cat_name = cat.get('name', '未知组别')
                prompt_parts.append(f"  · {cat_name}:")
                if 'name' in missing_fields:
                    prompt_parts.append(f"    - 组别名称")
                if 'distance' in missing_fields:
                    prompt_parts.append(f"    - 距离")
                if 'fee' in missing_fields:
                    prompt_parts.append(f"    - 报名费用")
                if 'total_quota' in missing_fields:
                    prompt_parts.append(f"    - 名额")

        prompt_parts.extend([
            "",
            "【返回格式】请严格按照以下JSON格式返回：",
            "{",
            '  "total_scale": "赛事总规模（纯数字，如：10000）",',
            '  "race_categories": [',
            '    {',
            '      "name": "组别名称（如：全程马拉松、半程马拉松）",',
            '      "distance": "距离（格式：42.195km、21.0975km）",',
            '      "fee": "报名费用（纯数字，如：120.00）",',
            '      "total_quota": "名额（纯数字，如：5000）"',
            '    }',
            '  ],',
            '  "source": "信息来源URL或名称",',
            '  "confidence": "high/medium/low"',
            "}",
            "",
            "【关键要求】",
            "1. 必须搜索真实的赛事信息，不要编造",
            "2. 所有数字必须准确（距离、费用、名额）",
            "3. 距离格式：42.195km、21.0975km、10km",
            "4. 费用和名额：纯数字，不带单位",
            "5. 如果某个字段确实找不到，填写null",
            "6. 必须列出所有组别（全马、半马、迷你等）",
            "7. total_scale = 所有组别名额之和"
        ])

        return '\n'.join(prompt_parts)

    # ==================== 第二层：百度搜索API ====================

    def layer2_baidu_search(self, event_name: str, missing_info: Dict) -> Optional[Dict]:
        """第二层：使用百度搜索API获取搜索结果，然后用AI提取"""
        try:
            logger.info(f"🔍 [第2层] 百度搜索API")

            # 构建搜索关键词
            search_queries = self._build_search_queries(event_name, missing_info)

            all_search_results = []

            for query in search_queries[:3]:  # 最多搜索3个关键词
                logger.info(f"  搜索关键词: {query}")

                # 使用百度搜索（通过爬取搜索结果页）
                search_results = self._baidu_search(query)

                if search_results:
                    all_search_results.extend(search_results)
                    logger.info(f"  获取到 {len(search_results)} 条结果")

                time.sleep(1)  # 避免请求过快

            if not all_search_results:
                logger.warning(f"⚠ [第2层] 未获取到搜索结果")
                return None

            # 使用AI从搜索结果中提取信息
            logger.info(f"  使用AI分析 {len(all_search_results)} 条搜索结果")
            completed_data = self._extract_from_search_results(event_name, all_search_results, missing_info)

            if completed_data and self._validate_completed_data(completed_data, missing_info):
                logger.info(f"✓ [第2层] 百度搜索成功")
                return completed_data
            else:
                logger.warning(f"⚠ [第2层] 数据提取失败")
                return None

        except Exception as e:
            logger.error(f"✗ [第2层] 百度搜索失败: {e}")
            return None

    def _build_search_queries(self, event_name: str, missing_info: Dict) -> List[str]:
        """构建搜索关键词列表"""
        queries = [
            f"{event_name} 报名费用 组别 名额",
            f"{event_name} 官方 招募",
            f"{event_name} 竞赛规程"
        ]
        return queries

    def _baidu_search(self, query: str, num_results: int = 5) -> List[Dict]:
        """使用百度搜索（爬取搜索结果页）"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            url = f"https://www.baidu.com/s?wd={quote(query)}&rn={num_results}"
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'utf-8'

            soup = BeautifulSoup(response.text, 'html.parser')

            results = []
            # 查找搜索结果
            for item in soup.find_all('div', class_='result')[:num_results]:
                try:
                    # 提取标题
                    title_tag = item.find('h3') or item.find('a')
                    title = title_tag.get_text(strip=True) if title_tag else ''

                    # 提取链接
                    link_tag = item.find('a')
                    link = link_tag.get('href') if link_tag else ''

                    # 提取摘要
                    abstract_tag = item.find('div', class_='c-abstract') or item.find('span', class_='content-right_8Zs40')
                    abstract = abstract_tag.get_text(strip=True) if abstract_tag else ''

                    if title and abstract:
                        results.append({
                            'title': title,
                            'link': link,
                            'abstract': abstract
                        })
                except Exception as e:
                    logger.debug(f"解析搜索结果项失败: {e}")
                    continue

            return results

        except Exception as e:
            logger.error(f"百度搜索失败: {e}")
            return []

    def _extract_from_search_results(self, event_name: str, search_results: List[Dict], missing_info: Dict) -> Optional[Dict]:
        """使用AI从搜索结果中提取信息"""
        try:
            from openai import OpenAI

            client = OpenAI(
                api_key=self.api_key,
                base_url=self.api_base,
                http_client=httpx.Client(
                    base_url=self.api_base,
                    follow_redirects=True,
                )
            )

            # 整理搜索结果文本
            search_text = f"赛事名称：{event_name}\n\n搜索结果：\n\n"
            for idx, result in enumerate(search_results[:10], 1):
                search_text += f"【结果{idx}】\n"
                search_text += f"标题：{result['title']}\n"
                search_text += f"摘要：{result['abstract']}\n\n"

            prompt = f"""
请从以下搜索结果中提取「{event_name}」的赛事信息。

{search_text}

请提取以下信息并返回JSON格式：
{{
  "total_scale": "赛事总规模（纯数字）",
  "race_categories": [
    {{
      "name": "组别名称",
      "distance": "距离（如：42.195km）",
      "fee": "报名费用（纯数字）",
      "total_quota": "名额（纯数字）"
    }}
  ],
  "source": "信息来源",
  "confidence": "high/medium/low"
}}

要求：
1. 只提取搜索结果中明确提到的信息
2. 找不到的字段填写null
3. 确保数字准确
"""

            response = client.chat.completions.create(
                model=self.search_model,
                messages=[
                    {"role": "system", "content": "你是数据提取专家，从搜索结果中提取准确的赛事信息。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )

            result = response.choices[0].message.content
            return json.loads(result)

        except Exception as e:
            logger.error(f"从搜索结果提取信息失败: {e}")
            return None

    # ==================== 第三层：微信公众号搜索 ====================

    def layer3_wechat_search(self, event_name: str, missing_info: Dict) -> Optional[Dict]:
        """第三层：搜索微信公众号文章"""
        try:
            logger.info(f"🔍 [第3层] 微信公众号搜索")

            # 使用搜狗微信搜索
            search_results = self._sogou_wechat_search(event_name)

            if not search_results:
                logger.warning(f"⚠ [第3层] 未找到公众号文章")
                return None

            logger.info(f"  找到 {len(search_results)} 篇公众号文章")

            # 使用AI从文章摘要中提取信息
            completed_data = self._extract_from_search_results(event_name, search_results, missing_info)

            if completed_data and self._validate_completed_data(completed_data, missing_info):
                logger.info(f"✓ [第3层] 公众号搜索成功")
                return completed_data
            else:
                logger.warning(f"⚠ [第3层] 数据提取失败")
                return None

        except Exception as e:
            logger.error(f"✗ [第3层] 公众号搜索失败: {e}")
            return None

    def _sogou_wechat_search(self, query: str, num_results: int = 5) -> List[Dict]:
        """使用搜狗微信搜索"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            url = f"https://weixin.sogou.com/weixin?type=2&query={quote(query)}"
            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'utf-8'

            soup = BeautifulSoup(response.text, 'html.parser')

            results = []
            for item in soup.find_all('div', class_='txt-box')[:num_results]:
                try:
                    title_tag = item.find('h3') or item.find('a')
                    title = title_tag.get_text(strip=True) if title_tag else ''

                    abstract_tag = item.find('p', class_='txt-info')
                    abstract = abstract_tag.get_text(strip=True) if abstract_tag else ''

                    if title and abstract:
                        results.append({
                            'title': title,
                            'abstract': abstract,
                            'link': ''
                        })
                except Exception as e:
                    logger.debug(f"解析公众号结果失败: {e}")
                    continue

            return results

        except Exception as e:
            logger.error(f"搜狗微信搜索失败: {e}")
            return []

    # ==================== 数据验证和合并 ====================

    def _validate_completed_data(self, completed_data: Dict, missing_info: Dict) -> bool:
        """验证补全数据的质量"""
        if not completed_data:
            return False

        # 检查是否至少补全了一些关键信息
        has_valid_data = False

        # 检查总规模
        if missing_info.get('missing_total_scale'):
            if completed_data.get('total_scale') and completed_data['total_scale'] != 'null':
                has_valid_data = True

        # 检查组别信息
        if completed_data.get('race_categories'):
            for cat in completed_data['race_categories']:
                if (cat.get('name') and cat['name'] != 'null' and
                    cat.get('distance') and cat['distance'] != 'null' and
                    cat.get('fee') and cat['fee'] != 'null'):
                    has_valid_data = True
                    break

        return has_valid_data

    def merge_completed_data(self, original_data: Dict, completed_data: Dict) -> Dict:
        """合并原始数据和补全数据"""
        if not completed_data:
            return original_data

        merged = original_data.copy()

        # 补全赛事总规模
        if completed_data.get('total_scale') and completed_data['total_scale'] != 'null':
            if not merged.get('total_scale') or merged.get('total_scale') == 'null':
                merged['total_scale'] = completed_data['total_scale']
                logger.info(f"  ✓ 补全赛事总规模: {completed_data['total_scale']}")

        # 补全组别信息
        if completed_data.get('race_categories'):
            if not merged.get('race_categories') or len(merged['race_categories']) == 0:
                merged['race_categories'] = completed_data['race_categories']
                logger.info(f"  ✓ 使用补全数据填充组别信息（共 {len(completed_data['race_categories'])} 个组别）")
            else:
                # 智能合并
                original_categories = merged['race_categories']
                completed_categories = completed_data['race_categories']

                for orig_cat in original_categories:
                    orig_name = orig_cat.get('name', '').strip()

                    for comp_cat in completed_categories:
                        comp_name = comp_cat.get('name', '').strip()

                        if orig_name and comp_name and self._is_similar_category_name(orig_name, comp_name):
                            # 补全缺失字段
                            for field in ['distance', 'fee', 'total_quota']:
                                if (not orig_cat.get(field) or orig_cat.get(field) == 'null') and comp_cat.get(field) and comp_cat.get(field) != 'null':
                                    orig_cat[field] = comp_cat[field]
                                    logger.info(f"  ✓ 补全 {orig_name} 的 {field}: {comp_cat[field]}")
                            break

                merged['race_categories'] = original_categories

        # 记录补全来源
        if completed_data.get('source'):
            merged['completion_source'] = completed_data['source']
        if completed_data.get('confidence'):
            merged['completion_confidence'] = completed_data['confidence']

        return merged

    def _is_similar_category_name(self, name1: str, name2: str) -> bool:
        """判断两个组别名称是否相似"""
        keywords = ['全程', '半程', '马拉松', '迷你', '健康跑', '亲子跑', '10公里', '5公里', '10km', '5km']
        name1_lower = name1.lower()
        name2_lower = name2.lower()

        for keyword in keywords:
            if keyword in name1_lower and keyword in name2_lower:
                return True
        return False

    # ==================== 主补全函数 ====================

    def complete_event_data(self, event_name: str, structured_data: Dict) -> Dict:
        """多层策略补全赛事数据"""
        logger.info(f"\n【数据补全】检查赛事: {event_name}")
        logger.info("-" * 80)

        # 检查缺失字段
        missing_info = self.check_missing_fields(structured_data)

        if not missing_info['has_missing']:
            logger.info("✓ 数据完整，无需补全")
            return structured_data

        logger.info(f"⚠ 发现缺失字段，启动多层补全策略...")

        # 第一层：AI联网搜索（带重试）
        for retry in range(self.max_retries):
            completed_data = self.layer1_ai_search(event_name, missing_info, retry)
            if completed_data:
                merged_data = self.merge_completed_data(structured_data, completed_data)
                # 再次检查是否还有缺失
                final_missing = self.check_missing_fields(merged_data)
                if not final_missing['has_missing']:
                    logger.info("✓✓✓ [第1层] 数据补全成功")
                    return merged_data
                else:
                    logger.info(f"⚠ [第1层] 部分字段仍缺失，继续重试...")
                    missing_info = final_missing
                    structured_data = merged_data

        # 第二层：百度搜索API
        logger.info(f"→ [第1层] 未完全补全，尝试第2层...")
        completed_data = self.layer2_baidu_search(event_name, missing_info)
        if completed_data:
            merged_data = self.merge_completed_data(structured_data, completed_data)
            final_missing = self.check_missing_fields(merged_data)
            if not final_missing['has_missing']:
                logger.info("✓✓✓ [第2层] 数据补全成功")
                return merged_data
            else:
                logger.info(f"⚠ [第2层] 部分字段仍缺失，继续尝试...")
                missing_info = final_missing
                structured_data = merged_data

        # 第三层：微信公众号搜索
        logger.info(f"→ [第2层] 未完全补全，尝试第3层...")
        completed_data = self.layer3_wechat_search(event_name, missing_info)
        if completed_data:
            merged_data = self.merge_completed_data(structured_data, completed_data)
            final_missing = self.check_missing_fields(merged_data)
            if not final_missing['has_missing']:
                logger.info("✓✓✓ [第3层] 数据补全成功")
                return merged_data
            else:
                logger.warning("⚠ [第3层] 部分字段仍缺失")
                return merged_data

        logger.warning("⚠ 所有补全策略已尝试，返回当前数据")
        return structured_data