"""
智能数据补全器 - 生产级多源验证系统
特性:
1. 多数据源补全(马拉松专业平台、官方报名系统等)
2. 数据置信度评分
3. 历史数据智能推断
4. 多源交叉验证
5. 异常值检测
"""
import logging
from typing import Dict, List, Optional, Tuple
from config import QWEN_API_KEY, QWEN_API_BASE
import httpx
import json
import requests
import time
import re
from bs4 import BeautifulSoup
from urllib.parse import quote, urljoin
from datetime import datetime
from collections import defaultdict
import statistics

logger = logging.getLogger(__name__)


class SmartDataCompleter:
    """智能数据补全器"""

    def __init__(self):
        self.api_key = QWEN_API_KEY
        self.api_base = QWEN_API_BASE
        self.search_model = "qwen-plus"

        # 数据源权重配置(用于置信度计算)
        self.source_weights = {
            'official_website': 1.0,  # 官方网站
            'registration_platform': 0.95,  # 报名平台
            'marathon_media': 0.85,  # 马拉松媒体
            'wechat_official': 0.90,  # 官方公众号
            'ai_search': 0.70,  # AI搜索
            'inference': 0.60,  # 推断数据
        }

        # 马拉松标准配置(用于推断和验证)
        self.standard_races = {
            '全程马拉松': {'distance': '42.195km', 'keywords': ['全马', '全程', 'marathon', '42']},
            '半程马拉松': {'distance': '21.0975km', 'keywords': ['半马', '半程', 'half', '21']},
            '10公里': {'distance': '10km', 'keywords': ['10公里', '10km', '10k']},
            '5公里': {'distance': '5km', 'keywords': ['5公里', '5km', '5k']},
            '迷你马拉松': {'distance': '5km', 'keywords': ['迷你', 'mini']},
            '健康跑': {'distance': '3km', 'keywords': ['健康跑', '健康']},
        }

        # 历史数据缓存(用于推断)
        self.historical_data = {}

    # ==================== 核心补全流程 ====================

    def complete_event_data(self, event_name: str, structured_data: Dict) -> Dict:
        """智能补全赛事数据 - 主入口"""
        logger.info(f"\n{'=' * 80}")
        logger.info(f"🎯 智能数据补全: {event_name}")
        logger.info(f"{'=' * 80}")

        # 步骤1: 检查缺失字段
        missing_info = self.check_missing_fields(structured_data)

        if not missing_info['has_missing']:
            logger.info("✓ 数据完整,无需补全")
            return structured_data

        logger.info(f"\n📋 缺失字段分析:")
        logger.info(f"  - 赛事总规模: {'缺失' if missing_info['missing_total_scale'] else '完整'}")
        logger.info(f"  - 不完整组别数: {len(missing_info['incomplete_categories'])}")

        # 步骤2: 多源数据收集
        logger.info(f"\n🔍 启动多源数据收集...")
        multi_source_data = self.collect_from_multiple_sources(event_name, missing_info)

        # 步骤3: 数据融合与验证
        logger.info(f"\n🔬 数据融合与交叉验证...")
        validated_data = self.validate_and_merge(multi_source_data, missing_info)

        # 步骤4: 智能推断补充
        logger.info(f"\n🧠 智能推断补充...")
        final_data = self.intelligent_inference(structured_data, validated_data, event_name)

        # 步骤5: 合并到原始数据
        logger.info(f"\n📊 合并数据...")
        result = self.merge_completed_data(structured_data, final_data)

        # 步骤6: 最终验证
        final_missing = self.check_missing_fields(result)

        completion_rate = self.calculate_completion_rate(missing_info, final_missing)
        logger.info(f"\n✅ 补全完成率: {completion_rate:.1f}%")

        if final_missing['has_missing']:
            logger.warning(f"⚠ 仍有部分字段缺失")
        else:
            logger.info(f"✓✓✓ 所有关键字段已完整补全")

        return result

    # ==================== 多源数据收集 ====================

    def collect_from_multiple_sources(self, event_name: str, missing_info: Dict) -> List[Dict]:
        """从多个数据源收集信息"""
        sources_data = []

        # 数据源1: AI联网搜索(通义千问)
        logger.info(f"  [源1] AI联网搜索...")
        ai_data = self.source1_ai_search(event_name, missing_info)
        if ai_data:
            ai_data['source_type'] = 'ai_search'
            ai_data['confidence_base'] = self.source_weights['ai_search']
            sources_data.append(ai_data)
            logger.info(f"    ✓ 获取到数据")

        # 数据源2: 马拉松专业平台搜索
        logger.info(f"  [源2] 马拉松平台搜索...")
        platform_data = self.source2_marathon_platforms(event_name, missing_info)
        if platform_data:
            platform_data['source_type'] = 'registration_platform'
            platform_data['confidence_base'] = self.source_weights['registration_platform']
            sources_data.append(platform_data)
            logger.info(f"    ✓ 获取到数据")

        # 数据源3: 百度精准搜索
        logger.info(f"  [源3] 百度精准搜索...")
        baidu_data = self.source3_baidu_precise_search(event_name, missing_info)
        if baidu_data:
            baidu_data['source_type'] = 'marathon_media'
            baidu_data['confidence_base'] = self.source_weights['marathon_media']
            sources_data.append(baidu_data)
            logger.info(f"    ✓ 获取到数据")

        # 数据源4: 微信公众号深度搜索
        logger.info(f"  [源4] 微信公众号搜索...")
        wechat_data = self.source4_wechat_deep_search(event_name, missing_info)
        if wechat_data:
            wechat_data['source_type'] = 'wechat_official'
            wechat_data['confidence_base'] = self.source_weights['wechat_official']
            sources_data.append(wechat_data)
            logger.info(f"    ✓ 获取到数据")

        logger.info(f"\n  📊 共收集到 {len(sources_data)} 个数据源的信息")
        return sources_data

    def source1_ai_search(self, event_name: str, missing_info: Dict) -> Optional[Dict]:
        """数据源1: AI联网搜索"""
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

            prompt = self._build_precise_search_prompt(event_name, missing_info)

            response = client.chat.completions.create(
                model=self.search_model,
                messages=[
                    {
                        "role": "system",
                        "content": """你是专业的马拉松赛事数据收集专家。你必须:
1. 通过搜索引擎查找真实准确的赛事信息
2. 优先从官方渠道获取(官网、官方公众号、官方报名平台)
3. 返回的数据必须准确,不能编造
4. 如果找不到某个字段,填写null而不是猜测
5. 明确标注信息来源和可信度"""
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)

            # 验证数据合理性
            if self._validate_ai_response(result, event_name):
                return result
            else:
                logger.warning("    AI返回数据未通过验证")
                return None

        except Exception as e:
            logger.error(f"    AI搜索失败: {e}")
            return None

    def source2_marathon_platforms(self, event_name: str, missing_info: Dict) -> Optional[Dict]:
        """数据源2: 马拉松专业平台(最酷、马拉马拉、爱燃烧等)"""
        try:
            # 构建搜索关键词
            keywords = [
                f"{event_name} 报名",
                f"{event_name} 组别",
                f"{event_name} 费用"
            ]

            all_results = []

            # 搜索马拉松平台
            platforms = [
                'zuicool.com',
                'malama.com.cn',
                'iranshao.com',
                'marathon.org.cn'
            ]

            for platform in platforms:
                for keyword in keywords[:2]:  # 每个平台搜索2个关键词
                    query = f"site:{platform} {keyword}"
                    results = self._baidu_search(query, num=3)
                    all_results.extend(results)
                    time.sleep(1)

            if not all_results:
                return None

            # 使用AI从搜索结果中提取信息
            extracted = self._extract_from_search_results(
                event_name,
                all_results,
                missing_info,
                source_hint="马拉松专业平台"
            )

            return extracted

        except Exception as e:
            logger.error(f"    马拉松平台搜索失败: {e}")
            return None

    def source3_baidu_precise_search(self, event_name: str, missing_info: Dict) -> Optional[Dict]:
        """数据源3: 百度精准搜索"""
        try:
            # 构建高精度搜索查询
            queries = []

            if missing_info.get('missing_total_scale'):
                queries.append(f'"{event_name}" 赛事规模 人数')
                queries.append(f'"{event_name}" 总名额')

            if missing_info.get('incomplete_categories'):
                queries.append(f'"{event_name}" 组别 距离 费用')
                queries.append(f'"{event_name}" 报名费 全马 半马')

            all_results = []
            for query in queries[:3]:
                results = self._baidu_search(query, num=5)
                all_results.extend(results)
                time.sleep(1.5)

            if not all_results:
                return None

            # 提取信息
            extracted = self._extract_from_search_results(
                event_name,
                all_results,
                missing_info,
                source_hint="百度搜索"
            )

            return extracted

        except Exception as e:
            logger.error(f"    百度搜索失败: {e}")
            return None

    def source4_wechat_deep_search(self, event_name: str, missing_info: Dict) -> Optional[Dict]:
        """数据源4: 微信公众号深度搜索"""
        try:
            # 使用搜狗微信搜索
            search_queries = [
                f"{event_name} 报名",
                f"{event_name} 竞赛规程",
                f"{event_name} 招募"
            ]

            all_articles = []
            for query in search_queries[:2]:
                articles = self._sogou_wechat_search(query, num=5)
                all_articles.extend(articles)
                time.sleep(1)

            if not all_articles:
                return None

            # 从公众号文章中提取信息
            extracted = self._extract_from_search_results(
                event_name,
                all_articles,
                missing_info,
                source_hint="微信公众号"
            )
            return extracted

        except Exception as e:
            logger.error(f"    微信搜索失败: {e}")
            return None

    # ==================== 数据验证与融合 ====================

    def validate_and_merge(self, sources_data: List[Dict], missing_info: Dict) -> Dict:
        """验证并融合多源数据"""
        if not sources_data:
            return {}

        logger.info(f"  🔬 开始交叉验证...")

        # 对每个字段进行多源验证
        merged = {
            'total_scale': None,
            'race_categories': [],
            'sources_used': [],
            'confidence_scores': {}
        }

        # 验证总规模
        total_scales = []
        for source in sources_data:
            if source.get('total_scale') and source['total_scale'] != 'null':
                try:
                    scale = int(re.sub(r'[^\d]', '', str(source['total_scale'])))
                    if 100 <= scale <= 100000:  # 合理范围
                        total_scales.append({
                            'value': scale,
                            'source': source.get('source'),
                            'confidence': source.get('confidence_base', 0.5)
                        })
                except:
                    pass

        if total_scales:
            # 使用加权平均和置信度
            merged['total_scale'] = self._weighted_consensus(total_scales)
            merged['confidence_scores']['total_scale'] = self._calculate_field_confidence(total_scales)
            logger.info(
                f"    ✓ 总规模: {merged['total_scale']} (置信度: {merged['confidence_scores']['total_scale']:.2f})")

        # 验证组别信息
        all_categories = []
        for source in sources_data:
            if source.get('race_categories'):
                for cat in source['race_categories']:
                    if self._is_valid_category(cat):
                        cat['source'] = source.get('source')
                        cat['confidence'] = source.get('confidence_base', 0.5)
                        all_categories.append(cat)

        if all_categories:
            # 按组别名称分组并验证
            merged['race_categories'] = self._merge_categories_with_validation(all_categories)
            logger.info(f"    ✓ 组别: {len(merged['race_categories'])} 个")

        # 记录使用的数据源
        merged['sources_used'] = list(set([s.get('source', 'unknown') for s in sources_data if s.get('source')]))
        merged['overall_confidence'] = sum(merged['confidence_scores'].values()) / len(merged['confidence_scores']) if \
        merged['confidence_scores'] else 0.5

        logger.info(f"    📊 整体置信度: {merged['overall_confidence']:.2f}")

        return merged

    def _merge_categories_with_validation(self, categories: List[Dict]) -> List[Dict]:
        """合并并验证组别数据"""
        # 按组别名称分组
        grouped = defaultdict(list)
        for cat in categories:
            name = self._normalize_category_name(cat.get('name', ''))
            if name:
                grouped[name].append(cat)

        merged_cats = []
        for name, cats in grouped.items():
            if len(cats) == 1:
                # 只有一个来源
                merged_cats.append(cats[0])
            else:
                # 多个来源,需要验证和融合
                merged_cat = self._merge_single_category(cats, name)
                if merged_cat:
                    merged_cats.append(merged_cat)

        return merged_cats

    def _merge_single_category(self, categories: List[Dict], name: str) -> Optional[Dict]:
        """融合单个组别的多源数据"""
        merged = {'name': name}

        # 距离验证
        distances = [c.get('distance') for c in categories if c.get('distance') and c['distance'] != 'null']
        if distances:
            # 检查一致性
            unique_distances = list(set(distances))
            if len(unique_distances) == 1:
                merged['distance'] = unique_distances[0]
                merged['distance_confidence'] = 1.0
            else:
                # 有分歧,使用出现次数最多的
                merged['distance'] = max(set(distances), key=distances.count)
                merged['distance_confidence'] = distances.count(merged['distance']) / len(distances)

        # 费用验证(取中位数避免异常值)
        fees = []
        for c in categories:
            if c.get('fee') and c['fee'] != 'null':
                try:
                    fee = float(re.sub(r'[^\d.]', '', str(c['fee'])))
                    if 0 < fee < 1000:  # 合理范围
                        fees.append(fee)
                except:
                    pass

        if fees:
            if len(fees) == 1:
                merged['fee'] = str(fees[0])
                merged['fee_confidence'] = 1.0
            else:
                # 使用中位数
                median_fee = statistics.median(fees)
                merged['fee'] = str(median_fee)
                # 计算置信度(费用差异小于20%认为一致)
                variations = [abs(f - median_fee) / median_fee for f in fees]
                merged['fee_confidence'] = 1.0 - (sum(variations) / len(variations))

        # 名额验证
        quotas = []
        for c in categories:
            if c.get('total_quota') and c['total_quota'] != 'null':
                try:
                    quota = int(re.sub(r'[^\d]', '', str(c['total_quota'])))
                    if 10 <= quota <= 50000:
                        quotas.append(quota)
                except:
                    pass

        if quotas:
            if len(quotas) == 1:
                merged['total_quota'] = str(quotas[0])
                merged['quota_confidence'] = 1.0
            else:
                # 使用中位数
                median_quota = int(statistics.median(quotas))
                merged['total_quota'] = str(median_quota)
                # 计算置信度
                variations = [abs(q - median_quota) / median_quota for q in quotas]
                merged['quota_confidence'] = 1.0 - (sum(variations) / len(variations))

        # 计算组别整体置信度
        confidences = []
        for field in ['distance_confidence', 'fee_confidence', 'quota_confidence']:
            if field in merged:
                confidences.append(merged[field])

        if confidences:
            merged['overall_confidence'] = sum(confidences) / len(confidences)

        return merged if len(merged) > 1 else None

    # ==================== 智能推断 ====================

    def intelligent_inference(self, original_data: Dict, validated_data: Dict, event_name: str) -> Dict:
        """智能推断缺失数据"""
        result = validated_data.copy()

        # 推断1: 基于赛事名称推断组别和距离
        if not result.get('race_categories') or len(result['race_categories']) == 0:
            logger.info("  🧠 基于赛事名称推断组别...")
            inferred_cats = self._infer_categories_from_name(event_name)
            if inferred_cats:
                for cat in inferred_cats:
                    cat['inferred'] = True
                    cat['confidence'] = self.source_weights['inference']
                result['race_categories'] = inferred_cats
                logger.info(f"    ✓ 推断出 {len(inferred_cats)} 个组别")

        # 推断2: 补全组别的距离
        if result.get('race_categories'):
            for cat in result['race_categories']:
                if not cat.get('distance') or cat['distance'] == 'null':
                    inferred_distance = self._infer_distance_from_name(cat.get('name', ''))
                    if inferred_distance:
                        cat['distance'] = inferred_distance
                        cat['distance_inferred'] = True
                        logger.info(f"    ✓ 推断 {cat['name']} 距离: {inferred_distance}")

        # 推断3: 基于历史数据推断费用范围
        if result.get('race_categories'):
            for cat in result['race_categories']:
                if not cat.get('fee') or cat['fee'] == 'null':
                    inferred_fee = self._infer_fee_from_history(cat.get('name', ''), event_name)
                    if inferred_fee:
                        cat['fee'] = inferred_fee
                        cat['fee_inferred'] = True
                        logger.info(f"    ✓ 推断 {cat['name']} 费用: {inferred_fee}")

        # 推断4: 计算总规模(如果缺失)
        if not result.get('total_scale') or result['total_scale'] == 'null':
            if result.get('race_categories'):
                total = 0
                for cat in result['race_categories']:
                    if cat.get('total_quota') and cat['total_quota'] != 'null':
                        try:
                            quota = int(re.sub(r'[^\d]', '', str(cat['total_quota'])))
                            total += quota
                        except:
                            pass

                if total > 0:
                    result['total_scale'] = str(total)
                    result['total_scale_inferred'] = True
                    logger.info(f"    ✓ 推断总规模: {total}")

        return result

    def _infer_categories_from_name(self, event_name: str) -> List[Dict]:
        """从赛事名称推断组别"""
        categories = []
        name_lower = event_name.lower()

        # 检查包含哪些组别关键词
        for race_name, config in self.standard_races.items():
            for keyword in config['keywords']:
                if keyword in name_lower:
                    categories.append({
                        'name': race_name,
                        'distance': config['distance'],
                        'fee': 'null',
                        'total_quota': 'null'
                    })
                    break

        # 如果没有检测到,默认推断常见组别
        if not categories:
            if '马拉松' in event_name or 'marathon' in name_lower:
                categories = [
                    {'name': '全程马拉松', 'distance': '42.195km', 'fee': 'null', 'total_quota': 'null'},
                    {'name': '半程马拉松', 'distance': '21.0975km', 'fee': 'null', 'total_quota': 'null'},
                ]

        return categories

    def _infer_distance_from_name(self, category_name: str) -> Optional[str]:
        """从组别名称推断距离"""
        if not category_name:
            return None

        name_lower = category_name.lower()

        for race_name, config in self.standard_races.items():
            for keyword in config['keywords']:
                if keyword in name_lower:
                    return config['distance']

        # 尝试从名称中提取数字
        distance_match = re.search(r'(\d+\.?\d*)\s*(公里|km|k)', name_lower)
        if distance_match:
            number = distance_match.group(1)
            return f"{number}km"

        return None

    def _infer_fee_from_history(self, category_name: str, event_name: str) -> Optional[str]:
        """基于历史数据推断费用"""
        # 这里可以从数据库查询历史数据
        # 暂时使用经验值

        fee_ranges = {
            '全程马拉松': (100, 180),
            '半程马拉松': (80, 150),
            '10公里': (60, 100),
            '5公里': (50, 80),
            '迷你马拉松': (50, 80),
        }

        for race_type, (min_fee, max_fee) in fee_ranges.items():
            if race_type in category_name:
                # 返回中间值
                avg_fee = (min_fee + max_fee) / 2
                return f"{avg_fee:.2f}"

        return None

    # ==================== 辅助函数 ====================

    def check_missing_fields(self, structured_data: Dict) -> Dict:
        """检查缺失字段"""
        missing = {
            'has_missing': False,
            'missing_fields': [],
            'incomplete_categories': [],
            'missing_total_scale': False
        }

        # 检查总规模
        total_scale = structured_data.get('total_scale')
        if not total_scale or total_scale == 'null' or str(total_scale).strip() == '':
            missing['has_missing'] = True
            missing['missing_total_scale'] = True
            missing['missing_fields'].append('total_scale')

        # 检查组别
        race_categories = structured_data.get('race_categories', [])
        if not race_categories:
            missing['has_missing'] = True
            missing['missing_fields'].append('race_categories')
        else:
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

        return missing

    def merge_completed_data(self, original_data: Dict, completed_data: Dict) -> Dict:
        """合并原始数据和补全数据"""
        if not completed_data:
            return original_data

        merged = original_data.copy()

        # 补全总规模
        if completed_data.get('total_scale') and completed_data['total_scale'] != 'null':
            if not merged.get('total_scale') or merged.get('total_scale') == 'null':
                merged['total_scale'] = completed_data['total_scale']

        # 补全组别
        if completed_data.get('race_categories'):
            if not merged.get('race_categories') or len(merged['race_categories']) == 0:
                merged['race_categories'] = completed_data['race_categories']
            else:
                # 智能合并
                original_cats = merged['race_categories']
                completed_cats = completed_data['race_categories']

                for orig_cat in original_cats:
                    orig_name = self._normalize_category_name(orig_cat.get('name', ''))

                    for comp_cat in completed_cats:
                        comp_name = self._normalize_category_name(comp_cat.get('name', ''))

                        if orig_name and comp_name and self._is_similar_category_name(orig_name, comp_name):
                            # 补全缺失字段
                            for field in ['distance', 'fee', 'total_quota']:
                                if (not orig_cat.get(field) or orig_cat.get(field) == 'null') and \
                                        comp_cat.get(field) and comp_cat.get(field) != 'null':
                                    orig_cat[field] = comp_cat[field]
                            break

                # 添加新组别
                orig_names = {self._normalize_category_name(c.get('name', '')) for c in original_cats}
                for comp_cat in completed_cats:
                    comp_name = self._normalize_category_name(comp_cat.get('name', ''))
                    if comp_name and comp_name not in orig_names and self._is_valid_category(comp_cat):
                        original_cats.append(comp_cat)

                merged['race_categories'] = original_cats

        # 记录补全信息
        merged['completion_sources'] = completed_data.get('sources_used', [])
        merged['completion_confidence'] = completed_data.get('overall_confidence', 0.5)

        return merged

    def calculate_completion_rate(self, before: Dict, after: Dict) -> float:
        """计算补全率"""
        total_fields = 1  # 总规模
        completed_fields = 0

        if not after.get('missing_total_scale'):
            completed_fields += 1

        # 统计组别字段
        if before.get('incomplete_categories'):
            total_fields += len(before['incomplete_categories']) * 4  # 每个组别4个字段

            if after.get('incomplete_categories'):
                completed_fields += (len(before['incomplete_categories']) - len(after['incomplete_categories'])) * 4
            else:
                completed_fields += len(before['incomplete_categories']) * 4

        return (completed_fields / total_fields * 100) if total_fields > 0 else 100.0

    # ==================== 搜索工具 ====================

    def _baidu_search(self, query: str, num: int = 10) -> List[Dict]:
        """百度搜索"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            url = f"https://www.baidu.com/s?wd={quote(query)}&rn={num}"

            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'utf-8'

            soup = BeautifulSoup(response.text, 'html.parser')
            results = []

            for item in soup.find_all('div', class_='result')[:num]:
                try:
                    title_tag = item.find('h3') or item.find('a')
                    title = title_tag.get_text(strip=True) if title_tag else ''

                    abstract_tag = item.find('div', class_='c-abstract') or item.find('span',
                                                                                      class_='content-right_8Zs40')
                    abstract = abstract_tag.get_text(strip=True) if abstract_tag else ''

                    link_tag = item.find('a')
                    link = link_tag.get('href', '') if link_tag else ''

                    if title and abstract:
                        results.append({'title': title, 'abstract': abstract, 'link': link})
                except Exception as e:
                    continue

            return results

        except Exception as e:
            logger.error(f"百度搜索失败: {e}")
            return []

    def _sogou_wechat_search(self, query: str, num: int = 10) -> List[Dict]:
        """搜狗微信搜索"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            url = f"https://weixin.sogou.com/weixin?type=2&query={quote(query)}"

            response = requests.get(url, headers=headers, timeout=10)
            response.encoding = 'utf-8'

            soup = BeautifulSoup(response.text, 'html.parser')
            results = []

            for item in soup.find_all('div', class_='txt-box')[:num]:
                try:
                    title_tag = item.find('h3') or item.find('a')
                    title = title_tag.get_text(strip=True) if title_tag else ''

                    abstract_tag = item.find('p', class_='txt-info')
                    abstract = abstract_tag.get_text(strip=True) if abstract_tag else ''

                    if title and abstract:
                        results.append({'title': title, 'abstract': abstract, 'link': ''})
                except:
                    continue

            return results

        except Exception as e:
            logger.error(f"搜狗微信搜索失败: {e}")
            return []

    def _extract_from_search_results(self, event_name: str, results: List[Dict],
                                     missing_info: Dict, source_hint: str = "") -> Optional[Dict]:
        """使用AI从搜索结果中提取信息"""
        if not results:
            return None

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

            # 整理搜索结果
            search_content = ""
            for idx, result in enumerate(results[:10], 1):
                search_content += f"\n【结果{idx}】\n"
                search_content += f"标题: {result.get('title', '')}\n"
                search_content += f"摘要: {result.get('abstract', '')}\n"

            prompt = f"""请从以下搜索结果中提取「{event_name}」的准确信息:

{search_content}

请提取以下信息并返回JSON格式:
{{
  "total_scale": "赛事总规模(纯数字)",
  "race_categories": [
    {{
      "name": "组别名称",
      "distance": "距离(如42.195km)",
      "fee": "费用(纯数字)",
      "total_quota": "名额(纯数字)"
    }}
  ],
  "source": "信息来源({source_hint})",
  "confidence": "high/medium/low"
}}

要求:
1. 只提取确定的信息,不确定的填null
2. 数字必须准确
3. 明确标注可信度"""

            response = client.chat.completions.create(
                model=self.search_model,
                messages=[
                    {"role": "system", "content": "你是数据提取专家,只提取确定的信息"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            return result

        except Exception as e:
            logger.error(f"从搜索结果提取信息失败: {e}")
            return None

    def _build_precise_search_prompt(self, event_name: str, missing_info: Dict) -> str:
        """构建精确的搜索提示词"""
        prompt_parts = [
            f"【任务】搜索「{event_name}」的准确赛事信息",
            "",
            "【搜索策略】",
            "1. 优先搜索: 官方网站、官方公众号、官方报名平台",
            "2. 备选来源: 马拉松专业媒体(马拉马拉、爱燃烧等)",
            "3. 关键词: 竞赛规程、报名简章、赛事手册",
            "",
            "【需要补全的信息】"
        ]

        if missing_info.get('missing_total_scale'):
            prompt_parts.append("✓ 赛事总规模(所有组别总人数)")

        if missing_info.get('incomplete_categories'):
            prompt_parts.append("✓ 组别详细信息:")
            for item in missing_info['incomplete_categories']:
                cat = item['category']
                prompt_parts.append(f"  - {cat.get('name', '未知组别')}: 需要{', '.join(item['missing_fields'])}")

        prompt_parts.extend([
            "",
            "【返回格式】",
            "{",
            '  "total_scale": "纯数字",',
            '  "race_categories": [',
            '    {"name": "组别名", "distance": "42.195km", "fee": "120.00", "total_quota": "5000"}',
            '  ],',
            '  "source": "信息来源URL",',
            '  "confidence": "high/medium/low"',
            "}",
            "",
            "【关键要求】",
            "1. 必须搜索真实信息,不能编造",
            "2. 优先官方渠道",
            "3. 找不到的字段填null",
            "4. 标注信息来源和可信度"
        ])

        return '\n'.join(prompt_parts)

    def _validate_ai_response(self, data: Dict, event_name: str) -> bool:
        """验证AI返回的数据合理性"""
        if not data:
            return False

        # 检查数据结构
        if 'race_categories' in data:
            for cat in data['race_categories']:
                # 验证距离格式
                if cat.get('distance') and cat['distance'] != 'null':
                    if not re.match(r'\d+\.?\d*km', str(cat['distance'])):
                        return False

                # 验证费用范围
                if cat.get('fee') and cat['fee'] != 'null':
                    try:
                        fee = float(re.sub(r'[^\d.]', '', str(cat['fee'])))
                        if fee < 0 or fee > 1000:
                            return False
                    except:
                        return False

        return True

    def _is_valid_category(self, category: Dict) -> bool:
        """检查组别是否有效"""
        return (
                category.get('name') and category['name'] != 'null' and
                category.get('distance') and category['distance'] != 'null'
        )

    def _normalize_category_name(self, name: str) -> str:
        """规范化组别名称"""
        if not name:
            return ''

        name = name.strip().lower()

        # 统一全马表述
        if any(k in name for k in ['全程', '全马', 'full', 'marathon', '42']):
            return '全程马拉松'
        # 统一半马表述
        elif any(k in name for k in ['半程', '半马', 'half', '21']):
            return '半程马拉松'
        # 统一10公里
        elif '10' in name or '十公里' in name:
            return '10公里'
        # 统一5公里
        elif '5' in name or '五公里' in name:
            return '5公里'

        return name

    def _is_similar_category_name(self, name1: str, name2: str) -> bool:
        """判断两个组别名称是否相似"""
        n1 = self._normalize_category_name(name1)
        n2 = self._normalize_category_name(name2)
        return n1 == n2

    def _weighted_consensus(self, values: List[Dict]) -> str:
        """加权共识算法"""
        if not values:
            return 'null'

        if len(values) == 1:
            return str(values[0]['value'])

        # 计算加权平均
        total_weight = sum(v['confidence'] for v in values)
        weighted_sum = sum(v['value'] * v['confidence'] for v in values)

        result = int(weighted_sum / total_weight) if total_weight > 0 else values[0]['value']
        return str(result)

    def _calculate_field_confidence(self, values: List[Dict]) -> float:
        """计算字段置信度"""
        if not values:
            return 0.0

        if len(values) == 1:
            return values[0]['confidence']

        # 多源验证,检查一致性
        nums = [v['value'] for v in values]
        avg = sum(nums) / len(nums)

        # 计算变异系数
        variations = [abs(n - avg) / avg for n in nums if avg > 0]
        avg_variation = sum(variations) / len(variations) if variations else 0

        # 置信度 = 基础置信度 * (1 - 变异系数)
        base_confidence = sum(v['confidence'] for v in values) / len(values)
        consistency_factor = max(0, 1 - avg_variation)

        return base_confidence * consistency_factor