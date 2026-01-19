import json
import logging
from typing import Dict
from config import QWEN_API_KEY, QWEN_API_BASE
import httpx

logger = logging.getLogger(__name__)


class DataExtractor:
    """数据提取器 - 使用大模型提取结构化数据"""

    def __init__(self):
        self.api_key = QWEN_API_KEY
        self.api_base = QWEN_API_BASE
        self.model = "qwen-plus"

    def extract_structured_data(self, raw_data: Dict) -> Dict:
        """从原始数据中提取结构化信息"""
        try:
            from openai import OpenAI

            # 创建客户端
            client = OpenAI(
                api_key=self.api_key,
                base_url=self.api_base,
                http_client=httpx.Client(
                    base_url=self.api_base,
                    follow_redirects=True,
                )
            )

            # 组织提示词
            prompt = self._build_extraction_prompt(raw_data)

            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "你是一个专业的数据提取助手，擅长从非结构化文本中提取赛事信息。请严格按照JSON格式返回数据，确保所有字段都存在。"
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
            extracted_data = json.loads(result)

            logger.debug(f"AI提取结果: {json.dumps(extracted_data, ensure_ascii=False, indent=2)}")

            return extracted_data

        except Exception as e:
            logger.error(f"提取结构化数据失败: {e}")
            return {}

    def _build_extraction_prompt(self, raw_data: Dict) -> str:
        """构建提取提示词"""

        # 整合所有文本信息
        all_text = []

        if raw_data.get('name'):
            all_text.append(f"赛事名称：{raw_data['name']}")

        if raw_data.get('detail_text'):
            all_text.append(f"详情页内容：{raw_data['detail_text']}")

        if raw_data.get('description'):
            all_text.append(f"赛事介绍：{raw_data['description']}")

        if raw_data.get('basic_info'):
            all_text.append(f"基本信息：{raw_data['basic_info']}")

        if raw_data.get('event_date'):
            all_text.append(f"日期信息：{raw_data['event_date']}")

        if raw_data.get('location'):
            all_text.append(f"地点信息：{raw_data['location']}")

        if raw_data.get('news_content_raw'):
            all_text.append(f"资讯内容：{raw_data['news_content_raw']}")

        if raw_data.get('race_categories_text'):
            all_text.append(f"组别信息：{', '.join(raw_data['race_categories_text'])}")

        if raw_data.get('images_analysis'):
            for idx, img in enumerate(raw_data['images_analysis'], 1):
                all_text.append(f"图片{idx}分析：{img.get('analysis', '')}")

        combined_text = '\n\n'.join(all_text)

        prompt = f"""
请从以下赛事信息中提取结构化数据，返回JSON格式。请尽可能完整地提取所有信息，如果某些字段无法确定，请填写null。

原始信息：
{combined_text}

请严格按照以下JSON格式返回数据（所有字段都必须存在）：
{{
    "event_date": "赛事举办日期（格式：YYYY-MM-DD）",
    "name": "赛事完整名称",
    "event_level": "赛事等级（如：A类、A1类、B类、C类、金标、银标、铜标等）",
    "location": "比赛地点省市县（县城如果有的情况下），请帮我保持格式统一，用-拼接，如：辽宁省-铁岭市-西丰县，如果有国外如：日本-东京都-新宿区 就变成：境外-日本-京都，如有不是地区的名称统一为：未知",
    "detailed_address": "详细地址（具体到街道、场馆、起点等）",
    "race_categories": [
        {{
            "name": "组别名称（如：全程马拉松、半程马拉松、10公里跑等）",
            "distance": "距离（格式为数字+km如：42.195km、21.0975km、10km等）",
            "fee": "该组别报名费用（不算早鸟价格，数字金额就可以如:60.00,100.00）",
            "zaoniao_fee":"该组别报名费用的早鸟价格（数字金额就可以如:60.00,100.00）",
            "registered_count": "该组别已报名人数（如果有的话）",
            "total_quota": "该组别名额（格式为数字即可，如果有的话）",
            "start_time":"组别开始时间，也是开抢时间，也是发枪时间"
            "cutoff_time":"组别关门时间，也是结束时间，可以为具体时间，也可以为小时，如果为几个小时最好是根据开始时间计算一下关门时间"
        }}
    ],
    "total_scale": "赛事总规模/总人数（所有组别加起来）格式为数字即可",
    "registration_fee": "报名费用范围（如：80-180,不需要金额单位）",
    "organizer": "运营单位/运营公司（必须获取到运营公司或者运营单位或者其他可能是运营单位的组织）",
    "host_units": "主办单位（必须获取到主办单位，多个用逗号分隔）",
    "co_organizers": "承办单位（必须获取到承办单位，如果有的情况下，多个用逗号分隔）",
    "supporters": "协办单位/支持单位（必须获取到协办单位或支持单位，如果有的情况下多个用逗号分隔）",
    "contact_phone": "联系电话/组委会电话（必须获取到联系电话如果有的情况下）",
    "contact_email": "组委会邮箱/联系邮箱（必须获取到联系邮箱，如果有的情况下）",
    "contact_person": "联系人",
    "registration_deadline": "报名截止时间，格式为日期或时间，不要带上中文",
    "status": "赛事状态（如：报名中、已截止、已取消、已延期等）"
}}

重要提示：
1. 日期格式统一为 YYYY-MM-DD（如：2025-12-28）
2. race_categories是数组，要列出所有组别，每个组别要尽可能包含费用和人数信息
3. 主办、承办、协办单位要分清楚，多个单位用逗号分隔这些尽量获取到
4. 费用信息要完整，包含数字和单位（元）
5. 联系方式（电话、邮箱）要准确提取
6. 如果信息中提到"取消"或"延期"，请在status字段标注
7. 所有字段都必须存在，找不到的填null，不要遗漏任何字段
"""

        return prompt