import requests
import base64
import logging
from typing import List, Dict
from config import QWEN_API_KEY, QWEN_API_BASE
import httpx

logger = logging.getLogger(__name__)


class ImageAnalyzer:
    """图片分析器 - 使用通义千问视觉模型"""

    def __init__(self):
        self.api_key = QWEN_API_KEY
        self.api_base = QWEN_API_BASE
        self.vision_model = "qwen-vl-max"

    def download_image(self, image_url: str) -> bytes:
        """下载图片"""
        try:
            response = requests.get(image_url, timeout=30)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.error(f"下载图片失败: {image_url}, 错误: {e}")
            return None

    def encode_image(self, image_data: bytes) -> str:
        """将图片编码为base64"""
        return base64.b64encode(image_data).decode('utf-8')

    def analyze_image(self, image_url: str) -> str:
        """分析单张图片"""
        try:
            from openai import OpenAI

            # 创建客户端，不使用proxies参数
            client = OpenAI(
                api_key=self.api_key,
                base_url=self.api_base,
                http_client=httpx.Client(
                    base_url=self.api_base,
                    follow_redirects=True,
                )
            )

            # 使用URL直接分析（通义千问支持）
            response = client.chat.completions.create(
                model=self.vision_model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {"url": image_url}
                            },
                            {
                                "type": "text",
                                "text": "请详细描述这张图片的内容，特别是关于赛事的信息，包括：赛事名称、时间、地点、项目、费用、规模等任何文字信息。"
                            }
                        ]
                    }
                ],
                max_tokens=1000
            )

            return response.choices[0].message.content

        except Exception as e:
            logger.error(f"分析图片失败: {image_url}, 错误: {e}")
            return ""

    def analyze_images_batch(self, image_urls: List[str]) -> List[Dict]:
        """批量分析图片"""
        results = []

        for idx, url in enumerate(image_urls, 1):
            logger.info(f"分析图片 {idx}/{len(image_urls)}: {url}")

            analysis = self.analyze_image(url)
            if analysis:
                results.append({
                    'url': url,
                    'analysis': analysis
                })

        return results