import requests
from bs4 import BeautifulSoup
import time
import logging
from typing import List, Dict, Optional, Tuple  # ✅ 合并所有类型导入
from config import CRAWLER_CONFIG, USER_AGENT
import re
from urllib.parse import urljoin
from datetime import datetime, date
from utils.event_level_extractor import EventLevelExtractorV2  # ✨ 新增：从HTML标签提取


logger = logging.getLogger(__name__)


class RaceCrawler:
    """赛事爬虫"""

    def __init__(self):
        self.base_url = CRAWLER_CONFIG['base_url']
        self.events_url = CRAWLER_CONFIG['events_url']
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        })
        self._consecutive_old_pages = 0  # 提前终止计数器

    def get_page(self, url: str, retry: int = 0) -> Optional[str]:
        """获取页面内容"""
        try:
            response = self.session.get(
                url,
                timeout=CRAWLER_CONFIG['timeout']
            )
            response.raise_for_status()
            response.encoding = 'utf-8'
            return response.text
        except Exception as e:
            if retry < CRAWLER_CONFIG['retry_times']:
                logger.warning(f"获取页面失败，重试 {retry + 1}/{CRAWLER_CONFIG['retry_times']}: {url}")
                time.sleep(2 ** retry)
                return self.get_page(url, retry + 1)
            else:
                logger.error(f"获取页面失败: {url}, 错误: {e}")
                return None

    def get_event_list_urls(self, max_pages: int = None) -> List[str]:
        """获取所有赛事列表页的URL"""
        max_pages = max_pages or CRAWLER_CONFIG['max_pages']
        urls = []

        for page in range(1, max_pages + 1):
            url = f"{self.events_url}?page={page}&per-page={CRAWLER_CONFIG['per_page']}"
            urls.append(url)

        return urls

    def parse_categories(self, html: str) -> Tuple[str, List[Dict]]:
        """
        解析组别列表，提取赛事整体状态和所有组别信息

        从详情页组别HTML提取：
        1. 赛事整体状态（报名中/已截止/已结束）
        2. 每个组别的报名状态和链接

        Args:
            html: 详情页HTML

        Returns:
            tuple: (event_status, categories_list)
            - event_status: 赛事整体状态
            - categories_list: 组别信息列表
        """
        from utils.event_status_extractor import EventStatusExtractor

        # 使用新的状态提取器
        event_status, categories_info = EventStatusExtractor.parse_all_categories_status(html)

        logger.info(f"✓ 赛事整体状态: {event_status}")
        logger.info(f"✓ 找到组别数量: {len(categories_info)}")

        # 解析每个组别的详细信息
        categories = []

        for idx, cat_info in enumerate(categories_info, 1):
            try:
                cat_soup = BeautifulSoup(cat_info['html'], 'html.parser')

                # 提取组别名称
                name_tag = cat_soup.find('h4', class_='name')
                name = name_tag.get_text(strip=True) if name_tag else None

                # 从名称中提取距离（如果有括号）
                distance = None
                if name and '（' in name and '）' in name:
                    match = re.search(r'（(.+?)）', name)
                    if match:
                        distance = match.group(1)

                # 提取价格
                price_div = cat_soup.find('div', class_='price')
                fee = price_div.get_text(strip=True) if price_div else None

                # 提取其他可能的信息
                highlight = cat_soup.find('small', class_='highlight_short')
                highlight_text = highlight.get_text(strip=True) if highlight else None

                # 组合数据
                category = {
                    'name': name,
                    'distance': distance,
                    'fee': fee,
                    'highlight': highlight_text,
                    'registration_status': cat_info['registration_status'],
                    'registration_url': cat_info['registration_url']
                }

                categories.append(category)

                logger.info(f"  组别{idx}: {name} - "
                            f"状态:{cat_info['registration_status']} - "
                            f"链接:{cat_info['registration_url'][:50] if cat_info['registration_url'] else 'None'}...")

            except Exception as e:
                logger.error(f"解析组别{idx}失败: {e}")
                continue

        return event_status, categories

    def parse_event_list(self, html: str) -> List[Dict]:
        """解析赛事列表页，提取赛事URL"""
        soup = BeautifulSoup(html, 'html.parser')
        events = []

        # 找到所有赛事项
        event_divs = soup.find_all('div', class_='event')

        for event_div in event_divs:
            try:
                # 获取赛事链接
                name_tag = event_div.find('h4', class_='name')
                if not name_tag:
                    continue

                link_tag = name_tag.find('a', class_='event-a')
                if not link_tag:
                    continue

                event_url = link_tag.get('href')
                event_name = link_tag.get_text(strip=True)

                # 提取event_id
                event_id = event_url.split('/')[-1] if event_url else None

                if not event_id or not event_url:
                    continue

                # 获取基本信息
                info_div = event_div.find('div', class_='info')
                info_text = info_div.get_text(strip=True) if info_div else ''

                # 提取赛事日期
                event_date = None
                if info_div:
                    info_p = info_div.find('p')
                    if info_p:
                        info_p_text = info_p.get_text()
                        # 匹配日期：2026.03.15 或 2026-03-15
                        date_match = re.search(r'(\d{4})[.\-](\d{2})[.\-](\d{2})', info_p_text)
                        if date_match:
                            year = date_match.group(1)
                            month = date_match.group(2)
                            day = date_match.group(3)
                            event_date = f"{year}-{month}-{day}"

                # 获取报名截止时间
                meta_div = event_div.find('div', class_='meta')
                deadline = ''
                if meta_div:
                    deadline_span = meta_div.find('span', string=lambda t: t and '报名截止' in t)
                    if deadline_span:
                        deadline = deadline_span.get_text(strip=True)

                # ✨ 提取赛事级别（从HTML标签）
                event_level = EventLevelExtractorV2.extract_event_level_from_html(str(event_div))

                events.append({
                    'event_id': event_id,
                    'event_url': urljoin(self.base_url, event_url),
                    'name': event_name,
                    'basic_info': info_text,
                    'registration_deadline': deadline,
                    'event_date': event_date,
                    'event_level': event_level  # ✨ 新增：赛事级别
                })

                logger.debug(f"解析到赛事: {event_name}, 日期: {event_date}, URL: {event_url}, ID: {event_id}")

            except Exception as e:
                logger.error(f"解析赛事项失败: {e}")
                continue

        return events

    def parse_event_detail(self, html: str, event_url: str, event_id: str) -> Dict:
        """解析赛事详情页

        Args:
            html: 页面HTML
            event_url: 赛事详情页URL
            event_id: 赛事ID
        """
        soup = BeautifulSoup(html, 'html.parser')
        detail = {
            'raw_html': html,
            'event_url': event_url
        }

        try:
            # 获取页面所有文本内容（作为详情文本）
            # 移除script、style等标签
            for tag in soup(['script', 'style', 'header', 'footer', 'nav']):
                tag.decompose()

            # 获取主要内容区域的文本
            main_content = soup.find('div', class_='container') or soup.find('main') or soup.body
            if main_content:
                detail['detail_text'] = main_content.get_text(strip=True, separator='\n')
                logger.debug(f"获取到详情页文本，长度: {len(detail['detail_text'])} 字符")

            # 获取赛事名称
            title_tag = soup.find('h1') or soup.find('h2', class_='event-title')
            if title_tag:
                detail['name'] = title_tag.get_text(strip=True)

            # 获取赛事介绍
            desc_div = soup.find('div', class_='event-description') or \
                       soup.find('div', class_='description') or \
                       soup.find('div', class_='event-detail')
            if desc_div:
                detail['description'] = desc_div.get_text(strip=True)

            # 获取赛事时间和地点
            info_section = soup.find('div', class_='event-info')
            if info_section:
                info_items = info_section.find_all('p')
                for item in info_items:
                    text = item.get_text(strip=True)
                    if '时间' in text or '日期' in text:
                        detail['event_date'] = text
                    elif '地点' in text or '地址' in text:
                        detail['location'] = text

            # 获取组别信息
            categories_section = soup.find('div', class_='race-categories') or \
                                 soup.find('section', string=lambda t: t and '组别' in t)
            if categories_section:
                categories = []
                category_items = categories_section.find_all('li') or \
                                 categories_section.find_all('div', class_='category')
                for item in category_items:
                    categories.append(item.get_text(strip=True))
                detail['race_categories_text'] = categories

            # ✨✨✨ 新增：解析组别列表和赛事整体状态 ✨✨✨
            event_status, categories = self.parse_categories(html)
            detail['status'] = event_status  # 赛事整体状态
            detail['categories'] = categories  # 组别列表

            logger.info(f"✓ 赛事状态: {event_status}, 组别数量: {len(categories)}")
            # ✨✨✨ 新增结束 ✨✨✨

            # 重要：查找资讯链接
            # 方法1: 查找href包含 /news/archives/tag/event 的链接
            news_link = soup.find('a', href=re.compile(r'/news/archives/tag/event\d+'))
            if news_link:
                href = news_link.get('href')
                detail['news_url'] = urljoin(self.base_url, href)
                logger.info(f"✓ 找到资讯链接（方法1）: {detail['news_url']}")
            else:
                # 方法2: 根据event_id构造资讯URL
                constructed_url = f"https://zuicool.com/news/archives/tag/event{event_id}"
                detail['news_url'] = constructed_url
                logger.info(f"✓ 构造资讯链接（方法2）: {detail['news_url']}")

        except Exception as e:
            logger.error(f"解析详情页失败: {e}")

        return detail

    def get_news_list_content(self, news_list_url: str) -> Dict:
        """获取资讯列表页面的所有文章内容

        Args:
            news_list_url: 资讯列表页URL，如 https://zuicool.com/news/archives/tag/event81730

        Returns:
            包含所有文章内容和图片的字典
        """
        html = self.get_page(news_list_url)
        if not html:
            logger.warning(f"无法获取资讯列表页: {news_list_url}")
            return {'text': '', 'images': [], 'articles': []}

        soup = BeautifulSoup(html, 'html.parser')
        all_content = {
            'news_list_url': news_list_url,
            'text': '',
            'images': [],
            'articles': []
        }

        try:
            # 查找所有文章链接
            article_links = []

            # 方法1: 查找所有指向 /news/archives/数字 的链接
            all_links = soup.find_all('a', href=re.compile(r'/news/archives/\d+$'))
            for link in all_links:
                article_url = urljoin(self.base_url, link.get('href'))
                if article_url not in article_links:
                    article_links.append(article_url)

            # 方法2: 查找文章标题链接
            if not article_links:
                articles = soup.find_all('article')
                for article in articles:
                    title_link = article.find('a', class_='entry-title-link') or \
                                 article.find('h2').find('a') if article.find('h2') else None
                    if title_link:
                        article_url = urljoin(self.base_url, title_link.get('href'))
                        if article_url not in article_links:
                            article_links.append(article_url)

            logger.info(f"在资讯列表页找到 {len(article_links)} 篇文章")

            if not article_links:
                logger.warning(f"未找到文章链接，可能页面结构不同: {news_list_url}")
                return all_content

            # 获取每篇文章的内容（最多5篇）
            for idx, article_url in enumerate(article_links[:5], 1):
                logger.info(f"获取文章 {idx}/{min(len(article_links), 5)}: {article_url}")
                time.sleep(CRAWLER_CONFIG['delay'])

                article_content = self.get_article_content(article_url)
                if article_content and article_content['text']:
                    all_content['articles'].append(article_content)
                    all_content['text'] += f"\n\n=== 文章 {idx}: {article_url} ===\n{article_content['text']}\n"
                    all_content['images'].extend(article_content['images'])
                else:
                    logger.warning(f"文章内容为空: {article_url}")

            logger.info(f"共获取到文本长度: {len(all_content['text'])} 字符")
            logger.info(f"共获取到 {len(all_content['images'])} 张图片")

        except Exception as e:
            logger.error(f"解析资讯列表页失败: {e}")

        return all_content

    def get_article_content(self, article_url: str) -> Dict:
        """获取单篇文章的内容 - 只从entry-content获取"""
        html = self.get_page(article_url)
        if not html:
            return {'article_url': article_url, 'text': '', 'images': []}

        soup = BeautifulSoup(html, 'html.parser')
        content = {
            'article_url': article_url,
            'text': '',
            'images': []
        }

        try:
            # 只获取entry-content中的内容
            entry_content = soup.find('div', class_='entry-content')

            if entry_content:
                # 获取文本内容
                # 移除script和style标签
                for script in entry_content(['script', 'style']):
                    script.decompose()

                content['text'] = entry_content.get_text(strip=True, separator='\n')

                # 只获取entry-content中的图片
                img_tags = entry_content.find_all('img')
                for img in img_tags:
                    src = img.get('src') or img.get('data-src')
                    if src:
                        # 过滤掉小图标和logo
                        src_lower = src.lower()
                        if any(keyword in src_lower for keyword in ['logo', 'icon', 'avatar', 'favicon']):
                            continue

                        # 检查图片尺寸属性
                        width = img.get('width')
                        height = img.get('height')

                        if width and height:
                            try:
                                if int(width) < 100 or int(height) < 100:
                                    continue
                            except (ValueError, TypeError):
                                pass

                        full_url = urljoin(self.base_url, src)
                        content['images'].append(full_url)

                logger.debug(f"从文章中提取: 文本 {len(content['text'])} 字符, 图片 {len(content['images'])} 张")
            else:
                logger.warning(f"未找到entry-content: {article_url}")

        except Exception as e:
            logger.error(f"解析文章页失败: {e}")

        return content

    def crawl_all_events(self, min_date: str = None) -> List[Dict]:
        """爬取所有赛事（支持日期过滤 + 智能停止）

        因为网站排序混乱，提高停止阈值到20页

        Args:
            min_date: 最小日期，格式'YYYY-MM-DD'，不传则爬取所有赛事
        """
        # 只有传了min_date才过滤日期
        min_date_obj = None
        if min_date:
            min_date_obj = datetime.strptime(min_date, '%Y-%m-%d').date()
            logger.info(f"📅 时间过滤: >= {min_date}")
        else:
            logger.info(f"⚙️  不过滤日期，爬取到最后一页或200页为止")

        logger.info(f"⚠️  网站排序混乱，提高停止阈值到20页")

        all_events = []
        list_urls = self.get_event_list_urls()

        # 提高停止阈值（网站排序混乱）
        STOP_THRESHOLD = 20

        logger.info(f"开始爬取 {len(list_urls)} 个列表页")
        if min_date_obj:
            logger.info(f"⚙️  提前停止阈值: 连续 {STOP_THRESHOLD} 页无有效赛事")
        else:
            logger.info(f"⚙️  不过滤日期，爬取到最后一页或200页为止")

        for idx, url in enumerate(list_urls, 1):
            logger.info(f"爬取列表页 {idx}/{len(list_urls)}: {url}")

            html = self.get_page(url)
            if not html:
                continue

            events = self.parse_event_list(html)
            logger.info(f"从列表页解析到 {len(events)} 个赛事")

            # 日期过滤（如果有min_date_obj）
            page_valid_count = 0
            for event in events:
                # 如果没有设置日期过滤，所有赛事都有效
                if min_date_obj is None:
                    all_events.append(event)
                    page_valid_count += 1
                elif self._is_event_valid_by_date(event, min_date_obj):
                    all_events.append(event)
                    page_valid_count += 1
                else:
                    logger.debug(f"过滤已过期赛事: {event['name']} ({event.get('event_date', 'N/A')})")

            logger.info(f"过滤后本页有效赛事: {page_valid_count} 个")

            # 智能停止逻辑
            if min_date_obj is None:
                # 不过滤日期的情况：只要这页没有赛事就停止
                if not events:
                    logger.info("已到达最后一页")
                    break
            else:
                # 过滤日期的情况：连续20页无有效赛事才停止
                if page_valid_count == 0:
                    self._consecutive_old_pages += 1
                    if self._consecutive_old_pages % 5 == 0:  # 每5页提示一次
                        logger.info(f"连续 {self._consecutive_old_pages} 页无有效赛事")

                    if self._consecutive_old_pages >= STOP_THRESHOLD:
                        logger.info(f"连续{STOP_THRESHOLD}页无有效赛事，停止爬取")
                        break
                else:
                    self._consecutive_old_pages = 0

            # 延迟
            time.sleep(CRAWLER_CONFIG['delay'])

        logger.info(f"✓ 共爬取到 {len(all_events)} 个有效赛事")
        return all_events

    def crawl_event_detail(self, event: Dict) -> Dict:
        """爬取单个赛事的详细信息

        流程：
        1. 获取赛事详情页
        2. 从详情页获取资讯链接
        3. 获取资讯内容和图片
        """
        logger.info(f"→ 爬取赛事详情: {event['name']}")
        logger.info(f"  详情URL: {event['event_url']}")

        # 步骤1: 获取详情页
        html = self.get_page(event['event_url'])
        if not html:
            logger.error(f"✗ 无法获取详情页: {event['event_url']}")
            return event

        detail = self.parse_event_detail(html, event['event_url'], event['event_id'])
        event.update(detail)

        # 步骤2: 获取资讯内容
        if 'news_url' in event and event['news_url']:
            logger.info(f"→ 获取资讯内容")
            logger.info(f"  资讯URL: {event['news_url']}")

            time.sleep(CRAWLER_CONFIG['delay'])
            news_content = self.get_news_list_content(event['news_url'])

            event['news_content_raw'] = news_content.get('text', '')
            event['images'] = news_content.get('images', [])

            logger.info(f"✓ 资讯文本: {len(event['news_content_raw'])} 字符")
            logger.info(f"✓ 资讯图片: {len(event['images'])} 张")
        else:
            logger.warning(f"✗ 没有找到资讯URL: {event['name']}")
            event['news_content_raw'] = ''
            event['images'] = []

        return event

    def _is_event_valid_by_date(self, event: dict, min_date_obj: date) -> bool:
        """判断赛事是否有效（日期过滤）

        Args:
            event: 赛事信息
            min_date_obj: 最小日期对象

        Returns:
            是否有效
        """
        event_date = event.get('event_date')
        if not event_date:
            # 没有日期的赛事保留
            return True

        try:
            event_date_obj = datetime.strptime(event_date, '%Y-%m-%d').date()
            return event_date_obj >= min_date_obj
        except Exception as e:
            logger.warning(f"解析日期失败: {event_date} - {e}")
            return True  # 解析失败的也保留