import logging
import logging.config
from config import LOG_CONFIG
from database import Database
# 测试
from crawler import RaceCrawler
from image_analyzer import ImageAnalyzer
from data_extractor import DataExtractor
from data_completer_enhanced import EnhancedDataCompleter
from enhanced_html_extractor import EnhancedHtmlCategoryExtractor

import time
import sys
import os
import concurrent.futures
from data_processor import EnhancedDataProcessor

os.system('chcp 65001')  # Windows 控制台切换 UTF-8 编码

# 配置日志，先用 dictConfig 进行配置
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger()


# 自定义安全编码日志处理器，定义一次即可
class SafeEncodingHandler(logging.StreamHandler):
    """安全编码的日志处理器，避免UnicodeEncodeError"""

    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except UnicodeEncodeError:
            # 发生编码错误时，忽略无法编码的字符
            safe_msg = msg.encode(stream.encoding, errors='ignore').decode(stream.encoding)
            stream.write(safe_msg + self.terminator)
            self.flush()


# 清除默认的 handler，添加自定义 handler
logger.handlers.clear()
handler = SafeEncodingHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class RaceCrawlerApp:
    """赛事爬虫应用"""

    def __init__(self):
        self.db = Database()
        self.crawler = RaceCrawler()
        self.image_analyzer = ImageAnalyzer()
        self.data_extractor = DataExtractor()
        self.data_completer = EnhancedDataCompleter()
        self.html_extractor = EnhancedHtmlCategoryExtractor()

        # ⭐ 新增：智能同步器
        from race_events_manager import DB_CONFIG

    def init(self):
        """初始化"""
        logger.info("初始化数据库...")
        self.db.init_db()
        logger.info("初始化完成\n")

    def _convert_to_mysql_format(self, sqlite_data: dict) -> list:
        """
        将SQLite数据格式转换为MySQL格式

        Args:
            sqlite_data: SQLite格式的数据（一个赛事包含多个组别）

        Returns:
            list: MySQL格式的数据（每个组别一条记录）
        """
        mysql_records = []

        # 基本信息
        event_id = sqlite_data.get('event_id')
        event_url = sqlite_data.get('event_url')
        event_name = sqlite_data.get('name')
        event_date = sqlite_data.get('event_date')
        event_level = sqlite_data.get('event_level')
        location = sqlite_data.get('location')
        detailed_address = sqlite_data.get('detailed_address')
        status = sqlite_data.get('status')
        total_scale = sqlite_data.get('total_scale')
        registration_fee = sqlite_data.get('registration_fee')
        organizer = sqlite_data.get('organizer')
        host_units = sqlite_data.get('host_units')
        co_organizers = sqlite_data.get('co_organizers')
        supporters = sqlite_data.get('supporters')
        contact_phone = sqlite_data.get('contact_phone')
        contact_email = sqlite_data.get('contact_email')
        contact_person = sqlite_data.get('contact_person')
        registration_deadline = sqlite_data.get('registration_deadline')

        # 组别信息
        categories = sqlite_data.get('race_categories', [])

        if not categories:
            # 如果没有组别，创建一条空记录
            mysql_records.append({
                'event_id': event_id,
                'event_url': event_url,
                'event_name': event_name,
                'event_date': event_date,
                'event_level': event_level,
                'location': location,
                'detailed_address': detailed_address,
                'status': status,
                'total_scale': total_scale,
                'registration_fee': registration_fee,
                'organizer': organizer,
                'host_units': host_units,
                'co_organizers': co_organizers,
                'supporters': supporters,
                'contact_phone': contact_phone,
                'contact_email': contact_email,
                'contact_person': contact_person,
                'registration_deadline': registration_deadline,
                'name': None,  # 组别名称
                'distance_numeric': None,
                'fee': None,
                'price_per_km': None,
                'zaoniao_fee': None,
                'total_quota': None,
                'start_time': None,
                'cutoff_time': None
            })
        else:
            # 每个组别创建一条记录
            for cat in categories:
                mysql_records.append({
                    'event_id': event_id,
                    'event_url': event_url,
                    'event_name': event_name,
                    'event_date': event_date,
                    'event_level': event_level,
                    'location': location,
                    'detailed_address': detailed_address,
                    'status': status,
                    'total_scale': total_scale,
                    'registration_fee': registration_fee,
                    'organizer': organizer,
                    'host_units': host_units,
                    'co_organizers': co_organizers,
                    'supporters': supporters,
                    'contact_phone': contact_phone,
                    'contact_email': contact_email,
                    'contact_person': contact_person,
                    'registration_deadline': registration_deadline,
                    'name': cat.get('name'),
                    'distance_numeric': cat.get('distance_numeric'),
                    'fee': cat.get('fee'),
                    'price_per_km': cat.get('price_per_km'),
                    'zaoniao_fee': cat.get('zaoniao_fee'),
                    'total_quota': cat.get('total_quota'),
                    'start_time': cat.get('start_time'),
                    'cutoff_time': cat.get('cutoff_time')
                })

        return mysql_records

    def _sync_to_mysql(self, manager, mysql_records: list) -> bool:
        """
        同步数据到MySQL

        Args:
            manager: RaceEventsManager实例
            mysql_records: MySQL格式的记录列表

        Returns:
            bool: 是否成功
        """
        try:
            import pymysql
            from datetime import datetime

            conn = manager._get_connection()
            cursor = conn.cursor()

            for record in mysql_records:
                event_id = record['event_id']
                name = record.get('name')

                # 查找是否已存在
                if name:
                    cursor.execute(
                        "SELECT id FROM race_categories WHERE event_id=%s AND name=%s",
                        (event_id, name)
                    )
                else:
                    cursor.execute(
                        "SELECT id FROM race_categories WHERE event_id=%s AND (name IS NULL OR name='')",
                        (event_id,)
                    )

                existing = cursor.fetchone()

                if existing:
                    # 更新
                    cursor.execute("""
                        UPDATE race_categories SET
                            event_url=%s, event_name=%s, event_date=%s, event_level=%s,
                            location=%s, detailed_address=%s, status=%s, total_scale=%s,
                            registration_fee=%s, organizer=%s, host_units=%s, co_organizers=%s,
                            supporters=%s, contact_phone=%s, contact_email=%s, contact_person=%s,
                            registration_deadline=%s, distance_numeric=%s, fee=%s, price_per_km=%s,
                            zaoniao_fee=%s, total_quota=%s, start_time=%s, cutoff_time=%s,
                            updated_at=%s
                        WHERE id=%s
                    """, (
                        record.get('event_url'), record.get('event_name'), record.get('event_date'),
                        record.get('event_level'), record.get('location'), record.get('detailed_address'),
                        record.get('status'), record.get('total_scale'), record.get('registration_fee'),
                        record.get('organizer'), record.get('host_units'), record.get('co_organizers'),
                        record.get('supporters'), record.get('contact_phone'), record.get('contact_email'),
                        record.get('contact_person'), record.get('registration_deadline'),
                        record.get('distance_numeric'), record.get('fee'), record.get('price_per_km'),
                        record.get('zaoniao_fee'), record.get('total_quota'), record.get('start_time'),
                        record.get('cutoff_time'), datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        existing[0]
                    ))
                else:
                    # 插入
                    cursor.execute("""
                        INSERT INTO race_categories (
                            event_id, event_url, event_name, event_date, event_level,
                            location, detailed_address, status, total_scale, registration_fee,
                            organizer, host_units, co_organizers, supporters,
                            contact_phone, contact_email, contact_person, registration_deadline,
                            name, distance_numeric, fee, price_per_km, zaoniao_fee,
                            total_quota, start_time, cutoff_time,
                            created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        record.get('event_id'), record.get('event_url'), record.get('event_name'),
                        record.get('event_date'), record.get('event_level'), record.get('location'),
                        record.get('detailed_address'), record.get('status'), record.get('total_scale'),
                        record.get('registration_fee'), record.get('organizer'), record.get('host_units'),
                        record.get('co_organizers'), record.get('supporters'), record.get('contact_phone'),
                        record.get('contact_email'), record.get('contact_person'), record.get('registration_deadline'),
                        record.get('name'), record.get('distance_numeric'), record.get('fee'),
                        record.get('price_per_km'), record.get('zaoniao_fee'), record.get('total_quota'),
                        record.get('start_time'), record.get('cutoff_time'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))

            conn.commit()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"MySQL同步错误: {e}")
            return False

    def process_event(self, event: dict):
        """处理单个赛事"""
        try:
            logger.info(f"\n" + "=" * 80)
            logger.info(f"处理赛事: {event['name']}")
            logger.info(f"赛事ID: {event['event_id']}")
            logger.info(f"详情URL: {event['event_url']}")
            logger.info("=" * 80)

            # 步骤1: 爬取详情和资讯
            logger.info("\n【步骤1/5】爬取赛事详情和资讯内容")
            logger.info("-" * 80)

            event_detail = self.crawler.crawl_event_detail(event)

            # 检查数据完整性
            if not event_detail.get('news_url'):
                logger.warning(f"⚠ 未找到资讯URL")
            else:
                logger.info(f"✓ 资讯URL: {event_detail['news_url']}")

            if not event_detail.get('news_content_raw'):
                logger.warning(f"⚠ 未获取到资讯内容")
            else:
                logger.info(f"✓ 资讯文本: {len(event_detail['news_content_raw'])} 字符")

            if not event_detail.get('detail_text'):
                logger.warning(f"⚠ 未获取到详情页文本")
            else:
                logger.info(f"✓ 详情文本: {len(event_detail['detail_text'])} 字符")

            # 步骤2: 分析图片
            logger.info(f"\n【步骤2/5】分析资讯图片")
            logger.info("-" * 80)

            images_analysis = []
            if event_detail.get('images') and len(event_detail['images']) > 0:
                logger.info(f"准备分析 {len(event_detail['images'])} 张图片（最多分析10张）")
                images_to_analyze = event_detail['images'][:10]

                try:
                    images_analysis = self.image_analyzer.analyze_images_batch(images_to_analyze)
                    logger.info(f"✓ 成功分析 {len(images_analysis)} 张图片")
                except Exception as e:
                    logger.error(f"✗ 图片分析失败: {e}")

                event_detail['images_analysis'] = images_analysis
            else:
                logger.info("跳过（没有找到图片）")
                event_detail['images_analysis'] = []

            # 步骤3: 提取结构化数据
            logger.info(f"\n【步骤3/6】使用AI模型提取结构化数据")
            logger.info("-" * 80)

            structured_data = {}
            try:
                structured_data = self.data_extractor.extract_structured_data(event_detail)
                if structured_data:
                    logger.info(f"✓ 成功提取结构化数据:")
                    logger.info(f"  · 赛事名称: {structured_data.get('name', 'null')}")
                    logger.info(f"  · 赛事日期: {structured_data.get('event_date', 'null')}")
                    logger.info(f"  · 赛事等级: {structured_data.get('event_level', 'null')}")
                    logger.info(f"  · 比赛地点: {structured_data.get('location', 'null')}")
                    logger.info(f"  · 详细地址: {structured_data.get('detailed_address', 'null')}")
                    logger.info(f"  · 组别数量: {len(structured_data.get('race_categories', []))}")
                    logger.info(f"  · 赛事总规模: {structured_data.get('total_scale', 'null')}")
                    logger.info(f"  · 报名费用: {structured_data.get('registration_fee', 'null')}")
                    logger.info(f"  · 运营单位: {structured_data.get('organizer', 'null')}")
                    logger.info(f"  · 主办单位: {structured_data.get('host_units', 'null')}")
                    logger.info(f"  · 承办单位: {structured_data.get('co_organizers', 'null')}")
                    logger.info(f"  · 联系电话: {structured_data.get('contact_phone', 'null')}")
                    logger.info(f"  · 联系邮箱: {structured_data.get('contact_email', 'null')}")

                    # 显示组别详情
                    if structured_data.get('race_categories'):
                        logger.info(f"  · 组别详情:")
                        for idx, cat in enumerate(structured_data['race_categories'], 1):
                            logger.info(f"    {idx}. {cat.get('name', 'unknown')} - "
                                        f"{cat.get('distance', 'unknown')} - "
                                        f"费用: {cat.get('fee', 'unknown')} - "
                                        f"已报: {cat.get('registered_count', 'unknown')}")
                else:
                    logger.warning("⚠ AI模型返回了空数据")
            except Exception as e:
                logger.error(f"✗ 提取结构化数据失败: {e}")

            # 步骤4: 智能补全缺失数据
            logger.info(f"\n【步骤4/6】智能补全缺失数据")
            logger.info("-" * 80)

            if structured_data:
                try:
                    event_name = structured_data.get('name') or event.get('name')
                    completed_data = self.data_completer.complete_event_data(event_name, structured_data)

                    # 使用补全后的数据
                    structured_data = completed_data

                    # 显示补全后的组别详情
                    if structured_data.get('race_categories'):
                        logger.info(f"\n补全后的组别详情:")
                        logger.info(f"  赛事总规模: {structured_data.get('total_scale', 'unknown')}")
                        for idx, cat in enumerate(structured_data['race_categories'], 1):
                            logger.info(f"  {idx}. {cat.get('name', 'unknown')} - "
                                        f"{cat.get('distance', 'unknown')} - "
                                        f"费用: {cat.get('fee', 'unknown')} - "
                                        f"名额: {cat.get('total_quota', 'unknown')}")
                except Exception as e:
                    logger.error(f"✗ 数据补全失败: {e}")
                    logger.warning("⚠ 将使用原始提取的数据")
            else:
                logger.info("跳过（没有提取到结构化数据）")

            # 步骤5: 合并数据
            logger.info(f"\n【步骤5/6】合并数据")
            logger.info("-" * 80)

            # 提取赛事详情
            detail_html = event_detail.get('raw_html')
            event_detail_text = None
            if detail_html:
                try:
                    event_detail_text = self.html_extractor.extract_event_detail(detail_html)
                    if event_detail_text:
                        logger.info(f"✓ 赛事详情提取成功: {len(event_detail_text)} 字符")
                    else:
                        logger.info("⚠️  未提取到赛事详情")
                except Exception as e:
                    logger.error(f"✗ 赛事详情提取失败: {e}")
            else:
                logger.info("⚠️  无详情页HTML，跳过详情提取")

            # ========================================
            # ✨✨✨ 新增：合并crawler和AI的组别数据 ✨✨✨
            # ========================================
            logger.info(f"\n【数据合并】合并crawler和AI提取的组别数据")
            logger.info("-" * 80)

            # 获取两边的数据
            crawler_categories = event_detail.get('categories', [])
            ai_categories = structured_data.get('race_categories', [])

            merged_categories = []

            if crawler_categories:
                logger.info(f"✓ crawler提取了 {len(crawler_categories)} 个组别（包含报名状态）")

                # 遍历crawler的每个组别
                for crawler_cat in crawler_categories:
                    cat_name = crawler_cat.get('name', '')

                    # 在AI数据中查找对应的组别
                    ai_cat = None
                    for ac in ai_categories:
                        if ac.get('name') == cat_name:
                            ai_cat = ac
                            break

                    if ai_cat is None:
                        ai_cat = {}

                    # 合并数据：AI的详细信息 + crawler的报名状态
                    merged_cat = {
                        'name': crawler_cat.get('name'),
                        'distance': ai_cat.get('distance') or crawler_cat.get('distance'),
                        'fee': ai_cat.get('fee') or crawler_cat.get('fee'),
                        'zaoniao_fee': ai_cat.get('zaoniao_fee'),
                        'total_quota': ai_cat.get('total_quota'),
                        'registered_count': ai_cat.get('registered_count'),
                        'start_time': ai_cat.get('start_time'),
                        'cutoff_time': ai_cat.get('cutoff_time'),

                        # ✨ 关键：保留crawler提取的报名状态字段
                        'registration_status': crawler_cat.get('registration_status'),
                        'registration_url': crawler_cat.get('registration_url')
                    }

                    merged_categories.append(merged_cat)

                    # 输出日志
                    status_str = merged_cat['registration_status'] or 'NULL'
                    url_preview = merged_cat['registration_url'][:35] + '...' if merged_cat[
                        'registration_url'] else 'NULL'
                    logger.info(f"  · {cat_name}: 状态={status_str}, 链接={url_preview}")

                logger.info(f"✓ 合并完成，保留了 {len(merged_categories)} 个组别的报名状态和链接")

            else:
                # 如果crawler没提取到，用AI的
                logger.warning(f"⚠️  crawler未提取到组别，使用AI数据（可能缺少报名状态）")
                merged_categories = ai_categories

            # 优先使用crawler的赛事整体状态
            event_status = event_detail.get('status') or structured_data.get('status', 'active')
            status_source = 'crawler(HTML)' if event_detail.get('status') else 'AI'
            logger.info(f"✓ 赛事整体状态: {event_status} (来源: {status_source})")
            logger.info("-" * 80)
            # ========================================
            # ✨✨✨ 合并结束 ✨✨✨
            # ========================================

            final_data = {
                'event_id': event['event_id'],
                'event_url': event['event_url'],
                'name': structured_data.get('name') or event.get('name'),
                'event_date': structured_data.get('event_date'),
                'event_level': structured_data.get('event_level'),
                'location': structured_data.get('location'),
                'detailed_address': structured_data.get('detailed_address'),
                'race_categories': merged_categories ,
                'total_scale': structured_data.get('total_scale'),
                'registration_fee': structured_data.get('registration_fee'),
                'organizer': structured_data.get('organizer'),
                'host_units': structured_data.get('host_units'),
                'co_organizers': structured_data.get('co_organizers'),
                'supporters': structured_data.get('supporters'),
                'contact_phone': structured_data.get('contact_phone'),
                'contact_email': structured_data.get('contact_email'),
                'contact_person': structured_data.get('contact_person'),
                'registration_deadline': structured_data.get('registration_deadline') or event.get(
                    'registration_deadline'),
                'status': event_status , # 优先使用crawler的status
                'description': event_detail.get('detail_text', ''),
                'news_content': event_detail.get('news_content_raw'),
                'images_analysis': event_detail.get('images_analysis'),
                'news_url': event_detail.get('news_url'),
                'event_detail': event_detail_text,
                'raw_html': event_detail.get('raw_html')
            }

            logger.info(f"✓ 数据合并完成")

            # 步骤6: 保存到数据库
            logger.info(f"\n【步骤6/6】保存到数据库")
            logger.info("-" * 80)
            final_data = EnhancedDataProcessor.process_event_data(final_data)
            # ✨ 使用智能同步，保护人工修改
            try:
                from smart_crawler_sync import SmartCrawlerSync
                from config import DATABASE_CONFIG

                smart_sync = SmartCrawlerSync(DATABASE_CONFIG)
                success = smart_sync.smart_sync_to_mysql(final_data)

                if success:
                    logger.debug(f"  ✓ 智能同步: {final_data}")
            except ImportError as e:
                logger.warning(f"  ⚠️  SmartCrawlerSync不可用: {e}，使用普通保存")
                success = self.db.save_race_event(final_data)
            except Exception as e:
                logger.warning(f"  ⚠️  智能同步失败: {e}，使用普通保存")
                success = self.db.save_race_event(final_data)
            if success:
                logger.info(f"✓✓✓ 成功保存赛事到MySQL数据库")

                # 验证保存
                saved_event = self.db.get_event_by_id(event['event_id'])
                if saved_event:
                    logger.info(f"✓ MySQL验证: 数据已成功存储 (DB ID: {saved_event.id})")
                else:
                    logger.error(f"✗ MySQL验证失败: 无法查询到刚保存的数据")




            else:
                logger.error(f"✗✗✗ 保存失败")

            logger.info("=" * 80 + "\n")

            return success

        except Exception as e:
            logger.error(f"✗✗✗ 处理赛事失败: {event.get('name')}", exc_info=True)
            logger.error(f"错误详情: {e}")
            return False

    def run(self, limit: int = None, max_workers: int = 40):
        """运行爬虫，支持多线程并行处理赛事"""
        logger.info("\n" + "=" * 80)
        logger.info(" " * 30 + "赛事爬虫启动")
        logger.info("=" * 80 + "\n")

        success_count = 0
        fail_count = 0
        start_time = time.time()

        try:
            # 阶段1: 获取赛事列表
            logger.info("【阶段1】获取赛事列表")
            logger.info("-" * 80)

            events = self.crawler.crawl_all_events()
            logger.info(f"✓ 共获取 {len(events)} 个赛事\n")

            if not events:
                logger.error("没有获取到任何赛事，程序退出")
                return

            # 如果设置了limit，只处理前N个
            if limit:
                events = events[:limit]
                logger.info(f"⚠ 测试模式：只处理前 {limit} 个赛事\n")

            # 阶段2: 并行处理赛事详情
            logger.info("【阶段2】并行处理赛事详情")
            logger.info("-" * 80)
            logger.info(f"总计: {len(events)} 个赛事\n")

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_event = {executor.submit(self.process_event, event): event for event in events}

                for idx, future in enumerate(concurrent.futures.as_completed(future_to_event), 1):
                    event = future_to_event[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                        else:
                            fail_count += 1
                        logger.info(
                            f"进度: {idx}/{len(events)} | 赛事: {event['name']} | 处理结果: {'成功' if success else '失败'}")
                    except Exception as e:
                        fail_count += 1
                        logger.error(f"进度: {idx}/{len(events)} | 赛事: {event['name']} | 处理异常: {e}")

                    # 每10个赛事显示一次数据库统计
                    if idx % 10 == 0:
                        stats = self.db.get_stats()
                        logger.info(f"数据库统计: 总计 {stats['total']} 条记录")

            elapsed_time = time.time() - start_time
            stats = self.db.get_stats()

            logger.info("\n" + "=" * 80)
            logger.info(" " * 30 + "爬虫运行完成")
            logger.info("=" * 80)
            logger.info(f"处理统计:")
            logger.info(f"  - 处理总数: {len(events)} 个赛事")
            logger.info(f"  - 成功: {success_count} 个")
            logger.info(f"  - 失败: {fail_count} 个")
            logger.info(
                f"  - 成功率: {success_count * 100 // (success_count + fail_count) if (success_count + fail_count) > 0 else 0}%")
            logger.info(f"数据库统计:")
            logger.info(f"  - 数据库记录总数: {stats['total']}")
            logger.info(f"  - 有资讯内容: {stats['with_news']}")
            logger.info(f"  - 有联系方式: {stats['with_contact']}")
            logger.info(f"耗时: {elapsed_time:.1f} 秒 ({elapsed_time / 60:.1f} 分钟)")
            logger.info("=" * 80 + "\n")

        except KeyboardInterrupt:
            logger.info("\n\n" + "=" * 80)
            logger.info(" " * 30 + "用户中断爬虫")
            logger.info("=" * 80)
            logger.info(f"已处理: ✓ 成功 {success_count} | ✗ 失败 {fail_count}")
            logger.info("=" * 80 + "\n")
        except Exception as e:
            logger.error(f"爬虫运行出错: {e}", exc_info=True)

    def process_events_list(self, events):
        """处理给定的赛事列表（用于日期过滤后的处理）"""
        success_count = 0
        fail_count = 0
        start_time = time.time()

        try:
            logger.info("\n" + "=" * 80)
            logger.info("【阶段2】并行处理赛事详情")
            logger.info("=" * 80)
            logger.info(f"总计: {len(events)} 个赛事\n")

            with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
                future_to_event = {executor.submit(self.process_event, event): event for event in events}

                for idx, future in enumerate(concurrent.futures.as_completed(future_to_event), 1):
                    event = future_to_event[future]
                    try:
                        success = future.result()
                        if success:
                            success_count += 1
                        else:
                            fail_count += 1
                        logger.info(
                            f"进度: {idx}/{len(events)} | 赛事: {event['name']} | 处理结果: {'成功' if success else '失败'}")
                    except Exception as e:
                        fail_count += 1
                        logger.error(f"进度: {idx}/{len(events)} | 赛事: {event['name']} | 处理异常: {e}")

                    # 每10个赛事显示一次数据库统计
                    if idx % 10 == 0:
                        stats = self.db.get_stats()
                        logger.info(f"数据库统计: 总计 {stats['total']} 条记录")

            elapsed_time = time.time() - start_time
            stats = self.db.get_stats()

            logger.info("\n" + "=" * 80)
            logger.info(" " * 30 + "爬虫运行完成")
            logger.info("=" * 80)
            logger.info(f"处理统计:")
            logger.info(f"  - 处理总数: {len(events)} 个赛事")
            logger.info(f"  - 成功: {success_count} 个")
            logger.info(f"  - 失败: {fail_count} 个")
            logger.info(
                f"  - 成功率: {success_count * 100 // (success_count + fail_count) if (success_count + fail_count) > 0 else 0}%")
            logger.info(f"数据库统计:")
            logger.info(f"  - 数据库记录总数: {stats['total']}")
            logger.info(f"  - 有资讯内容: {stats['with_news']}")
            logger.info(f"  - 有联系方式: {stats['with_contact']}")
            logger.info(f"耗时: {elapsed_time:.1f} 秒 ({elapsed_time / 60:.1f} 分钟)")
            logger.info("=" * 80 + "\n")

        except KeyboardInterrupt:
            logger.info("\n\n" + "=" * 80)
            logger.info(" " * 30 + "用户中断爬虫")
            logger.info("=" * 80)
            logger.info(f"已处理: ✓ 成功 {success_count} | ✗ 失败 {fail_count}")
            logger.info("=" * 80 + "\n")
        except Exception as e:
            logger.error(f"爬虫运行出错: {e}", exc_info=True)


def filter_events_by_date(events, start_date=None):
    """
    按日期过滤赛事

    Args:
        events: 赛事列表
        start_date: 开始日期 "YYYY-MM-DD"

    Returns:
        过滤后的赛事列表
    """
    if not start_date:
        return events

    from datetime import datetime
    import re

    try:
        filter_dt = datetime.strptime(start_date, '%Y-%m-%d')
    except Exception as e:
        logger.warning(f"日期格式错误: {start_date}, {e}")
        return events

    filtered = []
    logger.info(f"\n📅 时间过滤: >= {start_date}")
    logger.info("-" * 80)

    for event in events:
        basic_info = event.get('basic_info', '')

        # 尝试多种日期格式
        date_patterns = [
            r'(\d{4})\.(\d{1,2})\.(\d{1,2})',  # 2026.12.20
            r'(\d{4})-(\d{1,2})-(\d{1,2})',  # 2026-12-20
            r'(\d{4})/(\d{1,2})/(\d{1,2})',  # 2026/12/20
            r'(\d{4})年(\d{1,2})月(\d{1,2})日'  # 2026年12月20日
        ]

        event_dt = None
        for pattern in date_patterns:
            match = re.search(pattern, basic_info)
            if match:
                try:
                    year, month, day = match.groups()
                    event_dt = datetime(int(year), int(month), int(day))
                    break
                except:
                    continue

        if event_dt and event_dt >= filter_dt:
            filtered.append(event)

    logger.info(f"过滤结果: {len(filtered)}/{len(events)} 个赛事")
    logger.info(f"{'=' * 80}\n")

    return filtered


def main():
    """主函数（支持导入/导出/爬虫/日期过滤）"""
    import argparse

    # 解析命令行参数
    parser = argparse.ArgumentParser(description='赛事爬虫')

    # 可选参数
    parser.add_argument('--limit', type=int, default=None,
                        help='处理赛事数量限制（测试用）')
    parser.add_argument('--date', type=str, dest='start_date',
                        help='过滤日期，格式: YYYY-MM-DD (例如: 2026-12-01)')
    parser.add_argument('--import', dest='import_file', type=str,
                        help='导入Excel文件路径')
    parser.add_argument('--export', dest='export_file', type=str,
                        help='导出Excel文件路径')

    args = parser.parse_args()

    # 记录包含特殊字符的日志信息
    detail = {'news_url': 'http://example.com/news'}
    logger.info(f"构造资讯链接（方法2）: {detail['news_url']}")

    app = RaceCrawlerApp()
    app.init()

    # ========== 模式1：导入 ==========
    if args.import_file:
        logger.info(f"\n{'=' * 80}")
        logger.info(f"【导入模式】{args.import_file}")
        logger.info(f"{'=' * 80}\n")

        try:
            from race_events_manager import RaceEventsManager
            manager = RaceEventsManager()
            manager.import_manual_edits(args.import_file)
            logger.info(f"✓ 导入完成\n")
        except ImportError:
            logger.error("❌ 未找到 race_events_manager.py")
            logger.info("提示: 请确保文件在同一目录下")
        except Exception as e:
            logger.error(f"❌ 导入失败: {e}")

        # 如果只有导入，没有其他操作，直接返回
        if not args.export_file and not args.start_date and not args.limit:
            return

    # ========== 模式2：爬虫 ==========
    if args.start_date or args.limit or (not args.import_file and not args.export_file):
        # 显示运行参数
        if args.limit:
            logger.info(f"⚙️  设置处理限制: {args.limit} 个赛事")

        if args.start_date:
            logger.info(f"⚙️  日期过滤: >= {args.start_date}\n")

        # 获取赛事列表
        logger.info("\n" + "=" * 80)
        logger.info("【阶段1】获取赛事列表")
        logger.info("=" * 80)

        events = app.crawler.crawl_all_events()
        logger.info(f"✓ 共获取 {len(events)} 个赛事\n")

        if not events:
            logger.error("没有获取到任何赛事，程序退出")
            return

        # 日期过滤
        if args.start_date:
            events = filter_events_by_date(events, args.start_date)

            if not events:
                logger.error(f"❌ 没有符合条件的赛事 (>= {args.start_date})")
                logger.info("提示: 请检查日期格式或调整过滤条件")
                return

        # limit过滤
        if args.limit:
            events = events[:args.limit]
            logger.info(f"⚠️  测试模式：只处理前 {args.limit} 个赛事\n")

        # 处理赛事
        app.process_events_list(events)

    # ========== 模式3：导出 ==========
    if args.export_file:
        logger.info(f"\n{'=' * 80}")
        logger.info(f"【导出模式】{args.export_file}")
        logger.info(f"{'=' * 80}\n")

        try:
            from race_events_manager import RaceEventsManager
            manager = RaceEventsManager()
            manager.export_to_excel(args.export_file)
            logger.info(f"✓ 导出完成\n")
        except ImportError:
            logger.error("❌ 未找到 race_events_manager.py")
            logger.info("提示: 请确保文件在同一目录下")
        except Exception as e:
            logger.error(f"❌ 导出失败: {e}")


if __name__ == '__main__':
    main()