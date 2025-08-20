import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from dateutil.parser import parse as dateutil_parse
import yaml
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入所有可能的解析器类
import parsers
from parsers import BaseParser
from logger_config import setup_logging
from ai_processing import ai_rank_and_classify_articles, ai_summarize_content
from database import DatabaseManager

def load_config(config_path='config.yaml'):
    print(f"--- 正在从 {config_path} 加载配置 ---")
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        logging.error(f"错误：配置文件 {config_path} 未找到！")
        exit() # 如果没有配置文件，程序无法运行
    except Exception as e:
        logging.error(f"错误：解析配置文件时出错: {e}")
        exit()

def apply_force_keep_rules(df: pd.DataFrame, rules: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    根据规则列表，将DataFrame分割为“强制保留”和“候选”两部分。
    """
    if df.empty or not rules:
        return pd.DataFrame(), df

    force_keep_mask = pd.Series([False] * len(df), index=df.index)
    for rule in rules:
        rule_type = rule.get('type')
        if rule_type == 'keyword' and 'values' in rule:
            keywords = rule['values']
            force_keep_mask |= df['title'].str.contains('|'.join(keywords), case=False, na=False)
        elif rule_type == 'source' and 'values' in rule:
            sources = rule['values']
            force_keep_mask |= df['source'].isin(sources)
        elif rule_type == 'category' and 'values' in rule:
            categories = rule['values']
            force_keep_mask |= df['category'].isin(categories)
        elif rule_type == 'composite' and 'conditions' in rule:
            composite_mask = pd.Series([True] * len(df), index=df.index)
            for key, value in rule['conditions'].items():
                if key == 'source':
                    composite_mask &= (df['source'] == value)
                elif key == 'category':
                    composite_mask &= (df['category'] == value)
                elif key == 'keyword':
                    composite_mask &= df['title'].str.contains(value, case=False, na=False)
            force_keep_mask |= composite_mask

    forced_df = df[force_keep_mask].copy()
    candidate_df = df[~force_keep_mask].copy()

    return forced_df, candidate_df

def save_dataframe_to_files(df: pd.DataFrame, folder_name: str, base_filename: str, columns_to_save: list = None):
    """
    一个通用的函数，将任意DataFrame保存为CSV和Excel文件。
    """
    if df.empty:
        print(f"DataFrame为空，跳过保存'{base_filename}'。")
        return
    if columns_to_save is None:
        columns_to_save = df.columns.tolist()
    else:
        for col in columns_to_save:
            if col not in df.columns:
                logging.error(f"警告：请求保存的列'{col}'不存在于DataFrame中，将创建为空列。")
                df[col] = None

    os.makedirs(folder_name, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_base_filename = os.path.join(folder_name, f"{base_filename}_{timestamp}")
    try:
        print(f"\n正在保存数据到文件夹: {folder_name}")
        df_to_save = df[columns_to_save]
        csv_path = f"{full_base_filename}.csv"
        df_to_save.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f" - 已保存为CSV: {csv_path}")
        excel_path = f"{full_base_filename}.xlsx"
        df_to_save.to_excel(excel_path, index=False)
        print(f" - 已保存为Excel: {excel_path}")
    except Exception as e:
        logging.error(f"保存数据'{base_filename}'时出错: {e}")

def export_to_txt(df: pd.DataFrame, filename_suffix=""):
    """
    将最终处理好的DataFrame输出为TXT文件。
    """
    if df.empty:
        print("没有可供输出的内容。")
        return

    filename = f"新闻精简_{datetime.now().strftime('%Y%m%d')}{filename_suffix}.txt"
    print(f"\n--- 正在将最终结果输出到 {filename} ---")

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for index, row in df.iterrows():
                f.write("=" * 80 + "\n")
                f.write(f"标题: {row['title']}\n\n")
                f.write(f"链接: {row['url']}\n\n")
                f.write("--- AI精简后内容 ---\n")
                summary = row.get('summarized_content', '精简失败')
                f.write(summary.strip() + "\n\n")

        print(f"文件已成功保存！")
    except Exception as e:
        logging.error(f"保存文件时出错: {e}")

def parse_single_source(site_config: dict, global_config: dict) -> pd.DataFrame:
    """
    工作函数：处理单个新闻源的解析任务。
    这个函数将在独立的线程中执行。
    """
    logger = logging.getLogger(__name__)
    site_name = site_config.get('name', '未知源')
    print(f"线程池开始处理: {site_name}")
    try:
        parser_class_name = site_config.get('parser_class')
        if not parser_class_name:
            logger.warning(f"源 '{site_name}' 未配置parser_class，跳过。")
            return pd.DataFrame()

        parser_class = getattr(parsers, parser_class_name, None)
        if not (parser_class and issubclass(parser_class, BaseParser)):
            logger.warning(f"在parsers.py中未找到名为 '{parser_class_name}' 的有效解析器，跳过源 '{site_name}'。")
            return pd.DataFrame()

        parser = parser_class(site_config, global_config)
        articles_df = parser.parse()
        return articles_df
    except Exception as e:
        logger.error(f"处理 '{site_name}' 时线程内部发生错误: {e}", exc_info=True)
        return pd.DataFrame()

def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    today_local_date = datetime.now().date()

    logger.info(f"=== {today_local_date}日志记录 ===")
    load_dotenv()

    config = load_config()
    MAX_WORKERS = config.get('max_workers', 5)
    SITES_CONFIG = config.get('sites', [])
    FORCE_KEEP_RULES = config.get('force_keep_rules', [])
    CATEGORY_QUOTAS = config.get('category_quotas', {})
    TOTAL_LIMIT = config.get('selection_total_limit', 20)
    db_manager = DatabaseManager(config)

    #增量检查
    print("开始增量检查")
    existing_urls = db_manager.get_existing_urls()
    logger.info(f"数据库中已存在 {len(existing_urls)} 个URL。")


    #数据采集
    print("开始数据采集")
    all_articles_raw = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_site = {
            executor.submit(parse_single_source, site, config): site['name']
            for site in SITES_CONFIG
        }
        for future in as_completed(future_to_site):
            site_name = future_to_site[future]
            try:
                result_df = future.result()
                if not result_df.empty:
                    all_articles_raw.append(result_df)
                    logger.info(f"线程池成功完成: {site_name}，获取到 {len(result_df)} 篇文章。")
                else:
                    logger.info(f"线程池完成: {site_name}，未获取到文章。")
            except Exception as e:
                logger.error(f"从线程池收集 '{site_name}' 的结果时发生严重错误: {e}", exc_info=True)

    if not all_articles_raw:
        logger.warning("本次任务未爬取到任何数据，程序结束。")
        return

    raw_combined_df = pd.concat(all_articles_raw, ignore_index=True)
    logger.info(f"所有并发任务完成，原始数据爬取完成，共 {len(raw_combined_df)} 篇文章。")

    save_dataframe_to_files(
        df=raw_combined_df,
        folder_name=f"RawData_{today_local_date}",
        base_filename="raw_articles"
    )

    initial_count = len(raw_combined_df)
    new_articles_df = raw_combined_df[~raw_combined_df['url'].isin(existing_urls)].copy()
    new_articles_df.reset_index(drop=True, inplace=True)
    logger.info(f"筛选出 {len(new_articles_df)} 篇新文章（共爬取 {initial_count} 篇）")

    if new_articles_df.empty:
        logger.info("没有发现新文章，任务提前结束。")
        # 在结束前，执行数据清理
        if config.get('data_retention', {}).get('enabled', False):
            days = config.get('data_retention', {}).get('days_to_keep', 7)
            db_manager.clean_old_articles(days)
        db_manager.close()
        return

    #2. 日期过滤
    filtered_articles = []
    for index, row in new_articles_df.iterrows():
        try:
            if not row['publish_time'] or not isinstance(row['publish_time'], str):
                continue

            pub_datetime_aware = dateutil_parse(row['publish_time'])
            pub_datetime_local = pub_datetime_aware.astimezone()

            if pub_datetime_local.date() == today_local_date:
                filtered_articles.append(row)

        except (TypeError, ValueError, AttributeError) as e:
            logging.error(f"警告：无法解析日期或日期格式无效 '{row['publish_time']}'，跳过该条目。错误: {e}")
            continue

    if not filtered_articles:
        logging.warning("没有找到今天发布的新闻，程序结束。")
        return

    today_new_articles_df = pd.DataFrame(filtered_articles).reset_index(drop=True)
    print(f"日期过滤完成，共 {len(today_new_articles_df)} 篇今天发布的新闻。")

    save_dataframe_to_files(
        df= today_new_articles_df,
        folder_name=f"RawData_{today_local_date}",
        base_filename="raw_articles"
    )

    #文章去重
    initial_count = len(today_new_articles_df)
    today_new_articles_df.drop_duplicates(subset=['title'], keep='first', inplace=True)
    today_new_articles_df.reset_index(drop=True, inplace=True)
    final_count = len(today_new_articles_df)
    print(f"去重完成，移除了 {initial_count - final_count} 篇。剩余 {final_count} 篇。")

    #AI分析与分类
    classified_df = ai_rank_and_classify_articles(today_new_articles_df)
    if 'category' not in classified_df.columns or classified_df['category'].eq('未知').all():
        logging.warning("AI评估分类失败，程序退出。")
        return

    title_counts = classified_df.groupby('title')['title'].transform('count')
    frequency_bonus = (title_counts - 1).clip(upper=2)
    classified_df['importance_score'] += frequency_bonus
    if frequency_bonus.sum() > 0: print("已为高频标题增加重要性分数。")

    #强制保留规则
    forced_df, candidate_df = apply_force_keep_rules(classified_df, FORCE_KEEP_RULES)
    print(f"筛选出 {len(forced_df)} 篇强制保留文章。")

    #对候选文章进行排序和配额筛选
    ranked_candidates_df = candidate_df.sort_values(by='importance_score', ascending=False)

    quota_selection = []
    category_counts = {cat: 0 for cat in CATEGORY_QUOTAS.keys()}
    total_limit = TOTAL_LIMIT

    for index, row in ranked_candidates_df.iterrows():
        if len(quota_selection) >= total_limit: break
        cat = row['category']
        if cat in category_counts:
            if category_counts[cat] < CATEGORY_QUOTAS[cat]:
                quota_selection.append(row)
                category_counts[cat] += 1
        else:
            quota_selection.append(row)

    quota_selection_df = pd.DataFrame(quota_selection).reset_index(drop=True)
    print(f"根据配额从候选文章中选出 {len(quota_selection_df)} 篇。")

    #合并最终列表
    final_list_df = pd.concat([forced_df, quota_selection_df], ignore_index=True)
    final_list_df.drop_duplicates(subset=['url', 'title'], inplace=True)
    final_list_df.reset_index(drop=True, inplace=True)

    print(f"\n--- 最终新闻列表已生成 (共 {len(final_list_df)} 篇) ---")
    print("最终列表类别分布：\n", final_list_df['category'].value_counts())

    #抓取正文
    if 'description' in final_list_df.columns:
        final_list_df['full_content'] = final_list_df['description']
    else:
        logging.warning("警告：'description' 列在AI处理后丢失，将尝试重新抓取原文作为后备方案。")
        final_list_df['full_content'] = final_list_df.apply(
            lambda row: BaseParser.fetch_full_content_from_url(row['url'], '正文内容丢失'),
            axis=1
        )

    #保存包含正文的列表
    save_dataframe_to_files(
        df=final_list_df,
        folder_name=f"FinalList_{today_local_date}",
        base_filename="final_articles_with_content",
        columns_to_save=['title', 'url', 'source', 'importance_score', 'importance_score', 'category', 'full_content']
    )
    columns_to_write = ['url', 'title', 'source', 'publish_time', 'description']
    articles_to_db = today_new_articles_df[columns_to_write]
    db_manager.write_new_articles(articles_to_db.copy())

    print("开始清理老旧数据")
    if config.get('data_retention', {}).get('enabled', False):
        days = config.get('data_retention', {}).get('days_to_keep', 7)
        db_manager.clean_old_articles(days)
    else:
        print("数据清理功能未启用。")

    #AI精简正文
    final_df_summarized = ai_summarize_content(final_list_df)

    #输出最终结果到TXT
    export_to_txt(final_df_summarized)
    db_manager.close()
    logging.info("\n--- 任务执行完毕 ---")

if __name__ == "__main__":
    main()