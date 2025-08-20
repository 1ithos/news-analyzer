import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from dateutil.parser import parse as dateutil_parse

# 导入所有可能的解析器类
from parsers import StandardRSSParser, GeekParkRSSParser, BaseParser, TitlesParser
from ai_processing import ai_rank_and_classify_articles, ai_summarize_content

#网站配置
SITES_CONFIG = [
    {
        'name': '36氪',
        'url': 'https://36kr.com/feed',
        'parser_class': StandardRSSParser
    },
    {
        'name': '极客公园',
        'url': 'http://www.geekpark.net/rss',
        'parser_class': GeekParkRSSParser
    },
    {
        'name': '国务院政策文件库',
        'url': 'https://rsshub.app/gov/zhengce',
        'parser_class': StandardRSSParser
    },
    {
        'name': '新华社',
        'url': 'https://plink.anyfeeder.com/newscn/whxw',
        'parser_class': StandardRSSParser
    },
    {
        'name': '人民网',
        'url': 'http://www.people.com.cn/rss/society.xml',
        'parser_class': StandardRSSParser
    },
    {
        'name': '路透社',
        'url': 'https://reutersnew.buzzing.cc/feed.xml',
        'parser_class': TitlesParser
    },
    {
        'name': '法国国际广播电台',
        'url': 'https://plink.anyfeeder.com/rfi/cn',
        'parser_class': StandardRSSParser
    },
    {
        'name': '纽约时报',
        'url': 'https://plink.anyfeeder.com/nytimes/cn',
        'parser_class': StandardRSSParser
    },
    #内容收费
    {
        'name': '经济学人',
        'url': 'https://economistnew.buzzing.cc/feed.xml',
        'parser_class': TitlesParser
    },
]

#强制保留规则
FORCE_KEEP_RULES = [
    {'type': 'keyword', 'values': ['8点1氪', '氪星晚报']},
]

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
                print(f"警告：请求保存的列'{col}'不存在于DataFrame中，将创建为空列。")
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
        print(f"保存数据'{base_filename}'时出错: {e}")

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
        print(f"保存文件时出错: {e}")


def main():
    load_dotenv()
    today_local_date = datetime.now().date()

    # 1. 数据采集
    print("--- 1. 开始数据采集 ---")
    all_articles_raw = []
    for site_config in SITES_CONFIG:
        parser_class = site_config.get('parser_class')
        if not (parser_class and issubclass(parser_class, BaseParser)): continue
        parser = parser_class(site_config)
        articles_df = parser.parse()
        if not articles_df.empty: all_articles_raw.append(articles_df)

    if not all_articles_raw:
        print("本次任务未爬取到任何数据，程序结束。")
        return

    raw_combined_df = pd.concat(all_articles_raw, ignore_index=True)
    print(f"\n原始数据爬取完成，共 {len(raw_combined_df)} 篇文章。")

    # save_dataframe_to_files(
    #     df=raw_combined_df,
    #     folder_name=f"RawData_{today_local_date}",
    #     base_filename="raw_articles"
    # )

    #2. 日期过滤
    print("\n--- 2. 进行日期过滤 ---")

    filtered_articles = []
    for index, row in raw_combined_df.iterrows():
        try:
            if not row['publish_time'] or not isinstance(row['publish_time'], str):
                continue


            pub_datetime_aware = dateutil_parse(row['publish_time'])
            pub_datetime_local = pub_datetime_aware.astimezone()

            if pub_datetime_local.date() == today_local_date:
                filtered_articles.append(row)

        except (TypeError, ValueError, AttributeError) as e:
            print(f"警告：无法解析日期或日期格式无效 '{row['publish_time']}'，跳过该条目。错误: {e}")
            continue

    if not filtered_articles:
        print("没有找到今天发布的新闻，程序结束。")
        return

    today_combined_df = pd.DataFrame(filtered_articles).reset_index(drop=True)
    print(f"日期过滤完成，共 {len(today_combined_df)} 篇今天发布的新闻。")

    # save_dataframe_to_files(
    #     df=today_combined_df,
    #     folder_name=f"RawData_{today_local_date}",
    #     base_filename="raw_articles"
    # )

    # 2.5 文章去重
    initial_count = len(today_combined_df)
    today_combined_df.drop_duplicates(subset=['title'], keep='first', inplace=True)
    today_combined_df.reset_index(drop=True, inplace=True)
    final_count = len(today_combined_df)
    print(f"去重完成，移除了 {initial_count - final_count} 篇。剩余 {final_count} 篇。")

    # 3. AI分析与分类
    print(f"\n--- 3. AI重要性评估与分类 ---")
    classified_df = ai_rank_and_classify_articles(today_combined_df)
    if 'category' not in classified_df.columns or classified_df['category'].eq('未知').all():
        print("AI评估分类失败，程序退出。")
        return

    print("\n--- 3.5 正在根据标题出现频率进行加分 ---")
    title_counts = classified_df.groupby('title')['title'].transform('count')
    frequency_bonus = (title_counts - 1).clip(upper=2)
    classified_df['importance_score'] += frequency_bonus
    if frequency_bonus.sum() > 0: print("已为高频标题增加重要性分数。")

    # 4. 应用独立的强制保留规则
    print("\n--- 4. 正在应用自定义强制保留规则 ---")
    forced_df, candidate_df = apply_force_keep_rules(classified_df, FORCE_KEEP_RULES)
    print(f"筛选出 {len(forced_df)} 篇强制保留文章。")

    # 5. 对候选文章进行排序和配额筛选
    print("\n--- 5. 正在对候选文章进行排序和配额筛选 ---")
    ranked_candidates_df = candidate_df.sort_values(by='importance_score', ascending=False)

    CATEGORY_QUOTAS = {'科技与商业': 8, '国际动态': 8, '社会与文化': 6,}
    quota_selection = []
    category_counts = {cat: 0 for cat in CATEGORY_QUOTAS.keys()}
    total_limit = 20

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

    # 6. 合并最终列表
    final_list_df = pd.concat([forced_df, quota_selection_df], ignore_index=True)
    final_list_df.drop_duplicates(subset=['url', 'title'], inplace=True)
    final_list_df.reset_index(drop=True, inplace=True)

    print(f"\n--- 最终新闻列表已生成 (共 {len(final_list_df)} 篇) ---")
    print("最终列表类别分布：\n", final_list_df['category'].value_counts())

    # 7. 抓取正文
    print("\n--- 7. 为最终列表文章抓取原文/摘要 ---")
    if 'description' in final_list_df.columns:
        final_list_df['full_content'] = final_list_df['description']
    else:
        print("警告：'description' 列在AI处理后丢失，将尝试重新抓取原文作为后备方案。")
        final_list_df['full_content'] = final_list_df.apply(
            lambda row: BaseParser.fetch_full_content_from_url(row['url'], '正文内容丢失'),
            axis=1
        )

    # 8. 保存包含正文的列表
    save_dataframe_to_files(
        df=final_list_df,
        folder_name=f"FinalList_{today_local_date}",
        base_filename="final_articles_with_content",
        columns_to_save=['title', 'url', 'source', 'importance_score', 'category', 'full_content']
    )

    # 9. AI精简正文
    print("\n--- 9. 为最终列表文章进行AI内容精简 ---")
    final_df_summarized = ai_summarize_content(final_list_df)

    # 10. 输出最终结果到TXT
    export_to_txt(final_df_summarized)

    print("\n--- 任务执行完毕 ---")

if __name__ == "__main__":
    main()