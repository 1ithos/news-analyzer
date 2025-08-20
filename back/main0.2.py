import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from dateutil.parser import parse as dateutil_parse

# 导入所有可能的解析器类
from parsers import StandardRSSParser, GeekParkRSSParser, BaseParser, TitlesParser
from ai_processing import ai_rank_articles, ai_summarize_content

#配置区
SITES_CONFIG = [
    # {
    #     'name': '36氪',
    #     'url': 'https://36kr.com/feed',
    #     'type': '科技',
    #     'parser_class': StandardRSSParser
    # },
    # {
    #     'name': '极客公园',
    #     'url': 'http://www.geekpark.net/rss',
    #     'type': '科技',
    #     'parser_class': GeekParkRSSParser
    # },
    {
        'name': '国务院政策文件库',
        'url': 'https://rsshub.app/gov/zhengce',
        'type': '国内政策',
        'parser_class': StandardRSSParser
    },
    # {
    #     'name': '新华网',
    #     'url': 'https://www.stats.gov.cn/sj/zxfb/rss.xml',
    #     'type': '国内政策',
    #     'parser_class': StandardRSSParser
    # },
    # {
    #     'name': '路透社',
    #     'url': 'https://reutersnew.buzzing.cc/feed.xml',
    #     'type': '国际要闻',
    #     'parser_class': TitlesParser
    # },
    # {
    #     'name': '法广',
    #     'url': 'https://plink.anyfeeder.com/rfi/cn',
    #     'type': '国际要闻',
    #     'parser_class': StandardRSSParser
    # },
    # {
    #     'name': '纽约时报',
    #     'url': 'https://plink.anyfeeder.com/nytimes/cn',
    #     'type': '国际要闻',
    #     'parser_class': StandardRSSParser
    # },
    #内容收费
    # {
    #     'name': '经济学人',
    #     'url': 'https://economistnew.buzzing.cc/feed.xml',
    #     'type': '国际要闻',
    #     'parser_class': TitlesParser
    # },
]


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
    all_articles_raw = []

    today_local = datetime.now()
    now = today_local.strftime("%Y%m%d")

    # 1. 数据采集
    print("--- 1. 开始快速数据采集 ---")
    for site_config in SITES_CONFIG:
        parser_class = site_config.get('parser_class')
        if not issubclass(parser_class, BaseParser):
            print(f"错误：配置的解析器 {parser_class.__name__} 未继承BaseParser。")
            continue

        parser = parser_class(site_config)  # 实例化指定的解析器
        articles_df = parser.parse()

        if not articles_df.empty:
            all_articles_raw.append(articles_df)

    if not all_articles_raw:
        print("本次任务未爬取到任何数据，程序结束。")
        return

    raw_combined_df = pd.concat(all_articles_raw, ignore_index=True)
    print(f"\n原始数据爬取完成，共 {len(raw_combined_df)} 篇文章。")

    # save_dataframe_to_files(
    #     df=raw_combined_df,
    #     folder_name=f"RawData_{now}",
    #     base_filename="raw_articles"
    # )

    # 2. 日期过滤
    print("\n--- 2. 进行日期过滤 ---")

    filtered_articles = []
    for index, row in raw_combined_df.iterrows():
        try:
            if not row['publish_time'] or not isinstance(row['publish_time'], str):
                continue

            pub_datetime = dateutil_parse(row['publish_time'])

            if pub_datetime.month == today_local.month and pub_datetime.day == today_local.day:
                filtered_articles.append(row)

        except (TypeError, ValueError, AttributeError) as e:
            # 增加 AttributeError 以捕获更多潜在的解析问题
            print(f"警告：无法解析日期或日期格式无效 '{row['publish_time']}'，跳过该条目。错误: {e}")
            continue

    if not filtered_articles:
        print("没有找到今天发布的新闻，程序结束。")
        return

    today_combined_df = pd.DataFrame(filtered_articles).reset_index(drop=True)
    print(f"日期过滤完成，共 {len(today_combined_df)} 篇今天发布的新闻。")

    # 保存原始数据
    # save_dataframe_to_files(
    #     df=today_combined_df,
    #     folder_name=f"RawData_{now}",
    #     base_filename="raw_articles"
    # )

    # 3. AI分析话题度和重要性
    print(f"\n--- 3. AI重要性评估（共 {len(today_combined_df)} 篇文章） ---")
    ranked_df = ai_rank_articles(today_combined_df)

    if 'importance_score' not in ranked_df.columns or ranked_df['importance_score'].sum() == 0:
        print("AI评估失败，无法继续执行，程序退出。")
        return
    #
    # 4. 筛选出Top 20
    final_list_df = ranked_df.copy()
    print(f"\n--- 4. 已筛选出最终新闻列表 (共 {len(final_list_df)} 篇) ---")
    print(final_list_df[['title', 'importance_score', 'source']])

    # 5. 为Top 20新闻精准抓取原文
    if 'description' in final_list_df.columns:
        final_list_df['full_content'] = final_list_df['description']
    else:
        print("警告：'description' 列在AI处理后丢失，将尝试重新抓取原文作为后备方案。")
        final_list_df['full_content'] = final_list_df.apply(
            lambda row: BaseParser.fetch_full_content_from_url(row['url'], '正文内容丢失'),
            axis=1
        )

    print("\n--- 6. 保存包含正文的最终列表到文件 ---")
    save_dataframe_to_files(
        df=final_list_df,
        folder_name=f"FinalList_{now}",
        base_filename="final_articles_with_content",
        # 指定要保存的列，包括了full_content
        columns_to_save=['title', 'url', 'source', 'importance_score', 'full_content']
    )

    # 6. AI精简正文
    print("\n--- 7.对文章进行AI内容精简 ---")
    final_df = ai_summarize_content(final_list_df)

    # 7. 输出最终结果
    export_to_txt(final_df)
    print("\n--- 任务执行完毕 ---")

if __name__ == "__main__":
    main()