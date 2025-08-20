import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from parsers import RSSParser
from ai_processing import ai_rank_articles, ai_summarize_content

SITES_CONFIG = [
    {
        'name': '36氪',
        'url': 'https://36kr.com/feed',
        'type': '科技',
        'parser': 'rss'  # 指定使用哪个解析器
    },
    #需付费
    # {
    #     'name': '纽约客',
    #     'url': 'https://www.newyorker.com/feed/everything',
    #     'type': '杂谈',
    #     'parser': 'rss'
    # },
    # {
    #     'name': '中央人民政府',
    #     'url': 'http://www.gov.cn/rss/yaowen.xml',
    #     'type': '政策',
    #     'parser': 'rss'
    # },
]


def save_data(df, site_name, format='csv'):
    """
    将DataFrame保存到文件中。
    文件名现在由网站名动态生成。
    """
    try:
        # 使用网站名和日期创建文件夹
        date_folder = datetime.now().strftime(f"{site_name}_%Y%m%d")
        os.makedirs(date_folder, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d")
        filename = os.path.join(date_folder, f"{site_name}_{timestamp}")

        if format == 'csv':
            df.to_csv(f"{filename}.csv", index=False, encoding='utf-8-sig')
            print(f"数据已保存为 CSV 文件: {filename}.csv")
        elif format == 'excel':
            df.to_excel(f"{filename}.xlsx", index=False)
            print(f"数据已保存为 Excel 文件: {filename}.xlsx")

    except Exception as e:
        print(f"保存数据时出错: {e}")


def main():
    load_dotenv()
    all_articles = []

    print("--- 开始执行爬取任务 ---")

    # 1. 数据采集
    for site in SITES_CONFIG:
        parser = None
        if site['parser'] == 'rss':
            parser = RSSParser(site)
        # elif site['parser'] == 'web_scraper':
        #     parser = WebScraperParser(site) # 未来可以扩展

        if parser:
            articles_df = parser.parse()
            if not articles_df.empty:
                all_articles.append(articles_df)
                # 为每个网站单独保存一份原始数据
                save_data(articles_df, site['name'], 'csv')
                save_data(articles_df, site['name'], 'excel')
            else:
                print(f"未能从【{site['name']}】获取到任何数据。")
        print("-" * 20)

    if not all_articles:
        print("--- 本次任务未爬取到任何数据，程序结束。 ---")
        return

    # 将所有网站的数据合并到一个DataFrame中
    combined_df = pd.concat(all_articles, ignore_index=True)
    print("\n--- 所有网站数据已合并 ---")
    print(f"总共获取到 {len(combined_df)} 篇文章。")
    print("各类别文章数量分布：")
    print(combined_df['category'].value_counts())

    # 2. AI分析话题度和重要性 (后续步骤)
    # ranked_df = ai_rank_articles(combined_df)
    # if 'importance_score' in ranked_df.columns and ranked_df['importance_score'].sum() > 0:
    #     print("\n--- AI评估后的Top 5重要新闻 ---")
    #     print(ranked_df[['title', 'category', 'importance_score']].head(5))
    # else:
    #     print("\nAI评估未成功执行，后续步骤将使用原始顺序。")

    # 3. 根据规则筛选和精简内容 (后续步骤)
    # final_articles = filter_and_summarize(ranked_df)
    print("[占位符] 下一步：根据规则筛选并精简内容...")

    # 4. 输出TXT文件 (后续步骤)
    # export_to_txt(final_articles)
    print("[占位符] 下一步：输出最终的TXT文件。")

    print("\n--- 任务执行完毕 ---")


if __name__ == "__main__":
    main()