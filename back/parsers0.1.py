import feedparser
from bs4 import BeautifulSoup
import pandas as pd
from newspaper import Article


class RSSParser:
    """
    一个通用的RSS解析器，并能尝试抓取文章全文。
    """

    def __init__(self, site_config):
        self.config = site_config
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def _fetch_full_content(self, url, fallback_text):
        """
        尝试从URL抓取全文。如果失败，返回后备文本（通常是RSS的description）。
        """
        try:
            # newspaper3k需要设置语言为'zh'来更好地处理中文
            article = Article(url, language='zh')
            article.download()
            article.parse()
            # 如果解析出的文本太短，可能意味着抓取失败，不如使用description
            if len(article.text) > 100:
                return article.text
            else:
                print(f"  > [提示] 全文抓取结果过短，使用RSS描述作为后备。 URL: {url}")
                return fallback_text
        except Exception as e:
            print(f"  > [警告] 抓取全文失败: {e}。 URL: {url}")
            return fallback_text  # 抓取失败，返回原始的描述

    @staticmethod
    def _clean_description(desc):
        if not desc:
            return ""
        try:
            soup = BeautifulSoup(desc, 'html.parser')
            for element in soup(['script', 'style', 'iframe', 'noscript']):
                element.decompose()
            text = soup.get_text(separator=' ', strip=True)
            return ' '.join(text.split())
        except Exception as e:
            print(f"清理描述时出错: {e}")
            return desc[:200] + "..." if len(desc) > 200 else desc

    def parse(self):
        """
        执行解析，获取RSS条目并抓取全文，返回DataFrame。
        """
        print(f"正在抓取【{self.config['name']}】的内容，URL: {self.config['url']}")
        try:
            feed = feedparser.parse(self.config['url'], request_headers=self.headers)
            articles = []

            if feed.bozo:
                print(f"警告：【{self.config['name']}】的RSS源可能格式不正确。")

            for entry in feed.entries:
                try:
                    title = entry.get('title', '无标题').strip()
                    link = entry.get('link', '').strip()

                    # 先获取并清理RSS中的描述，作为后备内容
                    rss_description = self._clean_description(entry.get('description', ''))

                    # --- 核心改动：调用全文抓取方法 ---
                    print(f"  - 正在处理文章: {title}")
                    full_content = self._fetch_full_content(link, rss_description)

                    articles.append({
                        "title": title,
                        "url": link,
                        "publish_time": entry.get('published', ''),
                        "content": full_content,  # <--- 字段名改为'content'，存储全文或摘要
                        "category": self.config['type']
                    })
                except Exception as e:
                    print(f"处理条目时出错: {e}")
                    continue

            return pd.DataFrame(articles)

        except Exception as e:
            print(f"解析【{self.config['name']}】失败: {str(e)}")
            return pd.DataFrame()