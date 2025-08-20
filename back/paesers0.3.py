import feedparser
from bs4 import BeautifulSoup
import pandas as pd
from newspaper import Article
from abc import ABC, abstractmethod
import urllib.request

class BaseParser(ABC):
    def __init__(self, site_config):
        self.config = site_config
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        self.proxies = {
           'http': 'http://127.0.0.1:7890',
           'https': 'http://127.0.0.1:7890',
        }
        print(f"解析器已为【{self.config['name']}】配置代理: {self.proxies}")

    @abstractmethod
    def parse(self) -> pd.DataFrame:
        """
        抽象方法：所有具体解析器必须实现此方法来解析网站内容。
        """
        pass

    @staticmethod
    def fetch_full_content_from_url(url, fallback_text):
        """
        静态工具方法：从URL抓取全文。可以被所有解析器复用。
        """
        try:
            article = Article(url, language='zh')
            article.download()
            article.parse()
            if article.text and len(article.text) > 100:
                return article.text
            else:
                return fallback_text
        except Exception as e:
            # print(f"  > [警告] 抓取全文失败: {e}。将使用后备文本。URL: {url}")
            return fallback_text

    @staticmethod
    def _clean_html_description(desc):
        """
        静态工具方法：清理HTML描述。可以被所有解析器复用。
        """
        if not desc: return ""
        try:
            soup = BeautifulSoup(desc, 'html.parser')
            for element in soup(['script', 'style', 'iframe', 'noscript']): element.decompose()
            text = soup.get_text(separator=' ', strip=True)
            return ' '.join(text.split())
        except Exception:
            return desc[:200] + "..." if len(desc) > 200 else desc

class StandardRSSParser(BaseParser):
    """
    处理遵循标准RSS格式的网站。
    它只解析RSS条目中的title, link, published, description。
    """
    def parse(self) -> pd.DataFrame:
        print(f"正在解析【{self.config['name']}】...")
        try:
            # 为feedparser设置代理
            proxy_handler = urllib.request.ProxyHandler(self.proxies)
            feed = feedparser.parse(self.config['url'], request_headers=self.headers, handlers=[proxy_handler])

            if not feed.entries:
                print(
                    f"  > 警告：【{self.config['name']}】通过代理请求成功，但未解析出任何新闻条目。请检查RSS源地址是否正确。")
                return pd.DataFrame()

            articles = []
            for entry in feed.entries:
                try:
                    articles.append({
                        "title": entry.get('title', '无标题').strip(),
                        "url": entry.get('link', '').strip(),
                        "publish_time": entry.get('published', ''),
                        "description": self._clean_html_description(entry.get('description', '')),
                        "source": self.config['name']
                    })
                except Exception as e:
                    print(f"StandardRSSParser处理条目时出错: {e}")
                    continue
            return pd.DataFrame(articles)
        except Exception as e:
            print(f"StandardRSSParser在代理模式下解析【{self.config['name']}】失败: {str(e)}")
            return pd.DataFrame()

class GeekParkRSSParser(BaseParser):
    """
    为【极客公园】定制的RSS解析器。
    采用“标记-拆分”策略，能够精准处理将多篇文章聚合在一个<description>内的RSS条目。
    它根据<h2>标签将内容拆分为多篇独立文章，确保标题和正文正确对应。
    """
    def parse(self) -> pd.DataFrame:
        print(f"正在器解析【{self.config['name']}】...")
        try:
            feed = feedparser.parse(self.config['url'], request_headers=self.headers)
            processed_articles = []

            for entry in feed.entries:
                try:
                    raw_description_html = entry.get('description', '')
                    if not raw_description_html:
                        continue

                    soup = BeautifulSoup(raw_description_html, 'html.parser')
                    h2_tags = soup.find_all('h2')

                    # --- 回退机制：处理非聚合的普通文章 ---
                    if not h2_tags:
                        processed_articles.append({
                            "title": entry.get('title', '无标题').strip(),
                            "url": entry.get('link', '').strip(),
                            "publish_time": entry.get('published', ''),
                            "description": self._clean_html_description(raw_description_html),
                            "source": self.config['name']
                        })
                        continue

                    # --- 核心拆分逻辑：处理聚合文章 ---
                    SPLIT_MARKER = "<!-- GEEKPARK-SPLIT-MARKER -->"

                    for h2 in h2_tags:
                        # 在每个h2标签前插入一个唯一的HTML注释作为拆分标记
                        h2.insert_before(BeautifulSoup(SPLIT_MARKER, 'html.parser'))

                    # 按标记将整个HTML分割成多个块
                    html_chunks = str(soup).split(SPLIT_MARKER)

                    # 第一个块是h2之前的内容（通常是头图），可以忽略
                    # 遍历从第一个h2开始的每一个内容块
                    for chunk in html_chunks[1:]:
                        if not chunk.strip():
                            continue

                        chunk_soup = BeautifulSoup(chunk, 'html.parser')

                        title_tag = chunk_soup.find('h2')
                        if not title_tag:
                            continue

                        sub_title = title_tag.get_text(strip=True)
                        title_tag.decompose()  # 移除标题标签，剩下纯内容

                        sub_content = self._clean_html_description(str(chunk_soup))

                        if sub_title and sub_content:
                            processed_articles.append({
                                'title': sub_title,
                                'url': entry.get('link', '').strip(),
                                'publish_time': entry.get('published', ''),
                                'description': sub_content,
                                'source': self.config['name']
                            })

                except Exception as e:
                    print(f"GeekParkRSSParser处理条目时出错: {e}, URL: {entry.get('link', 'N/A')}")
                    continue

            return pd.DataFrame(processed_articles)

        except Exception as e:
            print(f"GeekParkRSSParser解析【{self.config['name']}】失败: {str(e)}")
            return pd.DataFrame()

class TitlesParser(BaseParser):
    """
    一个轻量级的解析器，专门用于快速、稳定地从RSS源中
    提取核心元数据（标题、链接、发布时间），不再尝试抓取正文。
    """

    def parse(self) -> pd.DataFrame:
        print(f"正在解析【{self.config['name']}】...")
        try:
            proxy_handler = urllib.request.ProxyHandler(self.proxies)
            feed = feedparser.parse(self.config['url'], request_headers=self.headers, handlers=[proxy_handler])

            if not feed.entries:
                print(f"  > 警告：【{self.config['name']}】未能解析出任何RSS条目。")
                return pd.DataFrame()

            processed_articles = []
            for entry in feed.entries:
                try:
                    title = entry.get('title', '无标题').strip()

                    # 使用我们之前修复好的健壮的链接提取逻辑
                    link_data = entry.get('link')
                    link = ''
                    if isinstance(link_data, str):
                        link = link_data
                    elif isinstance(link_data, dict):
                        link = link_data.get('href', '')

                    # Atom feed的时间标签通常是 'published' 或 'updated'
                    publish_time = entry.get('published', '') or entry.get('updated', '')

                    # RSS源中的摘要，作为description的备用
                    description = self._clean_html_description(entry.get('summary', ''))

                    processed_articles.append({
                        "title": title,
                        "url": link,
                        "publish_time": publish_time,
                        "description": description,  # description现在是RSS摘要
                        "source": self.config['name']
                    })

                except Exception as e:
                    print(f"  > [错误] 处理条目时发生意外: {e}")
                    continue

            return pd.DataFrame(processed_articles)

        except Exception as e:
            print(f"ReutersParser解析【{self.config['name']}】失败: {str(e)}")
            return pd.DataFrame()