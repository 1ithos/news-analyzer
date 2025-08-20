import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, config: dict):
        """
        初始化数据库管理器。
        :param config: 从config.yaml加载的全局配置字典。
        """
        db_config = config.get('database', {})
        self.db_path = db_config.get('file_path', 'news_archive.db')
        self.table_name = db_config.get('table_name', 'articles')
        self.conn = None
        self._connect_and_init()

    def _connect_and_init(self):
        """连接到数据库并初始化表结构。"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            cursor = self.conn.cursor()
            # url 作为主键，天然去重
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                url TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT,
                publish_time TEXT,
                description TEXT,
                insert_timestamp INTEGER
            )
            """)
            self.conn.commit()
            logger.info(f"成功连接并初始化数据库: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"数据库连接或初始化失败: {e}")
            raise

    def get_existing_urls(self) -> set:
        """
        从数据库中获取所有已存在的URL。
        返回一个集合(set)，用于快速查找。
        """
        if not self.conn:
            return set()
        try:
            df = pd.read_sql_query(f"SELECT url FROM {self.table_name}", self.conn)
            return set(df['url'])
        except Exception as e:
            logger.error(f"从数据库获取URL失败: {e}")
            return set()

    def write_new_articles(self, df: pd.DataFrame):
        """
        将新的文章DataFrame写入数据库。
        """
        if df.empty or not self.conn:
            return

        try:
            # 添加插入时的时间戳，用于后续清理
            df['insert_timestamp'] = int(datetime.now().timestamp())

            # 使用Pandas的to_sql方法，如果表已存在则追加
            # if_exists='append' 确保不会覆盖旧数据
            # index=False 表示不将DataFrame的索引写入数据库
            # 'url'是主键，如果尝试插入已存在的URL，to_sql会失败，所以我们前面做了增量检查
            df.to_sql(self.table_name, self.conn, if_exists='append', index=False)
            logger.info(f"成功将 {len(df)} 篇新文章写入数据库。")
        except Exception as e:
            logger.error(f"写入新文章到数据库失败: {e}")

    def clean_old_articles(self, days_to_keep: int):
        """
        根据插入时间戳，删除超过指定天数的旧数据。
        """
        if not (self.conn and days_to_keep > 0):
            return

        try:
            # 计算N天前的时间戳
            cutoff_timestamp = int((datetime.now() - timedelta(days=days_to_keep)).timestamp())

            cursor = self.conn.cursor()

            # 执行删除操作
            cursor.execute(f"DELETE FROM {self.table_name} WHERE insert_timestamp < ?", (cutoff_timestamp,))
            deleted_rows = cursor.rowcount
            self.conn.commit()

            if deleted_rows > 0:
                logger.info(f"数据清理完成，删除了 {deleted_rows} 条超过 {days_to_keep} 天的旧数据。")
            else:
                logger.info("数据清理：没有需要删除的旧数据。")

        except Exception as e:
            logger.error(f"清理旧文章时出错: {e}")

    def close(self):
        """关闭数据库连接。"""
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭。")