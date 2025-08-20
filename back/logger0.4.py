import logging
import os
from datetime import datetime


def setup_logging():
    """
    配置全局日志记录器。
    - 输出到控制台 (INFO 级别及以上)
    - 输出到带时间戳的文件 (DEBUG 级别及以上)
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # 防止重复添加处理器
    if logger.hasHandlers():
        logger.handlers.clear()

    # 2. 创建日志文件夹
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 3. 创建文件处理器
    log_filename = os.path.join(log_dir, f"news_aggregator_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)

    # 4. 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # 5. 定义日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 6. 将处理器添加到日志记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info("日志系统初始化完成。")