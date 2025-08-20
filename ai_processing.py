import os
import pandas as pd
import json
import time
import google.generativeai as genai
from dotenv import load_dotenv
import logging

def configure_and_get_model():
    """
    安全地加载API密钥并返回一个可用的Gemini模型实例。
    """
    # 从.env文件加载环境变量
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("错误：未设置GEMINI_API_KEY环境变量。请检查你的.env文件。")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.5-flash')
    return model


def get_ranking_and_category_prompt(news_titles_string):
    """
    生成用于评估新闻重要性的Prompt。
    """
    prompt = f"""
    你是一名资深的新闻编辑和行业分析师。你的任务是根据以下新闻标题，评估其重要性并进行分类。
    
    **第一步：分类**
    请先为每个标题从以下四个类别中选择最合适的一个：
    - **科技与商业**: 涵盖前沿技术、公司动态、行业趋势、财经、投融资等。
    - **国际动态**: 涵盖国际关系、地缘政治、主要经济体的宏观动态等。
    - **社会与文化**: 涵盖民生、法律、教育、环境、公共卫生、文化现象等。
    - **国内政策**: 仅指由国家级或省部级政府机构发布的、具有指导性的法规、条例、通知、意见等。
    
    **第二步：评分**
    在分类的基础上，请综合考虑行业影响力、公众关注度、突破性和长远价值，为每个标题给出一个1到10分的重要性评分（10分表示最重要）。
    
    请综合考虑以下几个维度进行打分：
    1.  **行业影响力**：事件是否会对相关行业产生重大影响？
    2.  **公众关注度**：事件是否和广大公众密切相关？
    3.  **技术或政策突破性**：是否代表了一项重大的技术进步或关键的政策变化？
    4.  **长远价值**：事件的长期影响和意义有多大？
    5.  **稀缺性与反常性 (重要)**：该话题是否在主流新闻中不常见？一个通常被视为“小众”或“特定圈层”的话题（如**游戏、绘画、动漫、特定犯罪案件**等）如果出现在标题中，**通常意味着它已经突破了原有圈层，引发了更广泛的社会关注**。对于这类话题，你应该给予**额外的关注和更高的分数**。

    **输出要求**
    你的输出必须是严格的JSON格式，一个包含对象的数组。每个对象必须包含三个字段：'title'（原始新闻标题）、'score'（你的评分）、'category'（你判断的类别）。确保JSON格式正确无误。

    例如，如果输入是：
    - 工信部发布《关于促进人形机器人产业发展的指导意见》
    - 专访xAI核心成员：Grok将如何挑战GPT-5的霸权？
    - 联合国就某地区冲突召开紧急会议

    你的输出应该是：
    ```json
    [
        {{"title": "《关于2025年调整退休人员基本养老金的通知》", "score": 9, "category": "国内政策"}},
        {{"title": "专访xAI核心成员：Grok将如何挑战GPT-5的霸权？", "score": 6, "category": "科技与商业"}},
        {{"title": "布朗队教练：迈尔斯·加雷特最近的超速罚单“令人极其失望”", "score": 2, "category": "国际动态"}}
    ]
    ```

    现在，请评估以下新闻标题列表，并输出它们的评分：
    {news_titles_string}
    """
    return prompt


def ai_rank_and_classify_articles(df: pd.DataFrame):
    """
    使用AI API为DataFrame中的文章进行重要性排序和分类。
    """
    if df.empty:
        logging.warning("输入的数据为空，跳过排序。")
        return df

    try:
        model = configure_and_get_model()
        unique_titles_df = df.drop_duplicates(subset=['title']).copy()
        titles_for_ai = "\n".join([f"- {title}" for title in unique_titles_df['title']])

        prompt = get_ranking_and_category_prompt(titles_for_ai)

        logging.info(f"正在调用AI API进行评估与分类（{len(unique_titles_df)}个独立标题）...")
        response = model.generate_content(prompt)

        json_text = response.text.strip().replace('```json', '').replace('```', '').strip()
        results_list = json.loads(json_text)

        # 创建从 title 到 score 和 category 的映射
        result_map = {item['title']: {'score': item['score'], 'category': item['category']} for item in results_list}

        # 将AI结果映射回原始DataFrame
        df['importance_score'] = df['title'].map(lambda title: result_map.get(title, {}).get('score', 0))
        df['category'] = df['title'].map(lambda title: result_map.get(title, {}).get('category', '未知'))

        logging.info("AI评估与分类完成。")
        # 直接返回带有评分和分类的DataFrame，排序将在main.py中进行
        return df

    except Exception as e:
        logging.error(f"AI评估分类过程中发生错误: {e}")
        df['importance_score'] = 0
        df['category'] = '未知'
        return df

def get_summary_prompt(article_content):
    """
    生成用于精简新闻内容的Prompt。
    """
    prompt = f"""
    你作为专业新闻编辑，请将后续提供的**每条新闻素材独立浓缩为摘要**，严格遵循：

    **硬性要求**
    1. 独立性：每条摘要自成单元，字数≤200字（复杂事件可放宽至250字）
    2. 核心要素：按优先级覆盖→
       ▶ 主体(Who) ▶ 事件(What) ▶ 时间(When) ▶ 地点(Where)  
       ▶ **涉及政策/冲突/决策时必含原因(Why)及关键方式(How)**
    3. 信源标注：
       ▶ 政策/数据 → "据[文件/机构]"（例：据财政部通知）
       ▶ 人物观点 → "[职务]称"（例：市长表示）
       ▶ 争议事实 → "据[机构]通报"（例：据警方初步调查）
       ▶ **多方信源冲突 → 简写标注（例：环保组织(X) vs 企业(Y)）**
    
    **内容准则**
    4. 背景精简：
       ▶ 仅保留和标题有关的内容
       ▶ 保留缺失会导致误解的信息（如职务/政策名称）  
       ▶ **反例：企业成立时间等非核心背景删除**
    5. 争议/数据特殊处理：
       ▶ **对立观点用分号分隔，各≤1句（例：支持者认为A；反对者担忧B）**  
       ▶ **关键数据保留趋势（例：'房价下跌7%'），次要数据删除**
    6. 语言规则：
       ▶ 主动语态优先（正确："欧盟通过法案" / 错误："法案被欧盟通过"）  
       ▶ **删除所有形容词/副词，仅保留事实性定语（例："致命车祸"可保留）**  
       ▶ 单句≤15字，逻辑紧密内容允许≤25字
       ▶ 使用简体中文  
       ▶ **禁用填充词（"值得注意的是""可以说"）、禁止转述直接引语**
    
    **容错机制**
    - 若原文未明确原因/方式 → 标注"原因未说明"或"方式未披露"  
    - 模糊表述继承原文限定词（例："疑似违规"不得简化为"违规"）
    
    新闻内容：
    {article_content}
    """
    return prompt


def ai_summarize_content(df: pd.DataFrame) -> pd.DataFrame:
    """
    使用AI API精简DataFrame中文章的完整内容。
    """
    if df.empty or 'full_content' not in df.columns:
        logging.warning("数据为空或缺少'full_content'列，跳过内容精简。")
        df['summarized_content'] = ""
        return df

    try:
        model = configure_and_get_model()
    except Exception as e:
        logging.error(f"初始化Gemini模型失败: {e}")
        df['summarized_content'] = df['full_content'].apply(lambda x: x[:200] + "..." if len(x) > 200 else x)
        return df  # 返回截断的原文作为后备

    logging.info("正在调用AI API精简文章内容，请稍候...")
    summaries = []

    for index, row in df.iterrows():
        content = row['full_content']
        if not content or len(content) < 50:
            summaries.append(content)
            continue

        prompt = get_summary_prompt(content)
        try:
            response = model.generate_content(prompt)
            summary = response.text.strip()
            summaries.append(summary)
            #print(f"  - 精简了: {row['title']}") # 可以打印进度
            time.sleep(0.5)
        except Exception as e:
            logging.error(f"  > [警告] 精简文章 '{row['title']}' 时出错: {e}。将使用原始内容截断。")
            summaries.append(content[:200] + "..." if len(content) > 200 else content)

    df['summarized_content'] = summaries
    logging.info("内容精简完成。")
    return df
