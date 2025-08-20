import os
import pandas as pd
import json
import time
import google.generativeai as genai
from dotenv import load_dotenv


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


def get_ranking_prompt(news_titles_string):
    """
    生成用于评估新闻重要性的Prompt。
    """
    prompt = f"""
    你是一名资深的新闻编辑和行业分析师。你的任务是根据以下新闻标题，评估每条新闻的综合重要性。

    请综合考虑以下几个维度进行打分：
    1.  **行业影响力**：事件是否会对相关行业产生重大影响？
    2.  **公众关注度**：事件是否会引起广大公众的兴趣和讨论？
    3.  **技术或政策突破性**：是否代表了一项重大的技术进步或关键的政策变化？
    4.  **长远价值**：事件的长期影响和意义有多大？

    请为每个标题给出一个1到10分的重要性评分（10分表示最重要，1分表示价值最低）。

    你的输出必须是严格的JSON格式，一个包含对象的数组，每个对象包含'title'（原始新闻标题）和'score'（你的评分）。确保JSON格式正确无误，不要包含任何额外的解释或标记。

    例如，如果输入是：
    - 中国发布新的AI管理法规
    - 某知名公司推出新款手机壳

    你的输出应该是：
    ```json
    [
        {{"title": "中国发布新的AI管理法规", "score": 9}},
        {{"title": "某知名公司推出新款手机壳", "score": 2}}
    ]
    ```

    现在，请评估以下新闻标题列表，并输出它们的评分：
    {news_titles_string}
    """
    return prompt


def ai_rank_articles(df: pd.DataFrame):
    """
    使用两阶段方法为文章排序：
    1. Python代码处理确定性规则（强制保留、频率加分）。
    2. AI处理模糊的重要性判断。
    """
    if df.empty:
        print("输入的数据为空，跳过排序。")
        return df

    # 规则1：强制保留
    force_keep_keywords = ['8点1氪', '氪星晚报']
    is_forced = df['title'].str.contains('|'.join(force_keep_keywords), case=False, na=False)

    forced_df = df[is_forced].copy()
    candidate_df = df[~is_forced].copy()

    print(f"检测到 {len(forced_df)} 篇强制保留文章。")

    # 规则2：计算标题频率作为潜在加分项
    if not candidate_df.empty:
        candidate_df['frequency_bonus'] = candidate_df.groupby('title')['title'].transform('count') - 1
        # 为了不让频率影响过大，可以设置一个上限，例如最多加2分
        candidate_df['frequency_bonus'] = candidate_df['frequency_bonus'].clip(upper=2)

    # --- 阶段二：调用AI进行核心评估 ---
    if not candidate_df.empty:
        try:
            model = configure_and_get_model()

            titles_for_ai = "\n".join([f"- {title}" for title in candidate_df['title'].unique()])

            prompt = get_ranking_prompt(titles_for_ai)

            print(f"正在调用AI API评估 {len(candidate_df['title'].unique())} 个独立标题，请稍候...")
            response = model.generate_content(prompt)

            json_text = response.text.strip().replace('```json', '').replace('```', '').strip()
            scores_list = json.loads(json_text)

            score_map = {item['title']: item['score'] for item in scores_list}
            # 将AI评分映射回候选DataFrame
            candidate_df['ai_score'] = candidate_df['title'].map(score_map).fillna(0)

        except Exception as e:
            print(f"AI评估过程中发生错误: {e}")
            print("将仅使用本地规则进行排序。")
            candidate_df['ai_score'] = 0  # AI失败则评分为0
    else:
        print("没有需要AI评估的文章，跳过API调用。")

    # --- 阶段三：合并与最终计分 ---
    # 为强制保留的文章赋予一个超高分，确保它们在最终列表的顶部
    forced_df['importance_score'] = 11

    if not candidate_df.empty:
        candidate_df['importance_score'] = candidate_df['ai_score'] + candidate_df.get('frequency_bonus', 0)
        ranked_candidates_df = candidate_df.sort_values(by='importance_score', ascending=False)
        top_20_candidates_df = ranked_candidates_df.head(20)
        final_df = pd.concat([forced_df, top_20_candidates_df], ignore_index=True)
    else:
        final_df = forced_df

    # 这一步是可选的，但能让输出更有条理
    final_output_df = final_df.sort_values(by='importance_score', ascending=False).reset_index(drop=True)

    print("所有文章已根据新规则筛选和排序完毕。")
    return final_output_df


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
    :param df: 包含'full_content'列的DataFrame。
    :return: 增加了'summarized_content'列的DataFrame。
    """
    if df.empty or 'full_content' not in df.columns:
        print("数据为空或缺少'full_content'列，跳过内容精简。")
        df['summarized_content'] = ""
        return df

    try:
        model = configure_and_get_model()
    except Exception as e:
        print(f"初始化Gemini模型失败: {e}")
        df['summarized_content'] = df['full_content'].apply(lambda x: x[:200] + "..." if len(x) > 200 else x)
        return df  # 返回截断的原文作为后备

    print("正在调用AI API精简文章内容，请稍候...")
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
            # print(f"  - 精简了: {row['title']}") # 可以打印进度
            time.sleep(0.5)  # 少量延迟，避免频率限制
        except Exception as e:
            print(f"  > [警告] 精简文章 '{row['title']}' 时出错: {e}。将使用原始内容截断。")
            summaries.append(content[:200] + "..." if len(content) > 200 else content)

    df['summarized_content'] = summaries
    print("内容精简完成。")
    return df