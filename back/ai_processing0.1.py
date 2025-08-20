import os
import pandas as pd
import json
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
    # 我们只在候选文章中计算频率
    if not candidate_df.empty:
        candidate_df['frequency_bonus'] = candidate_df.groupby('title')['title'].transform('count') - 1
        # 为了不让频率影响过大，可以设置一个上限，例如最多加2分
        candidate_df['frequency_bonus'] = candidate_df['frequency_bonus'].clip(upper=2)

    # --- 阶段二：调用AI进行核心评估 ---
    if not candidate_df.empty:
        try:
            model = configure_and_get_model()

            # 将候选标题格式化为无序列表，让AI更易处理
            titles_for_ai = "\n".join([f"- {title}" for title in candidate_df['title'].unique()])

            prompt = get_ranking_prompt(titles_for_ai)

            print(f"正在调用AI API评估 {len(candidate_df['title'].unique())} 个独立标题，请稍候...")
            response = model.generate_content(prompt)

            json_text = response.text.strip().replace('```json', '').replace('```', '').strip()
            scores_list = json.loads(json_text)

            # 创建从 title 到 score 的映射
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

    # 计算候选文章的最终总分
    if not candidate_df.empty:
        candidate_df['importance_score'] = candidate_df['ai_score'] + candidate_df['frequency_bonus']

    # 为强制保留的文章赋予最高分，确保它们排在最前面
    forced_df['importance_score'] = 11

    # 合并所有文章
    final_df = pd.concat([forced_df, candidate_df], ignore_index=True)

    # 按最终总分排序
    ranked_df = final_df.sort_values(by='importance_score', ascending=False).reset_index(drop=True)

    print("所有文章已根据综合规则排序完毕。")
    return ranked_df