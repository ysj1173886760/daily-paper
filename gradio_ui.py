import gradio as gr
import pandas as pd
from main import get_daily_papers, filter_existing_papers, save_to_parquet

# 默认查询参数
DEFAULT_QUERY = '"RAG" OR "Retrieval-Augmented Generation"'
MAX_RESULTS = 20
META_FILE = 'data/daily_papers_test.parquet'

# 获取论文数据
def get_papers(query=DEFAULT_QUERY, max_results=MAX_RESULTS):
    new_papers = get_daily_papers(query, max_results)
    filtered_papers = filter_existing_papers(new_papers, META_FILE)
    save_to_parquet(filtered_papers, META_FILE)
    df = pd.read_parquet(META_FILE)
    return df[['paper_title', 'paper_authors', 'paper_abstract', 'paper_url']]

# 创建Gradio界面
def create_ui():
    with gr.Blocks() as demo:
        with gr.Row():
            query_input = gr.Textbox(label='搜索查询', value=DEFAULT_QUERY)
            max_results_input = gr.Number(label='最大结果数', value=MAX_RESULTS, precision=0)
            refresh_btn = gr.Button('刷新')
        
        output_table = gr.Dataframe(
            headers=['标题', '作者', '摘要', '下载链接'],
            datatype=['str', 'str', 'str', 'str'],
            interactive=False
        )
        
        refresh_btn.click(
            fn=get_papers,
            inputs=[query_input, max_results_input],
            outputs=output_table
        )
        
        # 初始加载数据
        demo.load(
            fn=get_papers,
            inputs=[query_input, max_results_input],
            outputs=output_table
        )
    return demo

if __name__ == '__main__':
    ui = create_ui()
    ui.launch()