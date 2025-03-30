import os
import gradio as gr
import pandas as pd
from pathlib import Path

# 默认参数
META_FILE = 'data/daily_papers_test.parquet'

def format_paper(row):
    """格式化单篇论文数据"""
    return f"### {row['paper_title']}\n\n{row['summary']}\n\n"

def load_papers_from_parquet():
    """直接从parquet文件加载论文数据"""
    if Path(META_FILE).exists():
        df = pd.read_parquet(META_FILE)
        return df[['paper_id', 'paper_title', 'summary']] if 'summary' in df.columns else df[['paper_id', 'paper_title']]
    return pd.DataFrame(columns=['paper_id', 'paper_title'])

def create_ui():
    with gr.Blocks(title="论文展示系统") as demo:
        gr.Markdown("## 论文列表")
        
        # 状态显示
        status = gr.Textbox(label="状态", interactive=False)
        
        # 存储论文ID的全局变量
        paper_ids = gr.State([])
        
        # 唯一表格定义
        output_table = gr.Dataframe(
            headers=['论文详情', '操作'],
            datatype=['markdown', 'str'],
            interactive=False,
            wrap=True,
            elem_classes=["markdown-table"]
        )
        
        # 独立PDF查看窗口（调整为Column布局）
        pdf_window = gr.Column(visible=False)
        with pdf_window:
            gr.Markdown("## PDF浏览器")
            pdf_viewer = gr.HTML()
            close_btn = gr.Button("关闭窗口")

        # 新版事件处理函数
        def show_pdf(evt: gr.SelectData, paper_ids):
            if evt.index[1] == 1:
                pdf_path = os.path.join("papers", f"{paper_ids[evt.index[0]]}.pdf")
                if os.path.exists(pdf_path):
                    return (
                        gr.update(visible=True),
                        f'<iframe src="file={pdf_path}" width="100%" height="800px"></iframe>'
                    )
            return gr.update(visible=False), None

        # 绑定唯一事件处理器
        output_table.select(
            fn=show_pdf,
            inputs=[paper_ids],
            outputs=[pdf_window, pdf_viewer]
        )

        # 删除以下残留代码：
        # def refresh_papers(): ... （已有正确实现）
        # 重复的show_pdf定义
        # 旧的output_table.select绑定
        
        # 刷新按钮
        refresh_btn = gr.Button('刷新列表', variant="primary")
        
        # 删除旧的 pdf_viewer = gr.File(...) 定义
        
        def refresh_papers():
            df = load_papers_from_parquet()
            if not df.empty:
                # 格式化论文内容
                formatted = df.apply(format_paper, axis=1)
                # 添加操作按钮
                operations = df['paper_id'].apply(lambda x: "查看原文")
                # 保存论文ID列表
                return f"已加载 {len(df)} 篇论文", list(zip(formatted, operations)), df['paper_id'].tolist()
            return "没有找到论文", [], []
        
        # 修改按钮点击事件
        # 删除以下重复定义的部分（约 90-104 行）：
        # def show_pdf(evt: gr.SelectData, paper_ids):
        #     if evt.index[1] == 1: 
        #         pdf_path = os.path.join("papers", f"{paper_ids[evt.index[0]]}.pdf")
        #         if os.path.exists(pdf_path):
        #             return pdf_path
        #     return None
        
        # output_table.select(
        #     fn=show_pdf,
        #     inputs=[paper_ids],
        #     outputs=pdf_viewer
        # )
        
        refresh_btn.click(
            fn=refresh_papers,
            outputs=[status, output_table, paper_ids]
        )
        
        demo.load(
            fn=refresh_papers,
            outputs=[status, output_table, paper_ids]
        )
    
    return demo

if __name__ == '__main__':
    ui = create_ui()
    ui.launch()