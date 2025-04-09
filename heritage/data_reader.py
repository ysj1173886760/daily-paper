import pandas as pd
import ast
import re  # 新增正则模块

def convert_parquet_to_md(input_path, output_path="daily-papers.md"):
    # 读取数据并按更新时间降序排序
    df = pd.read_parquet(input_path).sort_values(
        by='update_time', 
        ascending=False,
        key=lambda x: pd.to_datetime(x)
    )
    
    with open(output_path, 'w', encoding='utf-8') as md_file:
        for _, row in df.iterrows():
            # 获取论文元数据
            title = str(row['paper_title'])
            update_time = row['update_time'].strftime("%Y-%m-%d")
            
            # 处理摘要内容
            summary_str = ast.literal_eval(str(row['summary']))[0].replace("\\n", "\n")

            # 生成带标题和日期的Markdown
            md_file.write(f"# {title}\n")
            md_file.write(f"**更新时间**: {update_time}\n\n")
            md_file.write(f"{summary_str}\n\n")

if __name__ == "__main__":
    # 使用示例（修改输入文件路径）
    convert_parquet_to_md("./data/daily_papers.parquet")