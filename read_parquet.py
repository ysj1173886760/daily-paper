import pandas as pd
import ast
import re  # 新增正则模块

def convert_parquet_to_md(input_path, output_path="daily-papers.md"):
    df = pd.read_parquet(input_path)
    
    with open(output_path, 'w', encoding='utf-8') as md_file:
        for _, row in df.iterrows():
            paper_id = str(row['paper_id'])

            summary_str = ast.literal_eval(str(row['summary']))[0].replace("\\n", "\n")

            md_file.write(f"# {paper_id}\n\n{summary_str}\n\n")

if __name__ == "__main__":
    # 使用示例（修改输入文件路径）
    convert_parquet_to_md("./data/daily_papers.parquet")