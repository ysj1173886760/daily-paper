import pandas as pd
import json
from pathlib import Path

def convert_parquet_to_json(parquet_file: str, json_file: str):
    # 读取 parquet 文件
    df = pd.read_parquet(parquet_file)
    
    # 转换日期列为字符串
    if 'publish_time' in df.columns:
        df['publish_time'] = df['publish_time'].astype(str)
    if 'update_time' in df.columns:
        df['update_time'] = df['update_time'].astype(str)
    
    # 转换为 JSON 并保存
    papers = df.to_dict(orient='records')
    
    # 确保目标目录存在
    Path(json_file).parent.mkdir(parents=True, exist_ok=True)
    
    # 保存为 JSON 文件
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(papers, f, ensure_ascii=False, indent=2)

def main():
    # 转换 RAG 相关论文
    convert_parquet_to_json(
        '../data/daily_papers.parquet',
        'public/data/rag_papers.json'
    )
    
    # 转换知识图谱相关论文
    convert_parquet_to_json(
        '../data/daily_papers_kg.parquet',
        'public/data/kg_papers.json'
    )

if __name__ == '__main__':
    main() 