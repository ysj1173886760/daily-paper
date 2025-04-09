import { NextResponse } from 'next/server';
import path from 'path';
import fs from 'fs';
import { Paper } from '@/types/paper';

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const topic = searchParams.get('topic');

  if (!topic) {
    return NextResponse.json({ error: 'Topic is required' }, { status: 400 });
  }

  try {
    const filePath = path.join(process.cwd(), 'public', 'data', `${topic}_papers.json`);
    
    if (!fs.existsSync(filePath)) {
      return NextResponse.json({ error: 'Data file not found' }, { status: 404 });
    }

    const fileContent = fs.readFileSync(filePath, 'utf-8');
    const papers: Paper[] = JSON.parse(fileContent);

    // 按更新时间排序
    papers.sort((a, b) => 
      new Date(b.update_time).getTime() - new Date(a.update_time).getTime()
    );

    return NextResponse.json(papers);
  } catch (error) {
    console.error('Error reading JSON file:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
} 