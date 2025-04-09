'use client';

import { useState, useEffect } from 'react';
import PaperList from '@/components/PaperList';
import PaperDetail from '@/components/PaperDetail';
import { Paper, Topic } from '@/types/paper';

// 模拟主题数据，后续可以从API获取
const topics: Topic[] = [
  {
    id: 'rag',
    name: 'RAG',
    description: 'Retrieval-Augmented Generation papers'
  },
  {
    id: 'kg',
    name: 'Knowledge Graph',
    description: 'Knowledge Graph related papers'
  }
];

export default function Home() {
  const [selectedTopic, setSelectedTopic] = useState<string>(topics[0].id);
  const [papers, setPapers] = useState<Paper[]>([]);
  const [selectedPaper, setSelectedPaper] = useState<Paper | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // 加载论文数据
  const loadPapers = async (topicId: string) => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`/api/papers?topic=${topicId}`);
      if (!response.ok) {
        throw new Error('Failed to fetch papers');
      }
      const data = await response.json();
      setPapers(data);
    } catch (error) {
      console.error('Error loading papers:', error);
      setError('Failed to load papers. Please try again later.');
      setPapers([]);
    } finally {
      setLoading(false);
    }
  };

  // 处理主题切换
  const handleTopicChange = (topicId: string) => {
    setSelectedTopic(topicId);
    setSelectedPaper(null);
    loadPapers(topicId);
  };

  // 页面加载时自动加载数据
  useEffect(() => {
    loadPapers(selectedTopic);
  }, []);

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="container mx-auto px-4 py-8">
        <div className="mb-8">
          <h1 className="text-3xl font-bold mb-4">Daily Paper</h1>
          <div className="flex space-x-4">
            {topics.map((topic) => (
              <button
                key={topic.id}
                onClick={() => handleTopicChange(topic.id)}
                className={`px-4 py-2 rounded-lg transition-colors ${
                  selectedTopic === topic.id
                    ? 'bg-blue-500 text-white'
                    : 'bg-white text-gray-700 hover:bg-gray-100'
                }`}
              >
                {topic.name}
              </button>
            ))}
          </div>
        </div>

        {error && (
          <div className="mb-8 p-4 bg-red-100 text-red-700 rounded-lg">
            {error}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          <div className="bg-white rounded-lg shadow-lg overflow-hidden">
            <div className="p-4 bg-gray-100 border-b">
              <h2 className="text-xl font-semibold">Papers</h2>
            </div>
            <div className="p-4">
              {loading ? (
                <div className="text-center py-4">Loading...</div>
              ) : papers.length > 0 ? (
                <PaperList
                  papers={papers}
                  onPaperClick={setSelectedPaper}
                />
              ) : (
                <div className="text-center py-4 text-gray-500">
                  No papers found
                </div>
              )}
            </div>
          </div>

          <div className="bg-white rounded-lg shadow-lg overflow-hidden">
            <div className="p-4 bg-gray-100 border-b">
              <h2 className="text-xl font-semibold">Paper Details</h2>
            </div>
            <PaperDetail paper={selectedPaper} />
          </div>
        </div>
      </div>
    </main>
  );
}
