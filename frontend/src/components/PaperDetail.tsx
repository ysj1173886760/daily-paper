import { Paper } from '@/types/paper';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface PaperDetailProps {
  paper: Paper | null;
}

export default function PaperDetail({ paper }: PaperDetailProps) {
  if (!paper) {
    return (
      <div className="p-6 text-center text-gray-500">
        Select a paper to view details
      </div>
    );
  }

  return (
    <div className="p-6 space-y-4">
      <h2 className="text-2xl font-bold">{paper.paper_title}</h2>
      
      <div className="bg-gray-50 p-4 rounded-lg">
        <h3 className="text-lg font-semibold mb-2">Authors</h3>
        <p className="text-gray-700">{paper.paper_authors}</p>
      </div>

      <div className="bg-gray-50 p-4 rounded-lg">
        <h3 className="text-lg font-semibold mb-2">Abstract</h3>
        <p className="text-gray-700">{paper.paper_abstract}</p>
      </div>

      {paper.summary && (
        <div className="bg-blue-50 p-4 rounded-lg">
          <h3 className="text-lg font-semibold mb-2">AI Summary</h3>
          <div className="prose prose-sm max-w-none text-gray-700">
            <ReactMarkdown remarkPlugins={[remarkGfm]}>
              {paper.summary}
            </ReactMarkdown>
          </div>
        </div>
      )}

      <div className="flex space-x-4">
        <a
          href={paper.paper_url}
          target="_blank"
          rel="noopener noreferrer"
          className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600 transition-colors"
        >
          View Paper
        </a>
      </div>
    </div>
  );
} 