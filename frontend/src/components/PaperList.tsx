import { Paper } from '@/types/paper';
import { useState } from 'react';

interface PaperListProps {
  papers: Paper[];
  onPaperClick: (paper: Paper) => void;
}

export default function PaperList({ papers, onPaperClick }: PaperListProps) {
  const [selectedPaperId, setSelectedPaperId] = useState<string | null>(null);

  const handlePaperClick = (paper: Paper) => {
    setSelectedPaperId(paper.paper_id);
    onPaperClick(paper);
  };

  return (
    <div className="space-y-4">
      {papers.map((paper) => (
        <div
          key={paper.paper_id}
          className={`p-4 rounded-lg cursor-pointer transition-colors ${
            selectedPaperId === paper.paper_id
              ? 'bg-blue-100 hover:bg-blue-200'
              : 'bg-white hover:bg-gray-100'
          }`}
          onClick={() => handlePaperClick(paper)}
        >
          <h3 className="text-lg font-semibold mb-2">{paper.paper_title}</h3>
          <div className="text-sm text-gray-600">
            <p>Authors: {paper.paper_authors}</p>
            <p>Updated: {new Date(paper.update_time).toLocaleDateString()}</p>
          </div>
        </div>
      ))}
    </div>
  );
} 