export interface Paper {
  paper_id: string;
  paper_title: string;
  paper_url: string;
  paper_abstract: string;
  paper_authors: string;
  paper_first_author: string;
  primary_category: string;
  publish_time: string;
  update_time: string;
  comments?: string;
  summary?: string;
}

export interface Topic {
  id: string;
  name: string;
  description: string;
} 