import { useQuery } from '@tanstack/react-query';

const GITHUB_OWNER = 'kennethhuang7';
const GITHUB_REPO = 'CourtVision';

declare global {
  interface Window {
    electron?: {
      fetchPipelineStatus?: () => Promise<{ success: boolean; data?: GitHubActionsResponse; error?: string }>;
    };
  }
}

interface WorkflowRun {
  id: number;
  name: string;
  status: 'queued' | 'in_progress' | 'completed' | 'waiting';
  conclusion: 'success' | 'failure' | 'cancelled' | 'skipped' | 'timed_out' | 'action_required' | null;
  created_at: string;
  updated_at: string;
  run_started_at: string;
  html_url: string;
}

interface GitHubActionsResponse {
  total_count: number;
  workflow_runs: WorkflowRun[];
}

export interface PipelineRun {
  id: number;
  name: string;
  status: 'success' | 'failure' | 'in_progress' | 'cancelled' | 'unknown';
  timestamp: Date;
  url: string;
}

function mapWorkflowRun(run: WorkflowRun): PipelineRun {
  let status: PipelineRun['status'] = 'unknown';

  if (run.status === 'in_progress' || run.status === 'queued' || run.status === 'waiting') {
    status = 'in_progress';
  } else if (run.status === 'completed') {
    if (run.conclusion === 'success') {
      status = 'success';
    } else if (run.conclusion === 'failure' || run.conclusion === 'timed_out') {
      status = 'failure';
    } else if (run.conclusion === 'cancelled' || run.conclusion === 'skipped') {
      status = 'cancelled';
    }
  }

  return {
    id: run.id,
    name: run.name,
    status,
    timestamp: new Date(run.run_started_at || run.created_at),
    url: run.html_url,
  };
}

async function fetchPipelineRuns(): Promise<PipelineRun[]> {
  if (window.electron?.fetchPipelineStatus) {
    const result = await window.electron.fetchPipelineStatus();
    if (!result.success || !result.data) {
      throw new Error(result.error || 'Failed to fetch pipeline status');
    }
    return result.data.workflow_runs.map(mapWorkflowRun);
  }

  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/actions/runs?per_page=10`;

  try {
    const response = await fetch(url, {
      headers: {
        'Accept': 'application/vnd.github.v3+json',
      },
      mode: 'cors',
    });

    if (!response.ok) {
      const errorText = await response.text().catch(() => '');
      console.error(`Pipeline status fetch failed: ${response.status}`, errorText);

      if (response.status === 403) {
        throw new Error('GitHub API rate limit exceeded');
      }
      if (response.status === 404) {
        throw new Error('Repository not found or private');
      }
      throw new Error(`Failed to fetch pipeline status: ${response.status}`);
    }

    const data: GitHubActionsResponse = await response.json();
    return data.workflow_runs.map(mapWorkflowRun);
  } catch (error) {
    console.error('Pipeline status fetch error:', error);
    throw error;
  }
}

export function usePipelineStatus() {
  return useQuery({
    queryKey: ['pipeline-status'],
    queryFn: fetchPipelineRuns,
    staleTime: 5 * 60 * 1000,
    refetchInterval: 5 * 60 * 1000,
    retry: 1,
  });
}
