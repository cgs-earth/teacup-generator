export type Change = {
  row: number;
  column: string;
  before: string;
  after: string;
};
export type CsvDocument = {
  text: string;
  headers: string[];
  rows: string[][];
  spans: { start: number; end: number }[][];
};
export type Snapshot = {
  text: string;
  sha: string;
  commitSha: string | null;
  repository: string;
  baseBranch: string;
  path: string;
};
export type Session = {
  configured: boolean;
  repository: string;
  baseBranch: string;
  user: null | { login: string; name: string; id: number };
  csrf?: string;
};
export type PullRequest = { url: string; number: number; branch: string };
