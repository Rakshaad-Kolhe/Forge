-- Forge V2: PR 09 - Job Execution Requirements
ALTER TABLE jobs
ADD COLUMN IF NOT EXISTS requirements JSONB NOT NULL DEFAULT '{}'::jsonb;
