-- Bảng A: repositories
CREATE TABLE IF NOT EXISTS repositories (
    repo_id BIGINT PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    description TEXT,
    primary_language VARCHAR(100),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

-- Bảng B: repo_daily_stats
CREATE TABLE IF NOT EXISTS repo_daily_stats (
    repo_id BIGINT NOT NULL REFERENCES repositories(repo_id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    stars_count INTEGER,
    forks_count INTEGER,
    open_issues_count INTEGER,
    PRIMARY KEY (repo_id, snapshot_date)
);

-- Bảng C: issues
CREATE TABLE IF NOT EXISTS issues (
    issue_id BIGINT PRIMARY KEY,
    repo_id BIGINT NOT NULL REFERENCES repositories(repo_id) ON DELETE CASCADE,
    issue_number INTEGER NOT NULL,
    title TEXT,
    state VARCHAR(20),
    author_login VARCHAR(255),
    comments_count INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ
);