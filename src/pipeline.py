import requests
import pandas as pd
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


def _check_response(response: requests.Response, repo: str, endpoint: str):
    """
    Validates HTTP response status code and distinguishes between success,
    not found, auth/rate-limit errors, and server errors.
    """
    status = response.status_code
    if response.ok:
        return
    if status == 404:
        raise LookupError(f"[{repo}] ({endpoint}) Không tìm thấy repository (HTTP 404).")
    if status in (401, 403, 429):
        remaining = response.headers.get("X-RateLimit-Remaining")
        if status == 429 or remaining == "0":
            reset_ts = response.headers.get("X-RateLimit-Reset")
            raise PermissionError(
                f"[{repo}] ({endpoint}) Vượt giới hạn API rate limit (HTTP {status}, reset={reset_ts})."
            )
        raise PermissionError(f"[{repo}] ({endpoint}) Lỗi quyền truy cập hoặc token không hợp lệ (HTTP {status}).")
    if 500 <= status < 600:
        raise RuntimeError(f"[{repo}] ({endpoint}) Lỗi server GitHub (HTTP {status}).")
    raise RuntimeError(f"[{repo}] ({endpoint}) Request thất bại (HTTP {status}): {response.text[:200]}")


def extract(repo):
    """
    Extracts the data from the given repository.

    Args:
        repo (str): The path to the repository.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Raw (repos_df, daily_stats_df, issues_df).
    """
    load_dotenv()
    my_key = os.getenv("GITHUB_TOKEN")
    headers = {"Authorization": f"Bearer {my_key}"}

    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()

    response1 = requests.get(f'https://api.github.com/repos/{repo}', headers=headers, timeout=30)
    _check_response(response1, repo, "repos")

    data = response1.json()
    df1 = pd.DataFrame([data])

    df2 = df1.copy()
    df2['snapshot_date'] = datetime.now(timezone.utc).date()

    page_number = 1
    while True:
        response2 = requests.get(
            f'https://api.github.com/repos/{repo}/issues?state=all&sort=updated&direction=desc&per_page=30&page={page_number}',
            headers=headers,
            timeout=30,
        )
        _check_response(response2, repo, f"issues page {page_number}")

        data2 = response2.json()
        if not data2:
            break
        df3 = pd.concat([df3, pd.DataFrame(data2)], axis=0, ignore_index=True)
        page_number += 1

    return df1, df2, df3


def transform(df1, df2, df3):
    """
    Flattens and transforms the dataframes by selecting specific columns,
    renaming them, and converting date columns to datetime.

    Args:
        df1 (pd.DataFrame): Repository dataframe.
        df2 (pd.DataFrame): Repository daily stats dataframe.
        df3 (pd.DataFrame): Issues dataframe.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Transformed (repos_df, stats_df, issues_df).
    """
    # Flatten nested JSON columns
    if not df1.empty:
        df1 = pd.json_normalize(df1.to_dict(orient='records'), sep='_')
    if not df2.empty:
        df2 = pd.json_normalize(df2.to_dict(orient='records'), sep='_')
    if not df3.empty:
        # T3. Loại Pull Requests (các mục có trường pull_request)
        if 'pull_request' in df3.columns:
            df3 = df3[df3['pull_request'].isna()].copy()
        if not df3.empty:
            df3 = pd.json_normalize(df3.to_dict(orient='records'), sep='_')

    # Transform repository dataframe (df1 -> repositories)
    for optional_col in ('description', 'language'):
        if optional_col not in df1.columns:
            df1[optional_col] = None

    repos_df = df1[['id', 'full_name', 'description', 'language', 'created_at', 'updated_at']].copy()
    repos_df = repos_df.rename(columns={
        'id': 'repo_id',
        'language': 'primary_language'
    })
    repos_df = repos_df.dropna(subset=['repo_id', 'full_name'])
    repos_df = repos_df[repos_df['full_name'].astype(str).str.strip() != '']
    repos_df = repos_df.drop_duplicates(subset=['repo_id'])
    repos_df['created_at'] = pd.to_datetime(repos_df['created_at'], errors='coerce')
    repos_df['updated_at'] = pd.to_datetime(repos_df['updated_at'], errors='coerce')

    # Transform daily stats dataframe (df2 -> repo_daily_stats)
    stats_columns = ['repo_id', 'snapshot_date', 'stars_count', 'forks_count', 'open_issues_count']
    if df2.empty:
        stats_df = pd.DataFrame(columns=stats_columns)
    else:
        stats_df = df2[['id', 'snapshot_date', 'stargazers_count', 'forks_count', 'open_issues_count']].copy()
        stats_df = stats_df.rename(columns={
            'id': 'repo_id',
            'stargazers_count': 'stars_count'
        })
        stats_df['snapshot_date'] = pd.to_datetime(stats_df['snapshot_date'], utc=True, errors='coerce').dt.date
        stats_df = stats_df.dropna(subset=['repo_id', 'snapshot_date']).drop_duplicates(subset=['repo_id', 'snapshot_date'])
        for count_col in ('stars_count', 'forks_count', 'open_issues_count'):
            stats_df = stats_df[stats_df[count_col].isna() | (stats_df[count_col] >= 0)]
        stats_df = stats_df[stats_columns]

    # Transform issues dataframe (df3 -> issues)
    issue_columns = [
        'issue_id', 'repo_id', 'issue_number', 'title', 'state',
        'author_login', 'comments_count', 'created_at', 'updated_at', 'closed_at'
    ]
    if df3.empty:
        issues_df = pd.DataFrame(columns=issue_columns)
    else:
        if 'user_login' not in df3.columns:
            df3['user_login'] = None

        issues_df = df3[['id', 'number', 'title', 'state', 'user_login', 'comments', 'created_at', 'updated_at', 'closed_at']].copy()
        issues_df = issues_df.rename(columns={
            'id': 'issue_id',
            'number': 'issue_number',
            'user_login': 'author_login',
            'comments': 'comments_count'
        })
        issues_df['repo_id'] = repos_df['repo_id'].iloc[0] if not repos_df.empty else None
        issues_df['author_login'] = issues_df['author_login'].where(issues_df['author_login'].notna(), None)
        issues_df = issues_df.dropna(subset=['issue_id']).drop_duplicates(subset=['issue_id'])
        issues_df = issues_df[issues_df['comments_count'].isna() | (issues_df['comments_count'] >= 0)]

        for col in ['created_at', 'updated_at', 'closed_at']:
            issues_df[col] = pd.to_datetime(issues_df[col], errors='coerce')

        issues_df = issues_df[issue_columns]

    return repos_df, stats_df, issues_df

def _make_upsert_method(conflict_cols: list[str]):
    """Returns a pandas to_sql callable that performs UPSERT (PostgreSQL or SQLite)."""
    def _upsert(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        if not data:
            return
        dialect_insert = sqlite_insert if conn.dialect.name == "sqlite" else pg_insert
        stmt = dialect_insert(table.table).values(data)
        update_cols = {
            col.name: getattr(stmt.excluded, col.name)
            for col in table.table.columns
            if col.name not in conflict_cols
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_=update_cols
        )
        conn.execute(stmt)
    return _upsert


def load(
    repos_df: pd.DataFrame,
    stats_df: pd.DataFrame,
    issues_df: pd.DataFrame,
):
    """
    Loads the transformed dataframes into PostgreSQL using to_sql with UPSERT
    and verifies the loaded row counts.

    Args:
        repos_df (pd.DataFrame): Transformed repository dataframe.
        stats_df (pd.DataFrame): Transformed repository daily stats dataframe.
        issues_df (pd.DataFrame): Transformed issues dataframe.
    """
    load_dotenv()
    db_connection_string = os.getenv("DATABASE_URL")
    if not db_connection_string:
        raise ValueError("Missing DATABASE_URL environment variable.")

    engine = create_engine(db_connection_string)

    # L2. Nạp metadata (UPSERT repositories theo repo_id)
    if not repos_df.empty:
        repos_df.to_sql(
            'repositories',
            engine,
            if_exists='append',
            index=False,
            method=_make_upsert_method(['repo_id'])
        )

    # L3. Nạp snapshot theo ngày (UPSERT repo_daily_stats theo repo_id + snapshot_date)
    if not stats_df.empty:
        stats_df.to_sql(
            'repo_daily_stats',
            engine,
            if_exists='append',
            index=False,
            method=_make_upsert_method(['repo_id', 'snapshot_date'])
        )

    # L4. Nạp issues (UPSERT issues theo issue_id)
    if not issues_df.empty:
        issues_df.to_sql(
            'issues',
            engine,
            if_exists='append',
            index=False,
            method=_make_upsert_method(['issue_id'])
        )

    # L5. Kiểm tra sau nạp
    db_counts = {}
    expected_counts = {
        'repositories': len(repos_df),
        'repo_daily_stats': len(stats_df),
        'issues': len(issues_df),
    }

    print("=== Load Verification ===")
    for table in ('repositories', 'repo_daily_stats', 'issues'):
        count = int(pd.read_sql(f"SELECT COUNT(*) AS cnt FROM {table}", engine)['cnt'].iloc[0])
        db_counts[table] = count
        print(f"{table}: loaded_batch={expected_counts[table]}, total_in_db={count}")

    engine.dispose()
    return db_counts