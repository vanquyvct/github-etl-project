import os
import sys
import tempfile
import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
from sqlalchemy import create_engine, text

# Allow importing pipeline from src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from pipeline import extract, transform, load


def _sample_repo_dict(**overrides):
    data = {
        "id": 1362490,
        "full_name": "psf/requests",
        "description": "A simple, yet elegant, HTTP library.",
        "language": "Python",
        "stargazers_count": 52000,
        "forks_count": 9300,
        "open_issues_count": 300,
        "created_at": "2011-02-13T18:38:17Z",
        "updated_at": "2026-11-19T00:00:00Z",
    }
    data.update(overrides)
    return data


def _sample_issue_dict(**overrides):
    data = {
        "id": 1001,
        "number": 10,
        "title": "Sample bug report",
        "state": "open",
        "user": {"login": "alice"},
        "comments": 3,
        "created_at": "2026-11-10T10:00:00Z",
        "updated_at": "2026-11-18T12:00:00Z",
        "closed_at": None,
    }
    data.update(overrides)
    return data


class TestPipelineUnit(unittest.TestCase):
    """Unit tests cho bước Transform (dữ liệu mẫu, không gọi API / DB thật)."""

    def test_1_valid_repository_json_has_expected_columns(self):
        repo = _sample_repo_dict()
        df1 = pd.DataFrame([repo])
        df2 = df1.copy()
        df2["snapshot_date"] = date(2026, 11, 19)
        df3 = pd.DataFrame([_sample_issue_dict()])

        repos_df, stats_df, issues_df = transform(df1, df2, df3)

        self.assertEqual(
            list(repos_df.columns),
            ["repo_id", "full_name", "description", "primary_language", "created_at", "updated_at"],
        )
        self.assertEqual(len(repos_df), 1)
        self.assertEqual(repos_df["repo_id"].iloc[0], 1362490)
        self.assertEqual(repos_df["primary_language"].iloc[0], "Python")

        self.assertEqual(
            list(stats_df.columns),
            ["repo_id", "snapshot_date", "stars_count", "forks_count", "open_issues_count"],
        )
        self.assertEqual(stats_df["stars_count"].iloc[0], 52000)

        self.assertEqual(len(issues_df), 1)
        self.assertEqual(issues_df["author_login"].iloc[0], "alice")

    def test_2_missing_description_and_language_does_not_crash(self):
        # Thiếu hoàn toàn key description và language trong JSON
        repo = _sample_repo_dict()
        del repo["description"]
        del repo["language"]

        df1 = pd.DataFrame([repo])
        df2 = df1.copy()
        df2["snapshot_date"] = date(2026, 11, 19)
        df3 = pd.DataFrame()

        repos_df, stats_df, issues_df = transform(df1, df2, df3)

        self.assertEqual(len(repos_df), 1)
        self.assertIsNone(repos_df["description"].iloc[0])
        self.assertIsNone(repos_df["primary_language"].iloc[0])
        self.assertTrue(issues_df.empty)

    def test_3_issues_containing_pull_requests_filters_out_prs(self):
        df1 = pd.DataFrame([_sample_repo_dict()])
        df2 = df1.copy()
        df2["snapshot_date"] = date(2026, 11, 19)

        real_issue = _sample_issue_dict(id=1001, number=10, title="Real Issue")
        pr_item = _sample_issue_dict(
            id=1002,
            number=11,
            title="Fix typo PR",
            pull_request={"url": "https://api.github.com/repos/psf/requests/pulls/11"},
        )
        df3 = pd.DataFrame([real_issue, pr_item])

        _, _, issues_df = transform(df1, df2, df3)

        self.assertEqual(len(issues_df), 1)
        self.assertEqual(issues_df["issue_id"].iloc[0], 1001)

    def test_4_duplicate_ids_are_deduplicated(self):
        repo = _sample_repo_dict()
        df1 = pd.DataFrame([repo, repo])
        df2 = df1.copy()
        df2["snapshot_date"] = date(2026, 11, 19)

        issue = _sample_issue_dict(id=2001)
        df3 = pd.DataFrame([issue, issue])

        repos_df, stats_df, issues_df = transform(df1, df2, df3)

        self.assertEqual(len(repos_df), 1)
        self.assertEqual(len(stats_df), 1)
        self.assertEqual(len(issues_df), 1)

    def test_5_negative_counts_and_missing_ids_are_rejected(self):
        valid_repo = _sample_repo_dict(id=1)
        missing_id_repo = _sample_repo_dict(id=None, full_name="org/no-id")
        df1 = pd.DataFrame([valid_repo, missing_id_repo])

        valid_stat = _sample_repo_dict(id=1, stargazers_count=100, forks_count=10, open_issues_count=5)
        negative_stat = _sample_repo_dict(id=2, stargazers_count=-5, forks_count=10, open_issues_count=5)
        df2 = pd.DataFrame([valid_stat, negative_stat])
        df2["snapshot_date"] = date(2026, 11, 19)

        valid_issue = _sample_issue_dict(id=3001, comments=2)
        missing_id_issue = _sample_issue_dict(id=None, comments=1)
        negative_comments_issue = _sample_issue_dict(id=3002, comments=-3)
        df3 = pd.DataFrame([valid_issue, missing_id_issue, negative_comments_issue])

        repos_df, stats_df, issues_df = transform(df1, df2, df3)

        self.assertEqual(list(repos_df["repo_id"]), [1])
        self.assertEqual(list(stats_df["repo_id"]), [1])
        self.assertEqual(list(issues_df["issue_id"]), [3001])


class TestPipelineEndToEnd(unittest.TestCase):
    """End-to-End test trên database thử nghiệm riêng (SQLite tạm thời)."""

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp_dir.name, "test_github_etl.db")
        self.db_url = f"sqlite:///{self.db_path}"
        self.engine = create_engine(self.db_url)

        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE repositories (
                    repo_id BIGINT PRIMARY KEY,
                    full_name VARCHAR(255) NOT NULL,
                    description TEXT,
                    primary_language VARCHAR(100),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                );
            """))
            conn.execute(text("""
                CREATE TABLE repo_daily_stats (
                    repo_id BIGINT NOT NULL,
                    snapshot_date DATE NOT NULL,
                    stars_count INTEGER,
                    forks_count INTEGER,
                    open_issues_count INTEGER,
                    PRIMARY KEY (repo_id, snapshot_date)
                );
            """))
            conn.execute(text("""
                CREATE TABLE issues (
                    issue_id BIGINT PRIMARY KEY,
                    repo_id BIGINT NOT NULL,
                    issue_number INTEGER NOT NULL,
                    title TEXT,
                    state VARCHAR(20),
                    author_login VARCHAR(255),
                    comments_count INTEGER,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    closed_at TIMESTAMP
                );
            """))

    def tearDown(self):
        self.engine.dispose()
        self.tmp_dir.cleanup()

    @staticmethod
    def _mock_response(payload, status_code=200):
        resp = MagicMock()
        resp.status_code = status_code
        resp.ok = 200 <= status_code < 300
        resp.json.return_value = payload
        resp.headers = {}
        return resp

    @patch("pipeline.load_dotenv")
    @patch("pipeline.requests.get")
    def test_e2e_etl_and_idempotent_rerun_same_day(self, mock_get, _mock_dotenv):
        with patch.dict(os.environ, {"DATABASE_URL": self.db_url}):
            # Lần chạy 1: 1 repo + 1 issue thật + 1 PR (sẽ bị loại)
            repo_v1 = _sample_repo_dict(stargazers_count=52000)
            issue_v1 = _sample_issue_dict(id=5001, state="open", comments=3)
            pr_item = _sample_issue_dict(
                id=5002,
                pull_request={"url": "https://api.github.com/repos/psf/requests/pulls/11"},
            )
            mock_get.side_effect = [
                self._mock_response(repo_v1),
                self._mock_response([issue_v1, pr_item]),
                self._mock_response([]),
            ]

            df1, df2, df3 = extract("psf/requests")
            repos_df, stats_df, issues_df = transform(df1, df2, df3)
            counts_run1 = load(repos_df, stats_df, issues_df)

            self.assertEqual(counts_run1, {"repositories": 1, "repo_daily_stats": 1, "issues": 1})

            # Lần chạy 2 cùng ngày với dữ liệu cập nhật: stars tăng, issue đóng và thêm comment
            repo_v2 = _sample_repo_dict(stargazers_count=52100)
            issue_v2 = _sample_issue_dict(id=5001, state="closed", comments=7)
            mock_get.side_effect = [
                self._mock_response(repo_v2),
                self._mock_response([issue_v2]),
                self._mock_response([]),
            ]

            df1, df2, df3 = extract("psf/requests")
            repos_df, stats_df, issues_df = transform(df1, df2, df3)
            counts_run2 = load(repos_df, stats_df, issues_df)

            # Xác nhận không phát sinh bản ghi trùng
            self.assertEqual(counts_run2, {"repositories": 1, "repo_daily_stats": 1, "issues": 1})

            # Đọc lại từ DB kiểm tra dữ liệu đã được cập nhật đúng
            stats_in_db = pd.read_sql("SELECT * FROM repo_daily_stats", self.engine)
            issues_in_db = pd.read_sql("SELECT * FROM issues", self.engine)

            self.assertEqual(int(stats_in_db["stars_count"].iloc[0]), 52100)
            self.assertEqual(issues_in_db["state"].iloc[0], "closed")
            self.assertEqual(int(issues_in_db["comments_count"].iloc[0]), 7)


if __name__ == "__main__":
    unittest.main()
