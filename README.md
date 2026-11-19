# GitHub ETL Project

Pipeline ETL thu thập dữ liệu từ **GitHub REST API** (thông tin repository, thống kê hàng ngày theo UTC, và danh sách issues đã lọc Pull Requests), làm sạch/chuẩn hóa dữ liệu bằng **pandas**, và nạp vào **PostgreSQL** theo cơ chế **UPSERT** (idempotent).

## Cấu trúc thư mục

```text
github-etl-project/
├── README.md
├── requirements.txt
├── .gitignore
├── .env.example
├── src/
│   ├── main.py          # Điểm chạy chính, vòng lặp ETL & logging
│   └── pipeline.py      # Các hàm extract, transform, load
├── tests/
│   └── test_pipeline.py # Unit tests & End-to-End test
├── sql/                 # Các truy vấn phân tích SQL
└── docs/
    └── design.md        # Sơ đồ kiến trúc, ERD & ghi chú thiết kế
```

## 1. Cài đặt môi trường

```bash
# Tạo và kích hoạt virtual environment (khuyến nghị)
python -m venv .venv
.venv\Scripts\activate

# Cài đặt thư viện
pip install -r requirements.txt
```

## 2. Cấu hình biến môi trường

Tạo file `.env` ở thư mục gốc của dự án với nội dung:

```env
GITHUB_TOKEN=ghp_your_github_personal_access_token
DATABASE_URL=postgresql+psycopg2://<username>:<password>@localhost:5432/github_etl
```

## 3. Tạo cơ sở dữ liệu & bảng trong PostgreSQL

Tạo database `github_etl`, sau đó chạy câu lệnh SQL sau để tạo 3 bảng:

```sql
CREATE TABLE IF NOT EXISTS repositories (
    repo_id BIGINT PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    description TEXT,
    primary_language VARCHAR(100),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS repo_daily_stats (
    repo_id BIGINT NOT NULL REFERENCES repositories(repo_id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    stars_count INTEGER,
    forks_count INTEGER,
    open_issues_count INTEGER,
    PRIMARY KEY (repo_id, snapshot_date)
);

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
```

## 4. Chạy Pipeline ETL

```bash
python src/main.py
```

Mặc định pipeline chạy lần lượt cho 3 repository:
- `psf/requests`
- `pallets/flask`
- `pandas-dev/pandas`

Log chạy (thời điểm bắt đầu, kết thúc, thành công/thất bại) được in ra console và lưu vào file `etl.log`.

## 5. Chạy kiểm thử (Tests)

```bash
python -m unittest discover -s tests -v
```

Bộ test bao gồm 5 Unit tests (không gọi API / DB thật) và 1 End-to-End test chạy trên database SQLite tạm thời để kiểm chứng tính idempotent khi chạy lại trong cùng ngày.
