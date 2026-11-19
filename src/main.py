import logging
from datetime import datetime, timezone
from pipeline import extract, transform, load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("etl.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

REPOS = [
    "psf/requests",
    "pallets/flask",
    "pandas-dev/pandas",
]

if __name__ == "__main__":
    pipeline_start = datetime.now(timezone.utc)
    logger.info("Pipeline STARTED at %s", pipeline_start.isoformat())

    succeeded = []
    failed = []

    for repo in REPOS:
        repo_start = datetime.now(timezone.utc)
        logger.info("[%s] ETL STARTED at %s", repo, repo_start.isoformat())
        try:
            df1, df2, df3 = extract(repo)
            logger.info("[%s] Extracted shapes: repos=%s, stats=%s, issues=%s", repo, df1.shape, df2.shape, df3.shape)

            repos_df, stats_df, issues_df = transform(df1, df2, df3)
            logger.info("[%s] Transformed shapes: repos=%s, stats=%s, issues=%s", repo, repos_df.shape, stats_df.shape, issues_df.shape)

            info = load(repos_df, stats_df, issues_df)
            repo_end = datetime.now(timezone.utc)
            logger.info("[%s] ETL SUCCEEDED at %s | DB counts: %s", repo, repo_end.isoformat(), info)
            succeeded.append(repo)
        except Exception as e:
            repo_end = datetime.now(timezone.utc)
            logger.exception("[%s] ETL FAILED at %s | Error: %s", repo, repo_end.isoformat(), e)
            failed.append(repo)

    pipeline_end = datetime.now(timezone.utc)
    status = "SUCCESS" if not failed else "PARTIAL/FAILED"
    logger.info(
        "Pipeline FINISHED at %s | Status: %s | Succeeded: %d | Failed: %d",
        pipeline_end.isoformat(),
        status,
        len(succeeded),
        len(failed),
    )
