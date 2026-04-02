"""
incremental_update.py
=====================
Tự động phát hiện và xử lý các tháng NYC Taxi chưa được pipeline xử lý.

State file: logs/processed_months.json
  {
    "processed": ["2022-01", "2022-02", ...],
    "last_updated": "2026-03-31T22:00:00"
  }

Logic:
  1. Đọc state -> biết tháng nào đã xử lý
  2. Tính target: từ BASE_START đến (hôm nay - LAG_MONTHS)
     vì TLC publish data trễ ~2 tháng
  3. Lọc tháng chưa xử lý -> check URL tồn tại (HTTP HEAD)
  4. Chạy pipeline: extract -> spark -> fetch
  5. Cập nhật state
  6. Chạy dbt nếu có tháng mới
"""

import os
import sys
import json
import argparse
import logging
import subprocess
import requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

# ── Config ────────────────────────────────────────────────────────────────────
ROOT_DIR   = Path(__file__).resolve().parent.parent
LOG_DIR    = ROOT_DIR / "logs"
STATE_FILE = LOG_DIR / "processed_months.json"
DBT_DIR    = ROOT_DIR / "nyc_taxi_dbt"

# Tháng bắt đầu khi state file chưa tồn tại
BASE_START = date(2022, 1, 1)

# TLC publish data trễ ~2 tháng
LAG_MONTHS = 2

TLC_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)
# ─────────────────────────────────────────────────────────────────────────────


def load_state() -> dict:
    """Đọc state file, tạo mới nếu chưa có."""
    if STATE_FILE.exists():
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"processed": [], "last_updated": None}


def save_state(state: dict):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    state["last_updated"] = datetime.utcnow().isoformat()
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
    logging.info(f"State saved -> {STATE_FILE}")


def get_target_months(processed: list[str]) -> list[tuple[int, int]]:
    """Tính danh sách tháng cần xử lý."""
    cutoff = date.today() - relativedelta(months=LAG_MONTHS)
    cutoff = cutoff.replace(day=1)

    targets = []
    cursor = BASE_START
    while cursor <= cutoff:
        key = f"{cursor.year}-{cursor.month:02d}"
        if key not in processed:
            targets.append((cursor.year, cursor.month))
        cursor += relativedelta(months=1)

    return targets


def url_exists(year: int, month: int) -> bool:
    """HEAD request kiểm tra file parquet có tồn tại trên TLC không."""
    url = TLC_URL_TEMPLATE.format(year=year, month=month)
    try:
        resp = requests.head(url, timeout=15, allow_redirects=True)
        return resp.status_code == 200
    except requests.RequestException:
        return False


def run_cmd(cmd: list, description: str):
    """Chạy subprocess, raise nếu lỗi."""
    logging.info(f"  >> {description}")
    logging.info(f"     {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT_DIR))
    if result.returncode != 0:
        raise RuntimeError(f"Command failed (exit {result.returncode}): {description}")


def process_month(year: int, month: int):
    """Chạy full pipeline cho 1 tháng."""
    logging.info(f"Processing {year}-{month:02d} ...")

    # Step 0: Extract -> HDFS
    run_cmd(
        [sys.executable, str(ROOT_DIR / "scripts" / "extract_data.py"),
         "--year", str(year), "--month", str(month)],
        f"extract_data {year}-{month:02d}"
    )

    # Step 1: Spark processing
    run_cmd(
        ["docker", "exec", "spark-master",
         "/spark/bin/spark-submit", "/scripts/spark_process.py",
         "--year", str(year), "--month", str(month)],
        f"spark_process {year}-{month:02d}"
    )

    # Step 2: Fetch processed parquet to local
    run_cmd(
        [sys.executable, str(ROOT_DIR / "scripts" / "fetch_from_datalake.py"),
         "--year", str(year), "--month", str(month)],
        f"fetch_from_datalake {year}-{month:02d}"
    )


def run_dbt():
    """Chạy dbt seed + run + test."""
    logging.info("Running dbt (seed -> run -> test) ...")
    for cmd in [["dbt", "seed"], ["dbt", "run"], ["dbt", "test"]]:
        result = subprocess.run(cmd, cwd=str(DBT_DIR))
        if result.returncode != 0:
            raise RuntimeError(f"dbt step failed: {' '.join(cmd)}")
    logging.info("dbt completed successfully.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="NYC Taxi Incremental Pipeline Updater"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ in ra danh sách tháng sẽ xử lý, không tải thật"
    )
    parser.add_argument(
        "--force-month", type=str, metavar="YYYY-MM",
        help="Ép xử lý 1 tháng cụ thể (bỏ qua state), VD: 2024-03"
    )
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # ── File handler for log ──────────────────────────────────────────────────
    log_file = LOG_DIR / f"incremental_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logging.getLogger().addHandler(file_handler)

    state = load_state()

    # ── Force single month mode ───────────────────────────────────────────────
    if args.force_month:
        try:
            y, m = map(int, args.force_month.split("-"))
            targets = [(y, m)]
        except ValueError:
            logging.error("--force-month phải có định dạng YYYY-MM")
            sys.exit(1)
    else:
        targets = get_target_months(state["processed"])

    if not targets:
        logging.info("Không có tháng mới cần xử lý. Pipeline đã up-to-date.")
        return

    logging.info(f"Phát hiện {len(targets)} tháng chưa xử lý:")
    for y, m in targets:
        logging.info(f"  -> {y}-{m:02d}")

    if args.dry_run:
        logging.info("[DRY RUN] Kết thúc. Không có gì được tải.")
        return

    # ── Process each month ───────────────────────────────────────────────────
    new_months = []
    failed_months = []

    for year, month in targets:
        key = f"{year}-{month:02d}"

        # Kiểm tra URL trước khi tải
        logging.info(f"Kiểm tra URL: {key} ...")
        if not url_exists(year, month):
            logging.warning(f"  File chưa có trên TLC ({key}), bỏ qua.")
            continue

        try:
            process_month(year, month)
            state["processed"].append(key)
            new_months.append(key)
            save_state(state)   # Lưu ngay sau mỗi tháng thành công
            logging.info(f"  DONE: {key}")
        except Exception as e:
            logging.error(f"  FAILED: {key} -> {e}")
            failed_months.append(key)

    # ── dbt nếu có tháng mới ─────────────────────────────────────────────────
    if new_months:
        logging.info(f"\n{len(new_months)} tháng mới xử lý xong. Chạy dbt...")
        try:
            run_dbt()
        except Exception as e:
            logging.error(f"dbt failed: {e}")

    # ── Summary ──────────────────────────────────────────────────────────────
    logging.info("\n" + "=" * 56)
    logging.info(f" Incremental update complete")
    logging.info(f" New months processed : {new_months or 'none'}")
    logging.info(f" Failed               : {failed_months or 'none'}")
    logging.info(f" Total processed ever : {len(state['processed'])} months")
    logging.info(f" Log: {log_file}")
    logging.info("=" * 56)


if __name__ == "__main__":
    main()
