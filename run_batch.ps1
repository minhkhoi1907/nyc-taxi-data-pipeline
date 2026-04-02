# ============================================================
# NYC Taxi Pipeline - Batch Runner 2022-2023
# Strategy:
#   - Mỗi tháng: extract_data -> spark_process -> fetch_from_datalake
#   - Cuối batch: dbt seed + run + test (1 lần duy nhất)
#   - Sau dbt: Streamlit dashboard
# ============================================================

$ErrorActionPreference = "Stop"
$ROOT = "K:\DataEngineer\nyc_taxi_pipeline"
$LOG_DIR = "$ROOT\logs"
$LOG_FILE = "$LOG_DIR\batch_run_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "$timestamp [$Level] $Message"
    Write-Host $line
    Add-Content -Path $LOG_FILE -Value $line
}

# ---- Config ----
$YEARS  = 2022, 2023
$MONTHS = 1..12
$DBT_DIR = "$ROOT\nyc_taxi_dbt"

$total   = $YEARS.Count * $MONTHS.Count
$current = 0
$failed  = @()

Write-Log "=========================================================="
Write-Log " NYC Taxi Pipeline - Batch Run 2022-2023"
Write-Log " Total months: $total"
Write-Log " Log: $LOG_FILE"
Write-Log "=========================================================="

# ============================================================
# PHASE 1: Per-month data processing
# ============================================================
Write-Log "PHASE 1: Data ingestion & Spark processing"

foreach ($year in $YEARS) {
    foreach ($month in $MONTHS) {
        $current++
        $monthStr = $month.ToString("D2")
        Write-Log "--- [$current/$total] $year-$monthStr ---"

        try {
            # Step 0: Extract to HDFS
            Write-Log "Step 0 [$year-$monthStr]: Extract -> HDFS"
            & python "$ROOT\scripts\extract_data.py" --year $year --month $month
            if ($LASTEXITCODE -ne 0) { throw "extract_data.py failed (exit $LASTEXITCODE)" }

            # Step 1: Spark job
            Write-Log "Step 1 [$year-$monthStr]: Spark processing"
            & docker exec spark-master /spark/bin/spark-submit /scripts/spark_process.py --year $year --month $month
            if ($LASTEXITCODE -ne 0) { throw "spark_process.py failed (exit $LASTEXITCODE)" }

            # Step 2: Fetch processed data to local
            Write-Log "Step 2 [$year-$monthStr]: Fetch processed parquet locally"
            & python "$ROOT\scripts\fetch_from_datalake.py" --year $year --month $month
            if ($LASTEXITCODE -ne 0) { throw "fetch_from_datalake.py failed (exit $LASTEXITCODE)" }

            Write-Log "[$current/$total] DONE: $year-$monthStr" "INFO"
        }
        catch {
            Write-Log "[$current/$total] FAILED: $year-$monthStr | $_" "ERROR"
            $failed += "$year-$monthStr"
        }
    }
}

# ============================================================
# PHASE 2: Lookup tables (1 lần)
# ============================================================
Write-Log ""
Write-Log "PHASE 2: Download taxi zone lookup tables"
try {
    & python "$ROOT\scripts\download_taxi_zones.py"
} catch {
    Write-Log "Taxi zones download failed (non-fatal): $_" "WARNING"
}

# ============================================================
# PHASE 3: dbt (1 lần cho toàn bộ data)
# ============================================================
Write-Log ""
Write-Log "PHASE 3: dbt seed -> run -> test"
try {
    Push-Location $DBT_DIR
    Write-Log "Running: dbt seed"
    & dbt seed
    if ($LASTEXITCODE -ne 0) { throw "dbt seed failed" }

    Write-Log "Running: dbt run"
    & dbt run
    if ($LASTEXITCODE -ne 0) { throw "dbt run failed" }

    Write-Log "Running: dbt test"
    & dbt test
    if ($LASTEXITCODE -ne 0) { throw "dbt test failed" }

    Pop-Location
    Write-Log "dbt completed." "INFO"
} catch {
    Write-Log "dbt failed: $_" "ERROR"
    $failed += "dbt"
    Pop-Location -ErrorAction SilentlyContinue
}

# ============================================================
# PHASE 4: Streamlit Dashboard
# ============================================================
$DASHBOARD = "$ROOT\app\Home.py"
if (Test-Path $DASHBOARD) {
    Write-Log ""
    Write-Log "PHASE 4: Launching Streamlit at http://localhost:8501"
    Start-Process powershell -ArgumentList "-NoProfile -Command streamlit run `"$DASHBOARD`"" -WindowStyle Minimized
    Write-Log "Dashboard started -> http://localhost:8501" "INFO"
}

# ============================================================
# Summary
# ============================================================
$monthFailed = $failed | Where-Object { $_ -ne "dbt" }
Write-Log ""
Write-Log "=========================================================="
Write-Log " BATCH COMPLETE"
Write-Log " Success: $($total - $monthFailed.Count) / $total months"
if ($failed.Count -gt 0) {
    Write-Log " Failed: $($failed -join ', ')" "ERROR"
}
Write-Log " Log: $LOG_FILE"
Write-Log "=========================================================="
