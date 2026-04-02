# ============================================================
# NYC Taxi Pipeline - Incremental Updater Runner
# Gọi incremental_update.py để tự động xử lý tháng mới
#
# Dùng thủ công:
#   .\run_update.ps1              # chạy bình thường
#   .\run_update.ps1 -DryRun      # xem trước, không tải
#   .\run_update.ps1 -ForceMonth 2024-03   # ép 1 tháng cụ thể
#
# Schedule với Windows Task Scheduler:
#   Action: powershell -ExecutionPolicy Bypass -File "K:\...\run_update.ps1"
# ============================================================

param(
    [switch]$DryRun,
    [string]$ForceMonth = ""
)

$ROOT   = "K:\DataEngineer\nyc_taxi_pipeline"
$SCRIPT = "$ROOT\scripts\incremental_update.py"

$args_list = @()
if ($DryRun)                  { $args_list += "--dry-run" }
if ($ForceMonth -ne "")       { $args_list += "--force-month"; $args_list += $ForceMonth }

Write-Host ""
Write-Host "========================================"
Write-Host " NYC Taxi Incremental Updater"
Write-Host " $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
if ($DryRun)       { Write-Host " Mode: DRY RUN" }
if ($ForceMonth)   { Write-Host " Force month: $ForceMonth" }
Write-Host "========================================"

& python $SCRIPT @args_list

Write-Host ""
Write-Host "Xem log chi tiết tại: $ROOT\logs\"
