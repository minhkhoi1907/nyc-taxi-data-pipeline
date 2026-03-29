# Hoàn thiện dự án NYC Taxi Pipeline

Chào bạn, tôi thấy bạn đã setup xong hạ tầng HDFS (docker-compose), script tải dữ liệu thô (extract, bulk load), kéo dữ liệu từ HDFS về local ([fetch_from_datalake.py](file:///k:/DataEngineer/nyc_taxi_pipeline/scripts/fetch_from_datalake.py)), và khởi tạo dbt (duckdb) với model `stg_yellow_tripdata` xử lý Schema Evolution.

Để "hoàn thành dự án" (tạo thành một data pipeline hoàn chỉnh), tôi đề xuất thực hiện 3 bước tiếp theo như sau:

## Proposed Changes

### 1. Xây dựng dbt Core & Mart Models
Tạo các lớp dữ liệu phân tích chuẩn (Fact và Mart):
#### [NEW] `nyc_taxi_dbt/models/core/fact_trips.sql`
- Model tổng hợp và làm sạch lần cuối từ `stg_yellow_tripdata` (ví dụ: tính toán thêm `trip_duration`, chuẩn hóa các record bất thường nếu có).
#### [NEW] `nyc_taxi_dbt/models/marts/mart_monthly_revenue.sql`
- Model phân tích (Mart) map từ Fact: Tổng hợp doanh thu (`total_amount`, `fare_amount`, `tip_amount`) và số chuyến đi theo từng tháng và loại thanh toán.

### 2. Thêm Testing & Documentation cho dbt
Để đảm bảo chất lượng dữ liệu:
#### [NEW] `nyc_taxi_dbt/models/staging/schema.yml`
#### [NEW] `nyc_taxi_dbt/models/core/schema.yml`
#### [NEW] `nyc_taxi_dbt/models/marts/schema.yml`
- Thêm các test cơ bản (`not_null`, `unique` nếu có, `accepted_values`) và mô tả cho các cột.

### 3. Orchestration (Tự động hóa toàn bộ Flow)
#### [NEW] `scripts/run_pipeline.py`
- Một script Python đóng vai trò Master orchestrator để chạy tuần tự:
  1. Kéo dữ liệu từ HDFS về ([fetch_from_datalake.py](file:///k:/DataEngineer/nyc_taxi_pipeline/scripts/fetch_from_datalake.py))
  2. Chạy `dbt run`
  3. Chạy `dbt test`

## Verification Plan

### Automated Tests
- Chạy `dbt test` thông qua command line để xác minh các ràng buộc dữ liệu (data quality tests) được định nghĩa trong `schema.yml`.

### Manual Verification
- Chạy thử `python scripts/run_pipeline.py` để đảm bảo end-to-end pipeline hoạt động trơn tru.
- Query trực tiếp duckdb file ([dev.duckdb](file:///k:/DataEngineer/nyc_taxi_pipeline/dev.duckdb)) để kiểm tra dữ liệu của bảng `mart_monthly_revenue`.

**Vui lòng xem qua kế hoạch này.** Nếu bạn đồng ý, tôi sẽ tiến hành code luôn nhé!
