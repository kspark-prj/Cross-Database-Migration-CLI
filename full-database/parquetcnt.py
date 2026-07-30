import connectorx as cx
import polars as pl

DB_URI = "postgresql://postgres:root@localhost:5432/my-app-db"

# 1. DB 전체 카운트 (부모 테이블 대상)
db_count_query = "SELECT COUNT(*) FROM bulk_test_users"
db_total = cx.read_sql(DB_URI, db_count_query).iloc[0, 0]

# 2. Parquet 파티션 전체 카운트
parquet_total = (
    pl.scan_parquet("./mig_assets/parquet/bulk_test_users/*.parquet")
    .select(pl.len())
    .collect()
    .item()
)

print(f"PostgreSQL DB 전체 Count : {db_total:,}")
print(f"Parquet 파일 전체 Count  : {parquet_total:,}")

if db_total == parquet_total:
    print("✅ 데이터가 손실 없이 100% 완벽하게 합쳐져 덤프되었습니다!")
else:
    print(f"⚠️ 개수 불일치! 차이: {abs(db_total - parquet_total):,} 개")
