# main.py
import time

from config import (
    CHUNK_SIZE,
    PARQUET_DIR,
    PK_SETTING,
    SOURCE_PG_URI,
    TARGET_PG_URI,
    TARGET_TABLES,
)
from exporter import export_pg_tables_to_parquet
from importer import import_parquet_to_pg_via_duckdb


def run_pipeline():
    start_time = time.time()
    print("==================================================")
    print(" PostgreSQL -> DuckDB Engine -> PostgreSQL 마이그레이션")
    print("==================================================")

    try:
        # 1. Source PG -> Parquet 추출
        # export_pg_tables_to_parquet(
        #     conn_str=SOURCE_PG_URI,
        #     tables=TARGET_TABLES,
        #     pk=PK_SETTING,
        #     output_dir=PARQUET_DIR,
        #     chunk_size=CHUNK_SIZE,
        # )

        # 2. Parquet -> Target PG 적재
        import_parquet_to_pg_via_duckdb(
            target_conn_str=TARGET_PG_URI,
            tables=TARGET_TABLES,
            input_dir=PARQUET_DIR,
            overwrite=True,
        )

        elapsed = time.time() - start_time
        print("\n==================================================")
        print(f"🎉 모든 마이그레이션 작업 완료! (소요 시간: {elapsed:.2f}초)")
        print("==================================================")

    except Exception as e:
        print(f"\n❌ 마이그레이션 도중 에러 발생: {e}")


if __name__ == "__main__":
    run_pipeline()
