# config.py
import os

# Source & Target PostgreSQL 접속 URI
SOURCE_PG_URI = os.getenv("SOURCE_PG_URI", "postgresql://postgres:root@127.0.0.1:5432/my-app-db")
TARGET_PG_URI = os.getenv("TARGET_PG_URI", "postgresql://postgres:root@127.0.0.1:5432/test_db")

# 임시 Parquet 청크 저장 경로
PARQUET_DIR = "./parquet_migration_temp"

# 마이그레이션 대상 테이블 목록
TARGET_TABLES = ["batch_checkpoints", "bulk_test_users"]

# 테이블별 PK 설정 (단일 문자열 또는 복합 PK 리스트)
PK_SETTING = {
    "batch_checkpoints": "job_name",  # 단일 PK
    "bulk_test_users": "id",  # 복합 PK 2개
    # "logs": ["tenant_id", "log_date", "log_id"],  # 복합 PK 3개
}

# 청크당 행(Row) 수
CHUNK_SIZE = 100_000
