import glob
import os

import duckdb
from sqlalchemy import create_engine, text


class DBValidator:
    def __init__(self, target_config=None, data_dir="./mig_assets", db_config=None, source_config=None):
        # target_config 및 db_config 하위 호환성 처리
        self.config = target_config or db_config
        self.source_config = source_config
        self.data_dir = data_dir
        self.parquet_dir = os.path.join(data_dir, "parquet")

        if self.config:
            self.engine = create_engine(self.config.get_sqlalchemy_url())
        else:
            self.engine = None

    def validate_table(self, table_name):
        table_parquet_dir = os.path.join(self.parquet_dir, table_name)
        if not os.path.exists(table_parquet_dir):
            return {"table": table_name, "status": "failed", "reason": "Parquet folder missing"}

        parquet_files = glob.glob(os.path.join(table_parquet_dir, "*.parquet"))
        if not parquet_files:
            return {"table": table_name, "status": "passed", "source_rows": 0, "target_rows": 0}

        # 1. Parquet 원본 개수 조회 via DuckDB
        con = duckdb.connect(database=":memory:")
        try:
            parquet_pattern = os.path.join(table_parquet_dir, "*.parquet").replace("\\", "/")
            src_count = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_pattern}')").fetchone()[0]
        finally:
            con.close()

        # 2. Target DB 건수 조회
        if not self.engine:
            return {"table": table_name, "status": "failed", "reason": "Target DB connection not initialized"}

        try:
            with self.engine.connect() as conn:
                res = conn.execute(text(f"SELECT count(*) FROM {table_name}")).fetchone()
                tgt_count = res[0] if res else 0
        except Exception as e:
            return {"table": table_name, "status": "failed", "reason": str(e)}

        is_valid = src_count == tgt_count
        return {
            "table": table_name,
            "status": "passed" if is_valid else "mismatch",
            "source_rows": src_count,
            "target_rows": tgt_count,
        }

    def validate_all(self):
        """전체 검증 실행"""
        if not os.path.exists(self.parquet_dir):
            return []

        tables = [d for d in os.listdir(self.parquet_dir) if os.path.isdir(os.path.join(self.parquet_dir, d)) and not d.startswith("__")]

        results = []
        for table_name in sorted(tables):
            results.append(self.validate_table(table_name))
        return results

    def validate(self):
        """main.py 호환용 래퍼 메서드"""
        return self.validate_all()
