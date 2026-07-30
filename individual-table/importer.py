# importer.py
import glob
import os

import duckdb


def import_parquet_to_pg_via_duckdb(
    target_conn_str: str,
    tables: list[str],
    input_dir: str = "./parquet_migration_temp",
    overwrite: bool = True,
) -> None:
    """DuckDB의 postgres 확장을 활용해 Parquet 파일들을 Target PostgreSQL로 적재합니다."""
    print("\n=== [Import] DuckDB Engine -> Target PostgreSQL 적재 시작 ===")

    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL postgres; LOAD postgres;")

    print("  └─ Target PostgreSQL에 DuckDB 엔진 연결 중...")
    con.execute(f"ATTACH '{target_conn_str}' AS target_db (TYPE postgres);")

    for table in tables:
        pattern = os.path.join(input_dir, f"{table}_chunk_*.parquet")
        chunk_files = sorted(
            glob.glob(pattern),
            key=lambda x: int(x.split("_chunk_")[-1].replace(".parquet", "") if "_chunk_" in x else 0),
        )

        if not chunk_files:
            print(f"  └─ WARNING: '{table}' 청크 파일을 찾을 수 없습니다.")
            continue

        print(f"\n[Import] '{table}' 적재 시작 (총 {len(chunk_files)}개 청크)...")

        if overwrite:
            con.execute(f"DROP TABLE IF EXISTS target_db.{table};")

        # 첫 번째 청크 파일 스키마 기반 테이블 자동 생성
        first_chunk = chunk_files[0].replace("\\", "/")
        con.execute(f"CREATE TABLE target_db.{table} AS SELECT * FROM read_parquet('{first_chunk}') LIMIT 0;")

        # 전체 청크 파일 다이렉트 Stream COPY 적재
        parquet_glob_pattern = os.path.join(input_dir, f"{table}_chunk_*.parquet").replace("\\", "/")
        print(f"  └─ DuckDB COPY 스트리밍 중: {parquet_glob_pattern}")

        con.execute(f"COPY target_db.{table} FROM '{parquet_glob_pattern}';")

        count = con.execute(f"SELECT COUNT(*) FROM target_db.{table};").fetchone()[0]  # type:ignore
        print(f"  └─ '{table}' PostgreSQL 적재 완료! (총 {count:,} 행)")

    con.execute("DETACH target_db;")
    con.close()
