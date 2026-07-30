# exporter.py
import os
from typing import Dict, List, Tuple, Union

import connectorx as cx
import polars as pl


def _normalize_pk_mapping(tables: list[str], pk_input: str | list[str] | dict[str, str | list[str]] | None) -> dict[str, list[str]]:
    """PK 설정을 {테이블명: [PK컬럼1, PK컬럼2...]} 형태로 규격화합니다."""
    default_pk = ["id"]
    if pk_input is None:
        return {table: default_pk for table in tables}
    if isinstance(pk_input, str):
        return {table: [pk_input] for table in tables}
    if isinstance(pk_input, (list, tuple)):
        if all(isinstance(x, str) for x in pk_input):
            return {table: list(pk_input) for table in tables}
    if isinstance(pk_input, dict):
        result = {}
        for table in tables:
            val = pk_input.get(table, default_pk)
            if isinstance(val, str):
                result[table] = [val]
            elif isinstance(val, (list, tuple)):
                result[table] = list(val)
            else:
                result[table] = default_pk
        return result
    return {table: default_pk for table in tables}


def export_pg_tables_to_parquet(
    conn_str: str,
    tables: list[str],
    pk: str | list[str] | dict[str, str | list[str]] | None = "id",
    output_dir: str = "./parquet_data",
    chunk_size: int = 100_000,
) -> None:
    """PostgreSQL 테이블들을 지정된 PK 기반 커서로 분할 추출하여 Parquet 청크로 저장합니다."""
    os.makedirs(output_dir, exist_ok=True)
    pk_map = _normalize_pk_mapping(tables, pk)

    for table in tables:
        pk_cols = pk_map[table]
        pk_cols_str = ", ".join(pk_cols)
        order_by_clause = ", ".join([f"{col} ASC" for col in pk_cols])

        print(f"\n[Export] '{table}' 추출 시작 (PK: [{pk_cols_str}])...")

        last_values = None
        chunk_idx = 0
        total_exported_rows = 0

        while True:
            if last_values is None:
                query = f"SELECT * FROM {table} ORDER BY {order_by_clause} LIMIT {chunk_size}"
            else:
                formatted_vals = []
                for v in last_values:
                    if isinstance(v, str):
                        escaped_v = v.replace("'", "''")
                        formatted_vals.append(f"'{escaped_v}'")
                    elif v is None:
                        formatted_vals.append("NULL")
                    else:
                        formatted_vals.append(str(v))

                if len(pk_cols) == 1:
                    where_clause = f"{pk_cols[0]} > {formatted_vals[0]}"
                else:
                    where_clause = f"({pk_cols_str}) > ({', '.join(formatted_vals)})"

                query = f"SELECT * FROM {table} WHERE {where_clause} ORDER BY {order_by_clause} LIMIT {chunk_size}"

            chunk_df = cx.read_sql(conn_str, query, return_type="polars")
            rows_count = len(chunk_df)

            if rows_count == 0:
                break

            last_row_dict = chunk_df.select(pk_cols).row(-1, named=True)  # type:ignore
            last_values = [last_row_dict[col] for col in pk_cols]

            file_name = f"{table}_chunk_{chunk_idx}.parquet"
            file_path = os.path.join(output_dir, file_name)
            chunk_df.write_parquet(file_path, compression="zstd")  # type:ignore

            total_exported_rows += rows_count
            print(f"  └─ Chunk {chunk_idx} 추출 완료 ({rows_count:,} rows) -> {file_name}")

            chunk_idx += 1
            if rows_count < chunk_size:
                break

        print(f"  └─ '{table}' 추출 완료: 총 {total_exported_rows:,} 행 -> {chunk_idx}개 파일 생성")
