import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import orjson
import polars as pl
from sqlalchemy import create_engine, inspect, text


class DBExtractor:
    def __init__(self, db_config, output_dir, chunk_size=100000, delay_ms=10, max_workers=4, resume=False):
        self.config = db_config
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.delay_ms = delay_ms
        self.max_workers = max_workers
        self.resume = resume

        # Paths
        self.parquet_dir = os.path.join(output_dir, "parquet")
        self.metadata_path = os.path.join(output_dir, "metadata.json")
        self.schema_path = os.path.join(output_dir, "schema.sql")
        self.checksums_path = os.path.join(output_dir, "source_checksums.json")
        self.progress_path = os.path.join(output_dir, "migration_progress.json")

        os.makedirs(self.parquet_dir, exist_ok=True)

        self.progress = self._load_progress()
        self.checksums = self._load_checksums()
        self.engine = self._create_engine()

    @staticmethod
    def _json_default(obj):
        if hasattr(obj, "item"):
            return obj.item()
        if hasattr(obj, "tolist"):
            return obj.tolist()
        return str(obj)

    def _create_engine(self):
        url = self.config.get_sqlalchemy_url()
        if self.config.dialect in ("postgresql", "mysql"):
            engine = create_engine(url, isolation_level="REPEATABLE READ")
        else:
            engine = create_engine(url)
        return engine

    def _load_progress(self):
        if self.resume and os.path.exists(self.progress_path):
            try:
                with open(self.progress_path, "rb") as f:
                    data = orjson.loads(f.read())
                    if data.get("phase") == "extract":
                        return data.get("tables", {})
            except Exception:
                pass
        return {}

    def _save_progress(self):
        data = {"phase": "extract", "tables": self.progress}
        with open(self.progress_path, "wb") as f:
            f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2, default=self._json_default))

    def _load_checksums(self):
        if self.resume and os.path.exists(self.checksums_path):
            try:
                with open(self.checksums_path, "rb") as f:
                    return orjson.loads(f.read())
            except Exception:
                pass
        return {}

    def _save_checksums(self):
        with open(self.checksums_path, "wb") as f:
            f.write(orjson.dumps(self.checksums, option=orjson.OPT_INDENT_2, default=self._json_default))

    def extract_metadata(self):
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()

        metadata = {"__source_dialect__": self.config.dialect}
        schema_ddl_parts = []

        from rich.console import Console

        console = Console()

        for table in tables:
            columns = [
                {
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col.get("nullable", True),
                    "default": str(col["default"]) if col.get("default") is not None else None,
                    "auto_increment": col.get("autoincrement", False),
                }
                for col in inspector.get_columns(table)
            ]

            pk_info = inspector.get_pk_constraint(table)
            pk = pk_info.get("constrained_columns", [])
            pk_status = f"PK: {', '.join(pk)}" if pk else "No PK"
            console.print(f"  🔍 [bold cyan]DDL Metadata[/bold cyan] | Table: [yellow]{table:<30}[/yellow] ({len(columns)} cols, {pk_status})")

            indexes = [
                {
                    "name": idx["name"],
                    "columns": idx["column_names"],
                    "unique": idx.get("unique", False),
                }
                for idx in inspector.get_indexes(table)
            ]

            fks = [
                {
                    "name": fk["name"],
                    "constrained_columns": fk["constrained_columns"],
                    "referred_table": fk["referred_table"],
                    "referred_columns": fk["referred_columns"],
                }
                for fk in inspector.get_foreign_keys(table)
            ]

            metadata[table] = {
                "columns": columns,
                "primary_keys": pk,
                "indexes": indexes,
                "foreign_keys": fks,
            }

            col_lines = [f"  {c['name']} {c['type']}" for c in columns]
            if pk:
                col_lines.append(f"  PRIMARY KEY ({', '.join(pk)})")
            schema_ddl_parts.append(f"CREATE TABLE {table} (\n" + ",\n".join(col_lines) + "\n);")

        with open(self.metadata_path, "wb") as f:
            f.write(orjson.dumps(metadata, option=orjson.OPT_INDENT_2, default=self._json_default))

        with open(self.schema_path, "w", encoding="utf-8") as f:
            f.write("\n\n".join(schema_ddl_parts))

        return metadata

    def _compute_chunk_checksum(self, df: pl.DataFrame, pks, is_numeric: bool) -> int:
        if df.is_empty():
            return 0

        is_composite = isinstance(pks, (list, tuple)) and len(pks) > 1

        if is_numeric and not is_composite:
            pk_col = pks[0] if isinstance(pks, (list, tuple)) else pks
            if pk_col in df.columns:
                res_sum = df.select(pl.col(pk_col).cast(pl.Int64).sum())[0, 0]
                return int(res_sum) if res_sum is not None else 0
            return 0
        else:
            pk_cols = list(pks) if isinstance(pks, (list, tuple)) else [pks]
            valid_pks = [c for c in pk_cols if c in df.columns]
            if not valid_pks:
                return 0

            concat_expr = pl.concat_str([pl.col(c).cast(pl.Utf8).fill_null("") for c in valid_pks])
            res_sum = df.select(concat_expr.hash(seed=0).sum())[0, 0]
            return int(res_sum) if res_sum is not None else 0

    def _format_sql_value(self, val):
        if val is None:
            return "NULL"
        if isinstance(val, (int, float)):
            return str(val)
        escaped = str(val).replace("'", "''")
        return f"'{escaped}'"

    def _build_cursor_query(self, table, pks, last_pk, limit):
        dialect = self.config.dialect
        order_cols = ", ".join(f"`{c}`" if dialect == "mysql" else f'"{c}"' for c in pks)

        where_clause = ""
        if last_pk is not None:
            if len(pks) == 1:
                col = pks[0]
                col_escaped = f"`{col}`" if dialect == "mysql" else f'"{col}"'
                val_escaped = self._format_sql_value(last_pk)
                where_clause = f"WHERE {col_escaped} > {val_escaped}"
            else:
                cols_escaped = ", ".join(f"`{c}`" if dialect == "mysql" else f'"{c}"' for c in pks)
                vals_escaped = ", ".join(self._format_sql_value(v) for v in last_pk)
                where_clause = f"WHERE ({cols_escaped}) > ({vals_escaped})"

        limit_clause = f"FETCH FIRST {limit} ROWS ONLY" if dialect == "oracle" else f"LIMIT {limit}"
        return f"SELECT * FROM {table} {where_clause} ORDER BY {order_cols} {limit_clause}"

    def extract_table(self, table, meta, ui_progress_callback=None):
        pks = meta.get("primary_keys", [])
        columns = [c["name"] for c in meta.get("columns", [])]

        table_dir = os.path.join(self.parquet_dir, table)
        os.makedirs(table_dir, exist_ok=True)

        if table not in self.progress:
            self.progress[table] = {"status": "in_progress", "completed_chunks": []}
            self._save_progress()

        if table not in self.checksums:
            self.checksums[table] = []

        has_pk = len(pks) >= 1
        is_composite = len(pks) > 1

        if not has_pk:
            return self._extract_table_stream(table, columns, table_dir, ui_progress_callback)

        cx_url = None
        try:
            import connectorx as cx

            cx_url = self.config.get_connectorx_url()
        except Exception:
            cx_url = None

        chunk_idx = 0
        last_pk = None

        while True:
            query = self._build_cursor_query(table, pks, last_pk, self.chunk_size)
            part_file = os.path.join(table_dir, f"part-{chunk_idx:04d}.parquet")

            rows_fetched = 0
            df = None

            if cx_url:
                try:
                    df = cx.read_sql(cx_url, query, return_type="polars")
                    rows_fetched = df.height
                except Exception:
                    rows_fetched = 0

            if rows_fetched == 0:
                try:
                    with self.engine.connect() as conn:
                        res = conn.execute(text(query))
                        df = pl.DataFrame(res.fetchall(), schema=list(res.keys()), orient="row")
                    rows_fetched = df.height
                except Exception:
                    rows_fetched = 0

            if rows_fetched == 0 or df is None or df.is_empty():
                break

            df.write_parquet(part_file)
            file_size = os.path.getsize(part_file) if os.path.exists(part_file) else 0

            if is_composite:
                last_pk = df.select(pks).row(-1)
                pk_start = df.select(pks).row(0)
                pk_end = last_pk
            else:
                pk_col = pks[0]
                last_pk = df[pk_col][-1]
                pk_start = df[pk_col][0]
                pk_end = last_pk

            checksum_val = self._compute_chunk_checksum(df.select(pks), pks, is_numeric=False)

            chunk_info = {
                "chunk_index": chunk_idx,
                "pk_col": pks,
                "pk_start": pk_start,
                "pk_end": pk_end,
                "is_numeric": False,
                "row_count": rows_fetched,
                "file_size": file_size,
                "checksum": checksum_val,
            }

            self.checksums[table] = [c for c in self.checksums[table] if c["chunk_index"] != chunk_idx]
            self.checksums[table].append(chunk_info)
            self._save_checksums()

            self.progress[table]["completed_chunks"].append(chunk_idx)
            self._save_progress()

            if ui_progress_callback:
                ui_progress_callback(table, rows_fetched, file_size, chunk_idx=chunk_idx + 1, total_chunks=None)

            chunk_idx += 1
            if self.delay_ms > 0:
                time.sleep(self.delay_ms / 1000.0)

        self.progress[table]["status"] = "completed"
        self._save_progress()

        # 테이블 단위 추출 완결 즉시 finished=True 콜백 전송 (스피너 및 속도문구 즉각 제거)
        if ui_progress_callback:
            ui_progress_callback(table, finished=True)

        return chunk_idx

    def _extract_table_stream(self, table, columns, table_dir, ui_progress_callback=None):
        chunk_idx = 0
        if self.progress[table]["status"] == "completed":
            if ui_progress_callback:
                ui_progress_callback(table, 0, 0, skipped=True)
            return

        with self.engine.connect() as conn:
            query = text(f"SELECT * FROM {table}")
            res = conn.execution_options(stream_results=True).execute(query)

            while True:
                rows = res.fetchmany(self.chunk_size)
                if not rows:
                    break

                df = pl.DataFrame(rows, schema=columns, orient="row")
                part_file = os.path.join(table_dir, f"part-{chunk_idx:04d}.parquet")
                df.write_parquet(part_file)
                file_size = os.path.getsize(part_file) if os.path.exists(part_file) else 0

                chunk_info = {
                    "chunk_index": chunk_idx,
                    "pk_col": None,
                    "pk_start": None,
                    "pk_end": None,
                    "is_numeric": False,
                    "row_count": df.height,
                    "file_size": file_size,
                    "checksum": 0,
                }
                self.checksums[table].append(chunk_info)
                self._save_checksums()

                if ui_progress_callback:
                    ui_progress_callback(table, df.height, file_size, chunk_idx=chunk_idx + 1, total_chunks=None)

                chunk_idx += 1
                if self.delay_ms > 0:
                    time.sleep(self.delay_ms / 1000.0)

        self.progress[table]["status"] = "completed"
        self._save_progress()

        # 테이블 단위 추출 완결 즉시 finished=True 콜백 전송 (스피너 및 속도문구 즉각 제거)
        if ui_progress_callback:
            ui_progress_callback(table, finished=True)

    def check_engine_status(self) -> list:
        results = []
        try:
            import connectorx as cx

            cx_url = self.config.get_connectorx_url()
            results.append((True, f"ConnectorX Rust engine ready (Primary, {self.config.dialect})"))
        except Exception:
            results.append((False, "ConnectorX not available"))

        results.append((True, "SQLAlchemy DBAPI fallback ready (Tertiary)"))
        return results
