import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import orjson
import polars as pl
from sqlalchemy import create_engine, event, inspect, text


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
            pk_col = pks[0] if isinstance(pks, (list, tuple)) else pks  # type:ignore
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

    def _get_pk_ranges(self, table, pk_col):
        with self.engine.connect() as conn:
            res = conn.execute(text(f"SELECT MIN({pk_col}), MAX({pk_col}), COUNT(*) FROM {table}")).fetchone()
            min_val, max_val, total_rows = res[0], res[1], res[2]  # type:ignore

        if total_rows == 0 or min_val is None or max_val is None:
            return [], total_rows

        if total_rows <= self.chunk_size:
            return [(min_val, max_val, True)], total_rows

        if isinstance(min_val, (int, float)):
            ranges = []
            curr = min_val
            span = self.chunk_size
            while curr <= max_val:
                nxt = curr + span
                is_last = nxt > max_val
                ranges.append((curr, nxt, is_last))
                curr = nxt
            return ranges, total_rows

        return [], total_rows

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

        ranges = []
        total_rows = 0
        is_splittable = False
        is_numeric = False

        if has_pk and not is_composite:
            try:
                ranges, total_rows = self._get_pk_ranges(table, pks[0])
                is_splittable = len(ranges) > 0
                if is_splittable:
                    is_numeric = True
            except Exception:
                is_splittable = False

        if has_pk and not is_splittable:
            try:
                with self.engine.connect() as conn:
                    res = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                    total_rows = res[0] if res else 0

                if total_rows > 0:
                    total_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size
                    for idx in range(total_chunks):
                        is_last = idx == total_chunks - 1
                        ranges.append((idx, is_last))
                    is_splittable = True
            except Exception:
                is_splittable = False

        if not is_splittable:
            self._extract_table_stream(table, columns, table_dir, ui_progress_callback)
            return

        cx_url = None
        try:
            import connectorx as cx

            cx_url = self.config.get_connectorx_url()
        except Exception:
            cx_url = None

        duckdb_con = None
        if not cx_url and self.config.dialect in ("postgresql", "mysql"):
            try:
                import duckdb

                duckdb_con = duckdb.connect()
                dsn = self.config.get_duckdb_dsn()
                if self.config.dialect == "postgresql":
                    duckdb_con.execute("INSTALL postgres; LOAD postgres;")
                    duckdb_con.execute(f"ATTACH '{dsn}' AS src (TYPE postgres);")
                elif self.config.dialect == "mysql":
                    duckdb_con.execute("INSTALL mysql; LOAD mysql;")
                    duckdb_con.execute(f"ATTACH '{dsn}' AS src (TYPE mysql);")
            except Exception:
                duckdb_con = None

        for idx, range_info in enumerate(ranges):
            if idx in self.progress[table]["completed_chunks"]:
                if ui_progress_callback:
                    ui_progress_callback(table, self.chunk_size, 0, skipped=True)
                continue

            if is_numeric:
                start, end, is_last = range_info
                pk_col = pks[0]
                if is_last:
                    query = f"SELECT * FROM {table} WHERE {pk_col} >= {start} AND {pk_col} <= {end}"
                else:
                    query = f"SELECT * FROM {table} WHERE {pk_col} >= {start} AND {pk_col} < {end}"
            else:
                is_last = range_info[1]
                pk_col = pks if is_composite else pks[0]
                last_pk = None
                if idx > 0:
                    for c in self.checksums.get(table, []):
                        if c.get("chunk_index") == idx - 1:
                            last_pk = c.get("pk_end")
                            break
                query = self._build_cursor_query(table, pks, last_pk, self.chunk_size)

            part_file = os.path.join(table_dir, f"part-{idx:04d}.parquet")
            rows_fetched = 0
            checksum_val = 0
            pk_start = None
            pk_end = None

            if cx_url:
                try:
                    df = cx.read_sql(cx_url, query, return_type="polars")
                    rows_fetched = df.height
                    if rows_fetched > 0:  # type:ignore
                        df.write_parquet(part_file)  # type:ignore
                        if is_numeric:
                            pk_series = df[pk_col]
                            pk_start = pk_series[0]
                            pk_end = pk_series[-1]
                            checksum_val = self._compute_chunk_checksum(
                                df.select([pk_col]),  # type:ignore
                                pk_col,
                                is_numeric=True,  # type:ignore
                            )
                        else:
                            if is_composite:
                                pk_start = df.select(pks).row(0)  # type:ignore
                                pk_end = df.select(pks).row(-1)  # type:ignore
                                checksum_val = self._compute_chunk_checksum(df.select(pks), pks, is_numeric=False)  # type:ignore
                            else:
                                pk_series = df[pk_col]
                                pk_start = pk_series[0]
                                pk_end = pk_series[-1]
                                checksum_val = self._compute_chunk_checksum(df.select([pk_col]), pk_col, is_numeric=False)  # type:ignore
                except Exception:
                    rows_fetched = 0

            if rows_fetched == 0 and duckdb_con:  # type:ignore
                success, rows_fetched, checksum_val, pk_start, pk_end = self._fetch_query_duckdb_or_fallback(
                    query, table, part_file, pks, is_composite, is_numeric, duckdb_con
                )

            if rows_fetched == 0:  # type:ignore
                try:
                    with self.engine.connect() as conn:
                        res = conn.execute(text(query))
                        df = pl.DataFrame(res.fetchall(), schema=list(res.keys()), orient="row")
                    rows_fetched = df.height
                    if rows_fetched > 0:
                        df.write_parquet(part_file)
                        if is_numeric:
                            pk_start = start
                            pk_end = end
                            checksum_val = self._compute_chunk_checksum(df, pk_col, is_numeric=True)
                        else:
                            if is_composite:
                                pk_start = df.select(pks).row(0)
                                pk_end = df.select(pks).row(-1)
                            else:
                                pk_start = df[pk_col][0]
                                pk_end = df[pk_col][-1]
                            checksum_val = self._compute_chunk_checksum(df, pk_col, is_numeric=False)
                except Exception:
                    rows_fetched = 0

            # 파일 크기 계산
            file_size = os.path.getsize(part_file) if os.path.exists(part_file) else 0

            if rows_fetched > 0:  # type:ignore
                chunk_info = {
                    "chunk_index": idx,
                    "pk_col": pk_col,
                    "pk_start": pk_start,
                    "pk_end": pk_end,
                    "is_numeric": is_numeric,
                    "row_count": rows_fetched,
                    "file_size": file_size,  # <-- 파일 사이즈(Bytes) 기록
                    "checksum": checksum_val,
                }
                self.checksums[table] = [c for c in self.checksums[table] if c["chunk_index"] != idx]
                self.checksums[table].append(chunk_info)
                self._save_checksums()

            self.progress[table]["completed_chunks"].append(idx)
            self._save_progress()

            if ui_progress_callback:
                ui_progress_callback(table, rows_fetched, file_size)

            if self.delay_ms > 0:
                time.sleep(self.delay_ms / 1000.0)

        self.progress[table]["status"] = "completed"
        self._save_progress()

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

                if chunk_idx in self.progress[table]["completed_chunks"]:
                    chunk_idx += 1
                    continue

                df = pl.DataFrame(rows, schema=columns, orient="row")
                part_file = os.path.join(table_dir, f"part-{chunk_idx:04d}.parquet")

                df.write_parquet(part_file)
                file_size = os.path.getsize(part_file) if os.path.exists(part_file) else 0

                checksum_val = 0
                pk_col = columns[0] if columns else None
                if pk_col:
                    checksum_val = self._compute_chunk_checksum(df, pk_col, is_numeric=False)

                chunk_info = {
                    "chunk_index": chunk_idx,
                    "pk_col": pk_col,
                    "pk_start": None,
                    "pk_end": None,
                    "is_numeric": False,
                    "row_count": df.height,
                    "file_size": file_size,  # <-- 파일 사이즈 기록
                    "checksum": checksum_val,
                }

                self.checksums[table] = [c for c in self.checksums[table] if c["chunk_index"] != chunk_idx]
                self.checksums[table].append(chunk_info)
                self._save_checksums()

                self.progress[table]["completed_chunks"].append(chunk_idx)
                self._save_progress()

                if ui_progress_callback:
                    ui_progress_callback(table, df.height, file_size)

                chunk_idx += 1

                if self.delay_ms > 0:
                    time.sleep(self.delay_ms / 1000.0)

        self.progress[table]["status"] = "completed"
        self._save_progress()

    def _fetch_query_duckdb_or_fallback(self, query, table, part_file, pks, is_composite, is_numeric, duckdb_con):
        if not duckdb_con:
            return False, 0, 0, None, None

        try:
            query_prefixed = query.replace(f"FROM {table}", f"FROM src.{table}")
            part_file_normalized = part_file.replace("\\", "/")
            duckdb_con.execute(f"COPY ({query_prefixed}) TO '{part_file_normalized}' (FORMAT 'parquet');")

            if is_numeric and not is_composite:
                pk_col = pks[0]
                checksum_expr = f'CAST(SUM(CAST("{pk_col}" AS BIGINT)) AS BIGINT)'
            else:
                if is_composite:
                    concat_expr = " || ".join(f"COALESCE(CAST(\"{col}\" AS VARCHAR), '')" for col in pks)
                else:
                    concat_expr = f"COALESCE(CAST(\"{pks[0]}\" AS VARCHAR), '')"
                checksum_expr = f"CAST(SUM(crc32({concat_expr})) AS BIGINT)"

            res = duckdb_con.execute(f"SELECT COUNT(*), COALESCE({checksum_expr}, 0) FROM read_parquet('{part_file_normalized}')").fetchone()
            row_count, checksum = int(res[0]), int(res[1])

            if row_count > 0:
                if is_composite:
                    cols_str = ", ".join(f'"{c}"' for c in pks)
                    start_res = duckdb_con.execute(f"SELECT {cols_str} FROM read_parquet('{part_file_normalized}') LIMIT 1").fetchone()
                    pk_start = list(start_res) if start_res else None

                    end_res = duckdb_con.execute(
                        f"SELECT {cols_str} FROM read_parquet('{part_file_normalized}') OFFSET {row_count - 1} LIMIT 1"
                    ).fetchone()
                    pk_end = list(end_res) if end_res else None
                else:
                    pk_col = pks[0]
                    start_res = duckdb_con.execute(f"SELECT \"{pk_col}\" FROM read_parquet('{part_file_normalized}') LIMIT 1").fetchone()
                    pk_start = start_res[0] if start_res else None

                    end_res = duckdb_con.execute(
                        f"SELECT \"{pk_col}\" FROM read_parquet('{part_file_normalized}') OFFSET {row_count - 1} LIMIT 1"
                    ).fetchone()
                    pk_end = end_res[0] if end_res else None
            else:
                pk_start, pk_end = None, None

            return True, row_count, checksum, pk_start, pk_end
        except Exception:
            return False, 0, 0, None, None

    def check_engine_status(self) -> list:
        results = []
        try:
            import connectorx as cx

            cx_url = self.config.get_connectorx_url()
            results.append((True, f"ConnectorX Rust engine ready (Primary, {self.config.dialect})"))
        except Exception:
            results.append((False, "ConnectorX not available"))

        if self.config.dialect in ("postgresql", "mysql"):
            try:
                import duckdb

                con = duckdb.connect()
                if self.config.dialect == "postgresql":
                    con.execute("INSTALL postgres; LOAD postgres;")
                elif self.config.dialect == "mysql":
                    con.execute("INSTALL mysql; LOAD mysql;")
                results.append((True, f"DuckDB C++ engine ready (Secondary, {self.config.dialect} extension)"))
            except Exception:
                results.append((False, "DuckDB extension not available"))
        else:
            results.append((False, f"DuckDB: no native {self.config.dialect} extension"))

        results.append((True, "SQLAlchemy DBAPI fallback ready (Tertiary)"))
        return results
