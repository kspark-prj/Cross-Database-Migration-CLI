import glob
import os
import re
import traceback

import orjson
import polars as pl
from rich.console import Console
from sqlalchemy import create_engine, inspect, text


class DBLoader:
    def __init__(self, db_config, data_dir, max_workers=4, resume=False, unlogged=True):
        self.config = db_config
        self.data_dir = data_dir
        self.max_workers = max_workers
        self.resume = resume
        self.unlogged = unlogged
        self.console = Console()

        # Paths
        self.parquet_dir = os.path.join(data_dir, "parquet")
        self.metadata_path = os.path.join(data_dir, "metadata.json")
        self.schema_path = os.path.join(data_dir, "schema.sql")
        self.progress_path = os.path.join(data_dir, "migration_progress.json")

        self.metadata = self._load_metadata()
        self.progress = self._load_progress()
        self.engine = create_engine(self.config.get_sqlalchemy_url())

    def _load_metadata(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "rb") as f:
                return orjson.loads(f.read())
        return {}

    def _load_progress(self):
        if self.resume and os.path.exists(self.progress_path):
            try:
                with open(self.progress_path, "rb") as f:
                    data = orjson.loads(f.read())
                    if data.get("phase") == "load":
                        return data.get("tables", {})
            except Exception:
                pass
        return {}

    def _save_progress(self):
        data = {"phase": "load", "tables": self.progress}
        with open(self.progress_path, "wb") as f:
            f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))

    def check_duckdb_support(self):
        try:
            import duckdb

            con = duckdb.connect()
            dialect = self.config.dialect
            if dialect == "postgresql":
                con.execute("INSTALL postgres; LOAD postgres;")
            elif dialect == "mysql":
                con.execute("INSTALL mysql; LOAD mysql;")
            return True, f"DuckDB native support available for {dialect}"
        except Exception as e:
            return False, f"DuckDB fallback to DBAPI (Reason: {e})"

    def prepare_schema(self, source_dialect: str):
        """기본 스키마 DDL 생성 및 기존 데이터 초기화를 진행하며 생성 목록을 트리 형태로 출력합니다."""
        inspector = inspect(self.engine)
        existing_tables = inspector.get_table_names()

        tables = [t for t in self.metadata.keys() if not t.startswith("__")]

        with self.engine.begin() as conn:
            for table in tables:
                t_meta = self.metadata[table]
                cols = t_meta.get("columns", [])

                self.console.print(f" 📦 [bold cyan]Table: {table}[/bold cyan]")
                tree_items = []

                if table in existing_tables:
                    # 재실행 시 중복 적재 방지를 위해 TRUNCATE 수행
                    if not self.resume:
                        try:
                            conn.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
                            tree_items.append(
                                "🧹 [dim]Existing table found: Truncated data (CASCADE)[/dim]"
                            )
                        except Exception:
                            conn.execute(text(f"DELETE FROM {table};"))
                            tree_items.append("🧹 [dim]Existing table found: Deleted data[/dim]")
                    else:
                        tree_items.append(
                            "ℹ️  [dim]Existing table found: Resume mode (Skipped truncate)[/dim]"
                        )
                else:
                    # 테이블 생성 DDL
                    col_defs = []
                    for c in cols:
                        col_name = c["name"]
                        col_type = self._map_data_type(
                            c["type"], source_dialect, self.config.dialect
                        )
                        nullable = "" if c.get("nullable", True) else " NOT NULL"
                        col_defs.append(f'  "{col_name}" {col_type}{nullable}')

                    unlogged_str = ""
                    unlogged_label = ""
                    if self.unlogged and self.config.dialect == "postgresql":
                        unlogged_str = "UNLOGGED "
                        unlogged_label = " [bold yellow](UNLOGGED)[/bold yellow]"

                    create_sql = (
                        f"CREATE {unlogged_str}TABLE {table} (\n" + ",\n".join(col_defs) + "\n);"
                    )
                    conn.execute(text(create_sql))
                    tree_items.append(
                        f"🏗️  [bold green]Created Table[/bold green]{unlogged_label} ({len(cols)} columns)"
                    )

                # 트리 형태로 출력
                total_items = len(tree_items)
                for idx, item in enumerate(tree_items):
                    is_last = idx == total_items - 1
                    branch = "   └── " if is_last else "   ├── "
                    self.console.print(f"{branch}{item}")

                self.console.print()

    def _map_data_type(self, src_type: str, src_dialect: str, tgt_dialect: str) -> str:
        src_type_upper = src_type.upper()

        if tgt_dialect == "postgresql":
            if "INT" in src_type_upper and "BIGINT" not in src_type_upper:
                return "INTEGER"
            if "DATETIME" in src_type_upper or "TIMESTAMP" in src_type_upper:
                return "TIMESTAMP"
            if "VARCHAR" in src_type_upper or "TEXT" in src_type_upper:
                return src_type
            if "BLOB" in src_type_upper:
                return "BYTEA"

        elif tgt_dialect == "mysql":
            if "BYTEA" in src_type_upper:
                return "LONGBLOB"
            if "TIMESTAMP" in src_type_upper:
                return "DATETIME"

        return src_type

    def load_table_data(self, table: str, ui_progress_callback=None):
        if table in self.progress and self.progress[table].get("status") == "completed":
            if ui_progress_callback:
                ui_progress_callback(table, 0, 0, skipped=True)
            return

        table_dir = os.path.join(self.parquet_dir, table)
        if not os.path.exists(table_dir):
            return

        parquet_files = sorted(glob.glob(os.path.join(table_dir, "part-*.parquet")))
        if not parquet_files:
            return

        if table not in self.progress:
            self.progress[table] = {"status": "in_progress", "completed_files": []}

        for p_file in parquet_files:
            file_name = os.path.basename(p_file)
            if file_name in self.progress[table]["completed_files"]:
                continue

            df = pl.read_parquet(p_file)
            row_count = df.height
            file_size = os.path.getsize(p_file)

            # 💡 p_file 경로를 직접 넘겨 단일 파일만 적재
            self._bulk_insert_df(table, df, p_file)

            self.progress[table]["completed_files"].append(file_name)
            self._save_progress()

            if ui_progress_callback:
                ui_progress_callback(table, row_count, file_size)

        self.progress[table]["status"] = "completed"
        self._save_progress()

    def _bulk_insert_df(self, table: str, df: pl.DataFrame, p_file: str):
        dialect = self.config.dialect

        try:
            import duckdb

            con = duckdb.connect()
            dsn = self.config.get_duckdb_dsn()
            # 💡 *.parquet 대신 전달받은 p_file 경로만 사용하도록 수정!
            p_path = p_file.replace("\\", "/")

            if dialect == "postgresql":
                con.execute("INSTALL postgres; LOAD postgres;")
                con.execute(f"ATTACH '{dsn}' AS tgt (TYPE postgres);")
                con.execute(f"INSERT INTO tgt.{table} SELECT * FROM read_parquet('{p_path}');")
                return
            elif dialect == "mysql":
                con.execute("INSTALL mysql; LOAD mysql;")
                con.execute(f"ATTACH '{dsn}' AS tgt (TYPE mysql);")
                con.execute(f"INSERT INTO tgt.{table} SELECT * FROM read_parquet('{p_path}');")
                return
        except Exception:
            pass

        with self.engine.begin() as conn:
            records = df.to_dicts()
            if records:
                cols = list(records[0].keys())
                placeholders = ", ".join([f":{c}" for c in cols])
                col_names = ", ".join([f'"{c}"' for c in cols])
                sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
                conn.execute(text(sql), records)

    def restore_constraints_and_indexes(self, source_dialect: str):
        """각 DDL을 트랜잭션 세이브포인트로 격리하여, 앞선 실패와 상관없이 SET LOGGED까지 확실하게 실행 및 출력합니다."""
        tables = [t for t in self.metadata.keys() if not t.startswith("__")]

        with self.engine.begin() as conn:
            for table in tables:
                t_meta = self.metadata[table]
                pks = t_meta.get("primary_keys", [])
                indexes = t_meta.get("indexes", [])
                fks = t_meta.get("foreign_keys", [])

                self.console.print(f" 📦 [bold cyan]Table: {table}[/bold cyan]")
                tree_items = []

                # 1. Primary Key 복원
                if pks:
                    pk_name = f"pk_{table}"
                    pk_cols = ", ".join([f'"{c}"' for c in pks])
                    try:
                        conn.execute(text("SAVEPOINT sp_pk"))
                        conn.execute(
                            text(
                                f'ALTER TABLE {table} ADD CONSTRAINT "{pk_name}" PRIMARY KEY ({pk_cols})'
                            )
                        )
                        conn.execute(text("RELEASE SAVEPOINT sp_pk"))
                        tree_items.append(
                            f"🔑 [bold yellow]Primary Key[/bold yellow]: {pk_name} ({', '.join(pks)})"
                        )
                    except Exception:
                        conn.execute(text("ROLLBACK TO SAVEPOINT sp_pk"))
                        tree_items.append(
                            f"🔑 [grey50]Primary Key: {pk_name} (Already exists or skipped)[/grey50]"
                        )

                # 2. Indexes 복원
                if indexes:
                    for idx_idx, idx in enumerate(indexes):
                        sp_name = f"sp_idx_{idx_idx}"
                        idx_name = idx.get("name") or f"idx_{table}_{'_'.join(idx['columns'])}"
                        unique_str = "UNIQUE " if idx.get("unique") else ""
                        cols_str = ", ".join([f'"{c}"' for c in idx["columns"]])
                        try:
                            conn.execute(text(f"SAVEPOINT {sp_name}"))
                            conn.execute(
                                text(
                                    f'CREATE {unique_str}INDEX "{idx_name}" ON {table} ({cols_str})'
                                )
                            )
                            conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
                            tree_items.append(
                                f"🔍 [bold blue]Index[/bold blue]: {idx_name} ({', '.join(idx['columns'])}) [{unique_str.strip() or 'NON-UNIQUE'}]"
                            )
                        except Exception:
                            conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
                            tree_items.append(
                                f"🔍 [grey50]Index: {idx_name} (Already exists or skipped)[/grey50]"
                            )

                # 3. Foreign Keys 복원
                if fks:
                    for fk_idx, fk in enumerate(fks):
                        sp_name = f"sp_fk_{fk_idx}"
                        fk_name = (
                            fk.get("name") or f"fk_{table}_{'_'.join(fk['constrained_columns'])}"
                        )
                        cols_str = ", ".join([f'"{c}"' for c in fk["constrained_columns"]])
                        ref_cols_str = ", ".join([f'"{c}"' for c in fk["referred_columns"]])
                        ref_table = fk["referred_table"]

                        try:
                            conn.execute(text(f"SAVEPOINT {sp_name}"))
                            conn.execute(
                                text(
                                    f'ALTER TABLE {table} ADD CONSTRAINT "{fk_name}" FOREIGN KEY ({cols_str}) REFERENCES {ref_table} ({ref_cols_str})'
                                )
                            )
                            conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
                            tree_items.append(
                                f"🔗 [bold magenta]Foreign Key[/bold magenta]: {fk_name} ({', '.join(fk['constrained_columns'])}) ➔ {ref_table}({', '.join(fk['referred_columns'])})"
                            )
                        except Exception:
                            conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
                            tree_items.append(
                                f"🔗 [grey50]Foreign Key: {fk_name} (Skipped)[/grey50]"
                            )

                # 4. Sequence / Auto-Increment 값 현행화
                if self.config.dialect == "postgresql":
                    for col in t_meta.get("columns", []):
                        if col.get("auto_increment") or "INT" in col["type"].upper():
                            if pks and col["name"] in pks:
                                try:
                                    conn.execute(text("SAVEPOINT sp_seq"))
                                    seq_res = conn.execute(
                                        text(
                                            f"SELECT pg_get_serial_sequence('{table}', '{col['name']}')"
                                        )
                                    ).fetchone()
                                    if seq_res and seq_res[0]:
                                        seq_name = seq_res[0]
                                        conn.execute(
                                            text(
                                                f"SELECT setval('{seq_name}', COALESCE((SELECT MAX(\"{col['name']}\") FROM {table}), 1))"
                                            )
                                        )
                                        tree_items.append(
                                            f"🔢 [bold green]Sequence Reset[/bold green]: {seq_name} ➔ MAX({col['name']})"
                                        )
                                    conn.execute(text("RELEASE SAVEPOINT sp_seq"))
                                except Exception:
                                    conn.execute(text("ROLLBACK TO SAVEPOINT sp_seq"))

                # 5. SET UNLOGGED -> LOGGED 변경 (독립 SAVEPOINT 적용)
                if self.unlogged and self.config.dialect == "postgresql":
                    try:
                        conn.execute(text("SAVEPOINT sp_logged"))
                        conn.execute(text(f"ALTER TABLE {table} SET LOGGED"))
                        conn.execute(text("RELEASE SAVEPOINT sp_logged"))
                        tree_items.append("🛡️  [dim]Table persistence updated: SET LOGGED[/dim]")
                    except Exception as e:
                        conn.execute(text("ROLLBACK TO SAVEPOINT sp_logged"))
                        tree_items.append(
                            f"🛡️  [grey50]Table persistence update failed: {e}[/grey50]"
                        )

                # 트리 출력 처리
                total_items = len(tree_items)
                for idx, item in enumerate(tree_items):
                    is_last = idx == total_items - 1
                    branch = "   └── " if is_last else "   ├── "
                    self.console.print(f"{branch}{item}")

                self.console.print()
