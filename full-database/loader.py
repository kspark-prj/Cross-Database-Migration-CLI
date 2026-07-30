import glob
import os
import time

import duckdb
import orjson
from sqlalchemy import create_engine, text

from converter import DialectConverter


class DBLoader:
    def __init__(
        self,
        db_config,
        data_dir,
        chunk_size=100000,
        delay_ms=0,
        max_workers=4,
        resume=False,
        cleanup=False,
        source_dialect="mysql",
    ):
        self.config = db_config
        self.data_dir = data_dir
        self.chunk_size = chunk_size
        self.delay_ms = delay_ms
        self.max_workers = max_workers
        self.resume = resume
        self.cleanup = cleanup

        # Dialect 이름 정규화 (postgresql/postgres -> postgres)
        self.source_dialect = self._normalize_dialect(source_dialect)

        self.parquet_dir = os.path.join(data_dir, "parquet")
        self.metadata_path = os.path.join(data_dir, "metadata.json")
        self.schema_path = os.path.join(data_dir, "schema.sql")
        self.progress_path = os.path.join(data_dir, "load_progress.json")

        self.progress = self._load_progress()

        # 1. Target DB에 DDL 스키마 자동 변환 및 사전 적용
        self._apply_schema_if_needed()

    @staticmethod
    def _normalize_dialect(dialect_str):
        """sqlglot 및 DuckDB 호환용 Dialect 이름 정규화"""
        d = str(dialect_str).lower().strip()
        if d in ["postgresql", "postgres", "pg"]:
            return "postgres"
        if d in ["mysql", "mariadb"]:
            return "mysql"
        if d in ["sqlite", "sqlite3"]:
            return "sqlite"
        if d in ["oracle", "oracledb"]:
            return "oracle"
        if d in ["mssql", "sqlserver"]:
            return "tsql"
        return d

    def _apply_schema_if_needed(self):
        """schema.sql의 DDL을 Target DB Dialect로 변환(Transpile)하여 테이블 생성"""
        if not os.path.exists(self.schema_path):
            return

        raw_target_dialect = getattr(self.config, "dialect", "postgres")
        target_dialect = self._normalize_dialect(raw_target_dialect)

        try:
            with open(self.schema_path, "r", encoding="utf-8") as f:
                raw_schema_sql = f.read()

            if not raw_schema_sql.strip():
                return

            translated_statements = []

            # Source와 Target의 Dialect가 다르면 sqlglot으로 DDL 문법 및 타입 변환
            if self.source_dialect != target_dialect:
                try:
                    import sqlglot

                    translated_statements = sqlglot.transpile(
                        raw_schema_sql,
                        read=self.source_dialect,
                        write=target_dialect,
                        pretty=True,
                    )
                    print(f"🔄 [Schema Transpiler] DDL 변환 완료: {self.source_dialect.upper()} ➡️ {target_dialect.upper()}")
                except ImportError:
                    print("⚠️  [Schema Warning] sqlglot 미설치로 원본 DDL을 실행합니다. (pip install sqlglot)")
                    translated_statements = [raw_schema_sql]
                except Exception as trans_err:
                    print(f"⚠️  [Schema Transpile Warning] DDL 자동 변환 우회 (원본 구문 실행): {trans_err}")
                    translated_statements = [raw_schema_sql]
            else:
                translated_statements = [raw_schema_sql]

            # 변환된 DDL을 Target DB에 실행
            engine = create_engine(self.config.get_sqlalchemy_url())
            with engine.begin() as conn:
                for stmt in translated_statements:
                    for single_stmt in stmt.split(";"):
                        clean_stmt = single_stmt.strip()
                        if clean_stmt:
                            try:
                                conn.execute(text(clean_stmt))
                            except Exception:
                                pass
            engine.dispose()

        except Exception as e:
            print(f"⚠️  [Schema Notice] DDL 사전 준비 안내: {e}")

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

    def get_tables_to_load(self):
        if not os.path.exists(self.parquet_dir):
            return []

        tables = [d for d in os.listdir(self.parquet_dir) if os.path.isdir(os.path.join(self.parquet_dir, d)) and not d.startswith("__")]
        return sorted(tables)

    def load_table(self, table_name, ui_progress_callback=None):
        table_parquet_dir = os.path.join(self.parquet_dir, table_name)
        if not os.path.exists(table_parquet_dir):
            if ui_progress_callback:
                ui_progress_callback(table_name, finished=True)
            return

        if self.resume and self.progress.get(table_name, {}).get("status") == "completed":
            if ui_progress_callback:
                ui_progress_callback(table_name, skipped=True)
            return

        if table_name not in self.progress or self.cleanup:
            self.progress[table_name] = {"status": "in_progress", "loaded_files": []}
            self._save_progress()

        parquet_files = sorted(glob.glob(os.path.join(table_parquet_dir, "*.parquet")))
        total_files = len(parquet_files)
        accumulated_rows = 0

        raw_target_dialect = getattr(self.config, "dialect", "postgres")
        target_dialect = self._normalize_dialect(raw_target_dialect)

        # ---------------------------------------------------------------------
        # Mode A: PostgreSQL / MySQL (DuckDB C++ Native Extension - 초고속 적재)
        # ---------------------------------------------------------------------
        if target_dialect in ["postgres", "mysql"]:
            duckdb_ext_name = target_dialect  # 'postgres' 또는 'mysql'
            engine_name = f"DuckDB Native C++ Extension ({duckdb_ext_name.upper()})"
            print(f"🚀 [Engine: {engine_name}] '{table_name}' 적재 시작...")

            con = duckdb.connect(database=":memory:")
            try:
                # DuckDB 익스텐션 설치 및 로드
                con.execute(f"INSTALL {duckdb_ext_name}; LOAD {duckdb_ext_name};")

                raw_url = self.config.get_sqlalchemy_url()
                if "://" in raw_url and "+" in raw_url.split("://")[0]:
                    dialect_prefix = raw_url.split("://")[0].split("+")[0]
                    clean_url = f"{dialect_prefix}://{raw_url.split('://')[1]}"
                else:
                    clean_url = raw_url

                duckdb_type = "POSTGRES" if duckdb_ext_name == "postgres" else "MYSQL"
                con.execute(f"ATTACH '{clean_url}' AS target_db (TYPE {duckdb_type});")

                if self.cleanup:
                    try:
                        con.execute(f"TRUNCATE TABLE target_db.{table_name};")
                    except Exception:
                        pass

                for idx, p_file in enumerate(parquet_files, start=1):
                    file_name = os.path.basename(p_file)

                    if self.resume and not self.cleanup and file_name in self.progress[table_name].get("loaded_files", []):
                        continue

                    try:
                        row_count = con.execute(f"SELECT count(*) FROM read_parquet('{p_file}')").fetchone()[0]
                        file_size = os.path.getsize(p_file) if os.path.exists(p_file) else 0

                        if row_count > 0:
                            con.execute(f"INSERT INTO target_db.{table_name} SELECT * FROM read_parquet('{p_file}')")

                        accumulated_rows += row_count
                        self.progress[table_name]["loaded_files"].append(file_name)
                        self._save_progress()

                        if ui_progress_callback:
                            ui_progress_callback(
                                table_name,
                                curr_file=idx,
                                total_files=total_files,
                                rows=accumulated_rows,
                                bytes_size=file_size,
                            )

                        if self.delay_ms > 0:
                            time.sleep(self.delay_ms / 1000.0)

                    except Exception as e:
                        raise RuntimeError(f"Error loading {p_file} into {table_name} via {engine_name}: {e}")

            finally:
                con.close()

        # ---------------------------------------------------------------------
        # Mode B: Oracle, MSSQL, SQLite 등 (Polars/SQLAlchemy Batch - 호환성 적재)
        # ---------------------------------------------------------------------
        else:
            import polars as pl

            engine_name = f"SQLAlchemy Bulk Driver ({target_dialect.upper()})"
            print(f"📦 [Engine: {engine_name}] '{table_name}' 적재 시작...")

            engine = create_engine(self.config.get_sqlalchemy_url())

            if self.cleanup:
                try:
                    with engine.begin() as conn:
                        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                except Exception:
                    pass

            for idx, p_file in enumerate(parquet_files, start=1):
                file_name = os.path.basename(p_file)

                if self.resume and not self.cleanup and file_name in self.progress[table_name].get("loaded_files", []):
                    continue

                try:
                    df = pl.read_parquet(p_file)
                    row_count = df.height
                    file_size = os.path.getsize(p_file) if os.path.exists(p_file) else 0

                    if row_count > 0:
                        df.to_pandas().to_sql(
                            name=table_name,
                            con=engine,
                            if_exists="append",
                            index=False,
                            chunksize=10000,
                            method="multi",
                        )

                    accumulated_rows += row_count
                    self.progress[table_name]["loaded_files"].append(file_name)
                    self._save_progress()

                    if ui_progress_callback:
                        ui_progress_callback(
                            table_name,
                            curr_file=idx,
                            total_files=total_files,
                            rows=accumulated_rows,
                            bytes_size=file_size,
                        )

                    if self.delay_ms > 0:
                        time.sleep(self.delay_ms / 1000.0)

                except Exception as e:
                    raise RuntimeError(f"Error loading {p_file} into {table_name} via {engine_name}: {e}")

            engine.dispose()

        self.progress[table_name]["status"] = "completed"
        self._save_progress()

        if ui_progress_callback:
            ui_progress_callback(
                table_name,
                curr_file=total_files,
                total_files=total_files,
                rows=accumulated_rows,
                finished=True,
            )

    def apply_post_load_ddl(self):
        """데이터 적재 완료 후 PK, 인덱스, FK 제약조건을 생성한다.
        metadata.json에서 각 테이블의 primary_keys / indexes / foreign_keys 정보를 읽어
        DialectConverter를 통해 Target Dialect에 맞는 DDL을 생성·실행한다.
        실행 순서: PK → 인덱스 → FK
        """
        if not os.path.exists(self.metadata_path):
            print("⚠️  [Post-Load DDL] metadata.json이 없어 PK/인덱스/FK 생성을 건너뜁니다.")
            return

        try:
            with open(self.metadata_path, "rb") as f:
                metadata = orjson.loads(f.read())
        except Exception as e:
            print(f"⚠️  [Post-Load DDL] metadata.json 로드 실패: {e}")
            return

        raw_target_dialect = getattr(self.config, "dialect", "postgres")
        target_dialect = self._normalize_dialect(raw_target_dialect)
        converter = DialectConverter(
            source_dialect=self.source_dialect,
            target_dialect=target_dialect,
        )

        # DDL 항목을 (category, label, ddl) 튜플로 수집
        ddl_items = []

        # ── 1. PK 제약조건 ──────────────────────────────────────────────
        for table_name, table_meta in metadata.items():
            if table_name.startswith("__"):
                continue
            pks = table_meta.get("primary_keys", [])
            if pks:
                pk_label = f"{table_name} ({', '.join(pks)})"
                for ddl in converter.generate_pk_ddls(table_name, pks):
                    ddl_items.append(("PK", pk_label, ddl))

        # ── 2. 인덱스 ──────────────────────────────────────────────────
        for table_name, table_meta in metadata.items():
            if table_name.startswith("__"):
                continue
            indexes = table_meta.get("indexes", [])
            for idx in indexes:
                idx_name = idx.get("name", "unnamed")
                cols = idx.get("columns", [])
                if not cols:
                    continue
                if str(idx_name).upper() == "PRIMARY":
                    continue
                idx_label = f"{idx_name} ON {table_name} ({', '.join(cols)})"
                for ddl in converter.generate_index_ddls(table_name, [idx]):
                    ddl_items.append(("INDEX", idx_label, ddl))

        # ── 3. FK 제약조건 ──────────────────────────────────────────────
        for table_name, table_meta in metadata.items():
            if table_name.startswith("__"):
                continue
            fks = table_meta.get("foreign_keys", [])
            for fk in fks:
                fk_name = fk.get("name", "unnamed")
                ref_table = fk.get("referred_table", "?")
                fk_label = f"{fk_name} ON {table_name} → {ref_table}"
                for ddl in converter.generate_fk_ddls(table_name, [fk]):
                    ddl_items.append(("FK", fk_label, ddl))

        if not ddl_items:
            print("ℹ️  [Post-Load DDL] 생성할 PK/인덱스/FK가 없습니다.")
            return

        # 카테고리별 개수 집계
        pk_count = sum(1 for c, _, _ in ddl_items if c == "PK")
        idx_count = sum(1 for c, _, _ in ddl_items if c == "INDEX")
        fk_count = sum(1 for c, _, _ in ddl_items if c == "FK")

        print(
            f"🔧 [Post-Load DDL] PK {pk_count}개, "
            f"인덱스 {idx_count}개, "
            f"FK 제약조건 {fk_count}개 생성 시작..."
        )

        engine = create_engine(self.config.get_sqlalchemy_url())
        success_count = 0
        skip_count = 0
        try:
            with engine.begin() as conn:
                for category, label, ddl in ddl_items:
                    try:
                        conn.execute(text(ddl))
                        success_count += 1
                        print(f"  ✓ [{category:>5}] {label}")
                    except Exception as ddl_err:
                        # 이미 존재하는 PK/인덱스/FK는 무시 (중복 실행 안전)
                        skip_count += 1
                        print(f"  ⊘ [{category:>5}] {label}  (스킵: 이미 존재)")
        finally:
            engine.dispose()

        print(
            f"✅ [Post-Load DDL] 완료 — 성공: {success_count}개"
            + (f", 스킵(이미 존재): {skip_count}개" if skip_count else "")
        )

