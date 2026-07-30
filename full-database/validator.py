import os

import orjson
from sqlalchemy import create_engine, inspect, text


class DBValidator:
    def __init__(self, target_config, data_dir):
        self.config = target_config
        self.data_dir = data_dir
        self.checksums_path = os.path.join(data_dir, "source_checksums.json")
        self.metadata_path = os.path.join(data_dir, "metadata.json")
        self.mismatch_log_path = os.path.join(data_dir, "mismatch_details.log")

        self.engine = create_engine(self.config.get_sqlalchemy_url())
        self.source_checksums = self._load_json(self.checksums_path)
        self.metadata = self._load_json(self.metadata_path)

    def _load_json(self, path):
        if os.path.exists(path):
            with open(path, "rb") as f:
                return orjson.loads(f.read())
        return {}

    def _run_target_checksum_query(self, table: str, chunk_info: dict):
        pk_col = chunk_info.get("pk_col")
        if not pk_col:
            pks = self.metadata.get(table, {}).get("primary_keys", [])
            pk_col = pks[0] if pks else None

        pk_start = chunk_info.get("pk_start")
        pk_end = chunk_info.get("pk_end")
        is_numeric = chunk_info.get("is_numeric", False)

        with self.engine.connect() as conn:
            # 1. 수치형 PK 범위를 통한 정확한 Chunk 검증
            if is_numeric and pk_col and pk_start is not None and pk_end is not None:
                query = text(f"""
                    SELECT
                        COUNT(*),
                        COALESCE(SUM(CAST("{pk_col}" AS BIGINT)), 0)
                    FROM "{table}"
                    WHERE "{pk_col}" >= :start AND "{pk_col}" <= :end
                """)
                res = conn.execute(query, {"start": pk_start, "end": pk_end}).fetchone()
                return int(res[0]), int(res[1])  # type:ignore

            # 2. 비수치형 또는 범위 미지정 시 Fallback
            else:
                checksum_expr = (
                    f'COALESCE(SUM(CAST("{pk_col}" AS BIGINT)), 0)'
                    if (is_numeric and pk_col)
                    else "0"
                )
                query = text(f'SELECT COUNT(*), {checksum_expr} FROM "{table}"')
                res = conn.execute(query).fetchone()
                return int(res[0]), int(res[1])  # type:ignore

    def validate_table(self, table: str, ui_callback=None):
        chunks = self.source_checksums.get(table, [])
        if not chunks:
            return {
                "total_chunks": 0,
                "matched_chunks": 0,
                "mismatched_chunks": 0,
                "src_rows": 0,
                "tgt_rows": 0,
                "row_count_matched": True,
                "src_indexes": [],
                "tgt_indexes": [],
                "index_matched": True,
            }

        src_total_rows = sum(c.get("row_count", 0) for c in chunks)
        matched_chunks = 0
        mismatched_chunks = 0

        with self.engine.connect() as conn:
            tgt_total_rows = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).fetchone()[0]  # type:ignore

        for chunk in chunks:
            chunk_idx = chunk.get("chunk_index")
            src_checksum = chunk.get("checksum")

            tgt_rows, tgt_checksum = self._run_target_checksum_query(table, chunk)

            if tgt_rows == chunk.get("row_count") and tgt_checksum == src_checksum:
                matched_chunks += 1
                if ui_callback:
                    ui_callback(table, chunk_idx, True)
            else:
                mismatched_chunks += 1
                if ui_callback:
                    ui_callback(table, chunk_idx, False)

        # Target DB 실제 생성된 인덱스 수집
        inspector = inspect(self.engine)
        src_indexes = self.metadata.get(table, {}).get("indexes", [])
        tgt_indexes = inspector.get_indexes(table) if inspector.has_table(table) else []

        return {
            "total_chunks": len(chunks),
            "matched_chunks": matched_chunks,
            "mismatched_chunks": mismatched_chunks,
            "src_rows": src_total_rows,
            "tgt_rows": tgt_total_rows,
            "row_count_matched": (src_total_rows == tgt_total_rows),
            "src_indexes": src_indexes,
            "tgt_indexes": tgt_indexes,
            "index_matched": len(src_indexes) == len(tgt_indexes),
        }

    # 💡 main.py에서 호출하는 validate_all_tables 메서드 추가
    def validate_all_tables(self, ui_callback=None):
        tables = [t for t in self.metadata.keys() if not t.startswith("__")]
        results = {}
        for table in tables:
            results[table] = self.validate_table(table, ui_callback=ui_callback)
        return results
