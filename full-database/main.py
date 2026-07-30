import glob
import os
import shutil
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import orjson

from config import DBConfig, parse_args, validate_args
from extractor import DBExtractor
from loader import DBLoader
from ui import MigrationUI
from validator import DBValidator


def run_extract_phase(args, ui):
    source_cfg = DBConfig(args.source_uri)
    ui.print_welcome(mode="extract", source=source_cfg.dialect, output_dir=args.output_dir)

    extractor = DBExtractor(
        db_config=source_cfg,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
        delay_ms=args.delay_ms,
        max_workers=args.max_workers,
        resume=args.resume,
    )

    engine_statuses = extractor.check_engine_status()
    for is_ok, msg in engine_statuses:
        if is_ok:
            ui.print_success(msg)
        else:
            ui.print_warning(msg)

    ui.console.print("Extracting database metadata and structures...")
    metadata = extractor.extract_metadata()
    tables = [t for t in metadata.keys() if not t.startswith("__")]

    ui.console.print(f"Discovered [yellow]{len(tables)}[/yellow] tables to extract.")

    # 추출 단계에서는 추출 전 용량을 모르므로 0으로 전달 (동적 증가)
    table_sizes = {t: 0 for t in tables}

    def run_parallel_extraction(update_callback, completed_tables, tasks, total_task, progress):
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {}
            for table in tables:

                def make_work_fn(t=table):
                    extractor.extract_table(t, metadata[t], ui_progress_callback=update_callback)
                    return t

                futures[executor.submit(make_work_fn)] = table

            for future in as_completed(futures):
                t_name = futures[future]
                try:
                    future.result()
                    completed_tables.add(t_name)
                    progress.update(
                        tasks[t_name],
                        description=f"[bold green]Table: {t_name} (Extracted)[/bold green]",
                    )
                except Exception as e:
                    progress.update(
                        tasks[t_name],
                        description=f"[bold red]Table: {t_name} (Failed)[/bold red]",
                    )
                    ui.print_error(f"Failed to extract table {t_name}: {e}")
                    traceback.print_exc()

    ui.run_with_progress("Extracting Tables", tables, table_sizes, run_parallel_extraction)
    ui.print_success("Extraction phase completed! Parquet files and metadata stored successfully.")


def run_load_phase(args, ui):
    target_cfg = DBConfig(args.target_uri)

    loader = DBLoader(
        db_config=target_cfg,
        data_dir=args.output_dir,
        max_workers=args.max_workers,
        resume=args.resume,
        unlogged=True,
    )

    duckdb_ok, duckdb_msg = loader.check_duckdb_support()
    if duckdb_ok:
        ui.print_success(f"{duckdb_msg} (Bulk Load Primary)")
    else:
        ui.print_warning(f"{duckdb_msg} (Native COPY/executemany active)")

    source_dialect = loader.metadata.get("__source_dialect__")
    if not source_dialect:
        source_dialect = "mysql"
        ui.print_warning(f"Source dialect not found in metadata. Defaulting to: {source_dialect}")

    ui.print_welcome(mode="load", target=target_cfg.dialect, output_dir=args.output_dir)

    tables = [t for t in loader.metadata.keys() if not t.startswith("__")]

    ui.console.print("Step 1/4: Applying skeletal schema DDL...")
    loader.prepare_schema(source_dialect)
    ui.print_success("Skeletal tables created.")

    ui.console.print("Step 2/4: Bulk loading data chunks into Target DB...")

    # 테이블별 예상 바이트 용량 수집 (source_checksums.json 또는 실제 Parquet 디렉토리 용량)
    checksums_path = os.path.join(args.output_dir, "source_checksums.json")
    table_sizes = {}

    source_checksums = {}
    if os.path.exists(checksums_path):
        try:
            with open(checksums_path, "rb") as f:
                source_checksums = orjson.loads(f.read())
        except Exception:
            pass

    for t in tables:
        # 1) source_checksums.json에서 file_size 정보 합산
        t_size = sum(chunk.get("file_size", 0) for chunk in source_checksums.get(t, []))

        # 2) 파일 크기가 명시 안 된 기존 호환성을 위해 실제 로컬 Parquet 파일 크기 측정
        if t_size == 0:
            t_dir = os.path.join(args.output_dir, "parquet", t)
            if os.path.exists(t_dir):
                for pf in glob.glob(os.path.join(t_dir, "part-*.parquet")):
                    t_size += os.path.getsize(pf)
        table_sizes[t] = t_size

    def run_parallel_load(update_callback, completed_tables, tasks, total_task, progress):
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {}
            for table in tables:

                def make_work_fn(t=table):
                    loader.load_table_data(t, ui_progress_callback=update_callback)
                    return t

                futures[executor.submit(make_work_fn)] = table

            for future in as_completed(futures):
                t_name = futures[future]
                try:
                    future.result()
                    completed_tables.add(t_name)
                    progress.update(
                        tasks[t_name],
                        description=f"[bold green]Table: {t_name} (Loaded)[/bold green]",
                    )
                except Exception as e:
                    progress.update(
                        tasks[t_name],
                        description=f"[bold red]Table: {t_name} (Failed)[/bold red]",
                    )
                    ui.print_error(f"Failed to load table {t_name}: {e}")
                    traceback.print_exc()

    ui.run_with_progress("Loading Tables", tables, table_sizes, run_parallel_load)
    ui.print_success("Bulk loading completed.")

    ui.console.print("Step 3/4: Restoring Indexes, FK Constraints, and Sequence values...")
    loader.restore_constraints_and_indexes(source_dialect)
    ui.print_success("Schema structure restored.")

    ui.console.print("Step 4/4: Performing Offline Integrity Validation...")
    validator = DBValidator(target_cfg, args.output_dir)

    def validation_ui_callback(table_name, chunk_idx, success):
        pass

    results = validator.validate_all_tables(ui_callback=validation_ui_callback)
    ui.display_validation_report(results, validator.mismatch_log_path)

    if args.cleanup:
        ui.console.print("\nCleaning up temporary Parquet files...")
        parquet_dir = os.path.join(args.output_dir, "parquet")
        if os.path.exists(parquet_dir):
            try:
                shutil.rmtree(parquet_dir)
                ui.print_success("Temporary Parquet files removed successfully.")
            except Exception as e:
                ui.print_error(f"Failed to clean up parquet files: {e}")


def main():
    try:
        args = parse_args()
        validate_args(args)

        ui = MigrationUI()

        if args.mode == "extract":
            run_extract_phase(args, ui)
        elif args.mode == "load":
            run_load_phase(args, ui)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
