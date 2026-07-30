import argparse
import glob
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import orjson
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text

from config import DBConfig
from extractor import DBExtractor
from loader import DBLoader
from ui import MigrationUI
from validator import DBValidator

console = Console()


# Custom Progress Column: Parquet 파일 적재 진행도 표시
class FileProgressColumn(ProgressColumn):
    def render(self, task):
        curr_file = task.fields.get("curr_file", 0)
        total_files = task.fields.get("total_files", 0)
        current_rows = task.fields.get("rows", 0)
        is_done = task.fields.get("is_done", False)
        is_skipped = task.fields.get("is_skipped", False)

        if is_skipped:
            return Text(f"[Skipped] ({current_rows:,} rows)", style="bold yellow")
        elif is_done:
            return Text(
                f"[Done]    ({total_files}/{total_files} files | {current_rows:,} rows)",
                style="bold green",
            )
        elif total_files > 0:
            return Text(
                f"Loading... ({curr_file}/{total_files} files | {current_rows:,} rows)",
                style="bold cyan",
            )
        else:
            return Text("Processing...", style="dim cyan")


def parse_args():
    parser = argparse.ArgumentParser(description="VeloxDB Cross-Database Migration & Validation CLI")

    parser.add_argument(
        "--mode",
        choices=["extract", "load", "validate", "all"],
        default="load",
        help="Execution mode: extract, load, validate, or all (default: load)",
    )
    parser.add_argument(
        "--target-uri",
        type=str,
        default=None,
        help="Target DB Connection URI (Required for load, validate, all)",
    )
    parser.add_argument(
        "--source-uri",
        type=str,
        default=None,
        help="Source DB Connection URI (Required for extract)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./mig_assets",
        help="Directory containing exported parquet and metadata",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Number of parallel worker threads",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100000,
        help="Batch chunk size for extraction/loading",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=0,
        help="Delay in milliseconds between chunk loads",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume execution from the last checkpoint",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Truncate target tables before loading",
    )

    return parser.parse_args()


def detect_source_dialect(data_dir: str, target_dialect: str, fallback_source_uri: str = None) -> str:
    meta_path = os.path.join(data_dir, "metadata.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "rb") as f:
                meta = orjson.loads(f.read())
                if "__source_dialect__" in meta:
                    return meta["__source_dialect__"]
                if "source_dialect" in meta:
                    return meta["source_dialect"]
                if "dialect" in meta:
                    return meta["dialect"]
        except Exception:
            pass

    if fallback_source_uri:
        try:
            return DBConfig(fallback_source_uri).dialect
        except Exception:
            pass

    return target_dialect


def display_header(mode, target_dialect, source_dialect, output_dir):
    panel_content = (
        f"Phase: [bold yellow]{mode.upper()}[/bold yellow]\n"
        f"Source DB Dialect: [bold blue]{source_dialect}[/bold blue] (Auto Detected)\n"
        f"Target DB Dialect: [bold green]{target_dialect}[/bold green] (From Target URI)\n"
        f"Data Directory: [bold cyan]{output_dir}[/bold cyan]"
    )
    console.print(
        Panel(
            panel_content,
            title="[bold magenta]VeloxDB Cross-DB Migration CLI[/bold magenta]",
            expand=True,
        )
    )


def run_extract_phase(args):
    console.print("[bold yellow]🚀 Starting Data Extraction Phase...[/bold yellow]")
    # 💡 콘솔에 ConnectorX + Polars 엔진 사용 명시
    console.print("[bold yellow]⚡ Engine: ConnectorX (C/Rust Native) + Polars (Apache Arrow)[/bold yellow]\n")
    console.print(
        "[bold yellow]⚡ [ ConnectorX가 Rust 엔진으로 DB에서 데이터를 가장 빠르게 뽑아오면, Polars가 Arrow 메모리 구조를 이용해 복사 과정 없이 즉시 Parquet 파일로 저장 ][/bold yellow]\n"
    )
    try:
        source_config = DBConfig(args.source_uri)
        extractor = DBExtractor(
            db_config=source_config,
            output_dir=args.output_dir,
            chunk_size=args.chunk_size,
            delay_ms=args.delay_ms,
            max_workers=args.max_workers,
            resume=args.resume,
        )

        console.print("[cyan]🔍 Extracting Schema Metadata...[/cyan]")
        metadata = extractor.extract_metadata()
        tables = [t for t in metadata.keys() if t != "__source_dialect__"]

        if not tables:
            console.print("[yellow]⚠️ No tables found to extract.[/yellow]")
            return

        console.print(f"📦 [bold green]Found {len(tables)} table(s) to extract:[/bold green] {', '.join(tables)}\n")

        ui = MigrationUI()

        def extraction_task(update_callback, completed_tables, tasks, table_stats, progress_obj):
            def work_fn(table_name):
                try:
                    meta = metadata[table_name]

                    # Extractor 콜백 연결
                    def extract_cb(tbl, rows=0, bytes_size=0, finished=False, skipped=False, **kwargs):
                        update_callback(
                            table_name=tbl,
                            rows=rows,
                            bytes_size=bytes_size,
                            finished=finished,
                            skipped=skipped,
                        )

                    extractor.extract_table(table_name, meta, ui_progress_callback=extract_cb)
                    return table_name, True, None
                except Exception as e:
                    return table_name, False, str(e)

            failed_tables = []
            with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                futures = {executor.submit(work_fn, t): t for t in tables}
                for future in as_completed(futures):
                    tbl, success, err_msg = future.result()
                    if not success:
                        failed_tables.append((tbl, err_msg))
                        console.print(f"\n[bold red]✗ Failed to extract table {tbl}:[/bold red] {err_msg}")

        start_time = time.time()
        ui.run_with_progress(
            title="Extracting Tables",
            tables=tables,
            table_sizes={},
            run_fn=extraction_task,
        )
        elapsed = time.time() - start_time
        console.print(f"\n[bold green]✓ Extraction phase completed! (Elapsed: {elapsed:.2f}s)[/bold green]\n")

    except Exception as e:
        console.print(f"[bold red]❌ Extraction Phase Critical Error:[/bold red] {e}")
        sys.exit(1)


def run_load_phase(args, target_config, source_dialect):
    loader = DBLoader(
        db_config=target_config,
        data_dir=args.output_dir,
        chunk_size=args.chunk_size,
        delay_ms=args.delay_ms,
        max_workers=args.max_workers,
        resume=args.resume,
        cleanup=args.cleanup,
        source_dialect=source_dialect,
    )

    tables = loader.get_tables_to_load()
    if not tables:
        console.print(f"[bold yellow]⚠️  No tables found to load in[/bold yellow] {args.output_dir}")
        return

    console.print(f"📦 [bold green]Found {len(tables)} table(s) to load:[/bold green] {', '.join(tables)}\n")

    start_time = time.time()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        FileProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        refresh_per_second=10,
    ) as progress:
        task_ids = {}
        for t in tables:
            task_ids[t] = progress.add_task(
                f"Table: {t:<22}",
                curr_file=0,
                total_files=0,
                rows=0,
                is_done=False,
                is_skipped=False,
            )

        def ui_callback(
            table_name,
            curr_file=0,
            total_files=0,
            rows=0,
            finished=False,
            skipped=False,
            bytes_size=0,
            **kwargs,
        ):
            task_id = task_ids.get(table_name)
            if task_id is None:
                return

            task = progress.tasks[task_id]

            if skipped:
                progress.update(
                    task_id,
                    curr_file=curr_file,
                    total_files=total_files,
                    rows=rows,
                    is_done=True,
                    is_skipped=True,
                )
            elif finished:
                final_rows = rows if rows > 0 else task.fields.get("rows", 0)
                progress.update(
                    task_id,
                    curr_file=total_files,
                    total_files=total_files,
                    rows=final_rows,
                    is_done=True,
                    is_skipped=False,
                )
            else:
                progress.update(
                    task_id,
                    curr_file=curr_file,
                    total_files=total_files,
                    rows=rows,
                    is_done=False,
                    is_skipped=False,
                )

        def work_fn(table_name):
            try:
                loader.load_table(table_name, ui_progress_callback=ui_callback)
                return table_name, True, None
            except Exception as e:
                return table_name, False, str(e)

        failed_tables = []
        with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            futures = {executor.submit(work_fn, t): t for t in tables}
            for future in as_completed(futures):
                tbl, success, err_msg = future.result()
                if not success:
                    failed_tables.append((tbl, err_msg))
                    console.print(f"\n[bold red]✗ Failed to load table {tbl}:[/bold red] {err_msg}")

    elapsed = time.time() - start_time
    if failed_tables:
        console.print(f"\n[bold red]❌ Loading completed with {len(failed_tables)} errors! (Elapsed: {elapsed:.2f}s)[/bold red]")
    else:
        console.print(f"\n[bold green]✓ Loading phase completed successfully! (Elapsed: {elapsed:.2f}s)[/bold green]\n")


def run_validation_phase(args, target_config):
    console.print("[bold yellow]Starting Data Validation...[/bold yellow]")

    try:
        validator = DBValidator(
            target_config=target_config,
            data_dir=args.output_dir,
            source_config=DBConfig(args.source_uri) if args.source_uri else None,
        )
        val_results = validator.validate()

        table = Table(title="Data Validation Summary")
        table.add_column("Table Name", style="cyan")
        table.add_column("Status", style="bold")
        table.add_column("Details", style="magenta")

        all_valid = True
        for res in val_results:
            tbl_name = res.get("table", "Unknown")
            status = res.get("status", "failed")
            is_valid = status == "passed"

            if is_valid:
                status_str = "[green]PASSED[/green]"
                details = f"Match ({res.get('source_rows', 0):,} rows)"
            else:
                all_valid = False
                status_str = f"[red]{status.upper()}[/red]"
                details = res.get("reason", f"Src: {res.get('source_rows', 0):,} / Tgt: {res.get('target_rows', 0):,}")

            table.add_row(tbl_name, status_str, details)

        console.print(table)

        if all_valid:
            console.print("[bold green]🎉 All data validation checks passed![/bold green]")
        else:
            console.print("[bold red]⚠️ Data validation detected discrepancies![/bold red]")

    except Exception as e:
        console.print(f"[bold red]❌ Validation Phase Error:[/bold red] {e}")


def main():
    args = parse_args()

    # 모드별 인자 검증
    if args.mode in ["extract", "all"] and not args.source_uri:
        console.print("[bold red]Error: --source-uri is required for 'extract' mode.[/bold red]")
        sys.exit(1)

    if args.mode in ["load", "validate", "all"] and not args.target_uri:
        console.print("[bold red]Error: --target-uri is required for 'load' / 'validate' / 'all' mode.[/bold red]")
        sys.exit(1)

    target_config = None
    if args.target_uri:
        try:
            target_config = DBConfig(args.target_uri)
        except Exception as e:
            console.print(f"[bold red]Error parsing Target DB URI:[/bold red] {e}")
            sys.exit(1)

    target_dialect = target_config.dialect if target_config else "N/A"
    source_dialect = detect_source_dialect(args.output_dir, target_dialect, args.source_uri)

    display_header(
        mode=args.mode,
        target_dialect=target_dialect,
        source_dialect=source_dialect,
        output_dir=args.output_dir,
    )

    if args.mode in ["extract", "all"]:
        run_extract_phase(args)

    if args.mode in ["load", "all"]:
        run_load_phase(args, target_config, source_dialect)
        run_validation_phase(args, target_config)

    if args.mode in ["validate", "all"]:
        run_validation_phase(args, target_config)


if __name__ == "__main__":
    main()
