import time

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    ProgressColumn,
)
from rich.table import Table
from rich.text import Text


class ChunkProgressColumn(ProgressColumn):
    def render(self, task):
        chunk_str = task.fields.get("chunk_str", "")
        return Text(chunk_str, style="progress.download")


class CurrentSizeColumn(ProgressColumn):
    def render(self, task):
        completed_bytes = task.fields.get("completed_bytes", 0)
        megabytes = completed_bytes / (1024 * 1024)
        return Text(f"{megabytes:.2f} MB", style="progress.download")


class MigrationUI:
    def __init__(self):
        self.console = Console()

    def print_welcome(
        self,
        mode: str,
        source: str = None,
        target: str = None,
        output_dir: str = None,
    ):
        title = "[bold green]VeloxDB Cross-DB Migration CLI[/bold green]"

        info_lines = []
        if mode == "extract":
            info_lines.append(
                "[bold cyan]Phase 1: Dump & Extract (Source DB Connection Mode)[/bold cyan]"
            )
            info_lines.append(f"Source DB Dialect: [yellow]{source}[/yellow]")
            info_lines.append(f"Output Directory: [yellow]{output_dir}[/yellow]")
        else:
            info_lines.append(
                "[bold cyan]Phase 2: Load & Validate (Target DB Connection Mode)[/bold cyan]"
            )
            info_lines.append(f"Target DB Dialect: [yellow]{target}[/yellow]")
            info_lines.append(f"Data Source Directory: [yellow]{output_dir}[/yellow]")

        panel = Panel(
            "\n".join(info_lines),
            title=title,
            border_style="green",
            padding=(1, 2),
        )
        self.console.print(panel)
        self.console.print()

    def print_success(self, text: str):
        self.console.print(f"[bold green]✓[/bold green] {text}")

    def print_error(self, text: str):
        self.console.print(f"[bold red]✗[/bold red] {text}")

    def print_warning(self, text: str):
        self.console.print(f"[bold yellow]![/bold yellow] {text}")

    def run_with_progress(self, title: str, tables: list, table_sizes: dict, run_fn):
        """
        :param title: 프로그레스바 타이틀
        :param tables: 대상 테이블 리스트
        :param table_sizes: 테이블별 예상 전체 바이트 용량 {table_name: bytes}
        """
        is_extract = "Extracting" in title
        done_verb = "Extracted" if is_extract else "Loaded"

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            TaskProgressColumn(),
            ChunkProgressColumn(),  # "Extracted chunk 7/500" 형태로 커스텀 표시
            CurrentSizeColumn(),    # "? 0:00:00" 대신 현재까지의 용량을 MB 단위로 표시
            console=self.console,
        )

        tasks = {}
        for table in tables:
            tasks[table] = progress.add_task(
                f"Table: {table}", 
                total=None,
                chunk_str="",
                completed_bytes=0
            )

        overall_total_bytes = sum(table_sizes.values())
        total_task = progress.add_task(
            "[bold yellow]Overall Progress[/bold yellow]",
            total=overall_total_bytes if overall_total_bytes > 0 else len(tables),
        )

        table_stats = {t: {"rows": 0, "bytes": 0} for t in tables}
        completed_tables = set()

        def update_callback(table_name, rows, bytes_size=0, skipped=False, chunk_idx=None, total_chunks=None):
            if table_name not in tasks:
                return

            task_id = tasks[table_name]

            if skipped:
                cur_total = total_chunks or progress.tasks[task_id].total or 1
                progress.update(
                    task_id,
                    description=f"[grey50]Table: {table_name} (추출완료)[/grey50]" if is_extract else f"[grey50]Table: {table_name} (적재완료)[/grey50]",
                    completed=cur_total,
                    total=cur_total,
                    completed_bytes=bytes_size,
                    chunk_str="Skipped"
                )
                if table_name not in completed_tables:
                    completed_tables.add(table_name)
                    if overall_total_bytes > 0:
                        progress.advance(total_task, advance=bytes_size)
                    else:
                        progress.advance(total_task, advance=1)
                return

            table_stats[table_name]["rows"] += rows
            table_stats[table_name]["bytes"] += bytes_size

            completed_bytes = table_stats[table_name]["bytes"]

            if chunk_idx is not None:
                if chunk_idx == 0:
                    desc = f"Table: {table_name} (용량계산중...)" if is_extract else f"Table: {table_name} (적재준비중...)"
                    chunk_str = ""
                    completed = 0
                else:
                    desc = f"Table: {table_name} (추출중...)" if is_extract else f"Table: {table_name} (적재중...)"
                    if total_chunks is not None:
                        chunk_str = f"{done_verb} chunk {chunk_idx}/{total_chunks}"
                    else:
                        chunk_str = f"{done_verb} chunk {chunk_idx}"
                    completed = chunk_idx
            else:
                desc = f"Table: {table_name} (추출중...)" if is_extract else f"Table: {table_name} (적재중...)"
                chunk_str = ""
                completed = progress.tasks[task_id].completed

            progress.update(
                task_id,
                description=desc,
                completed=completed,
                total=total_chunks if total_chunks is not None else progress.tasks[task_id].total,
                completed_bytes=completed_bytes,
                chunk_str=chunk_str
            )

            if overall_total_bytes > 0:
                progress.advance(total_task, advance=bytes_size)

        with Live(progress, console=self.console, refresh_per_second=10):
            run_fn(update_callback, completed_tables, tasks, total_task, progress)

            for table in tables:
                if table not in completed_tables:
                    final_bytes = table_stats[table]["bytes"]
                    task_total = progress.tasks[tasks[table]].total or 1
                    progress.update(
                        tasks[table],
                        description=f"[bold green]Table: {table} (추출완료)[/bold green]" if is_extract else f"[bold green]Table: {table} (적재완료)[/bold green]",
                        completed=task_total,
                        total=task_total,
                        completed_bytes=final_bytes,
                    )

    def display_validation_report(self, results: dict, mismatch_log_path: str):
        self.console.print()
        self.console.print(
            "[bold cyan]================================================================================[/bold cyan]"
        )
        self.console.print(
            "[bold cyan]                         OFFLINE INTEGRITY VALIDATION REPORT                    [/bold cyan]"
        )
        self.console.print(
            "[bold cyan]================================================================================[/bold cyan]"
        )
        self.console.print()

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Table Name", style="bold white")
        table.add_column("Src Rows", justify="right")
        table.add_column("Tgt Rows", justify="right")
        table.add_column("Row Match", justify="center")
        table.add_column("Src Indexes", justify="right")
        table.add_column("Tgt Indexes", justify="right")
        table.add_column("Index Match", justify="center")
        table.add_column("Total Chunks", justify="right")
        table.add_column("Matched Chunks", justify="right", style="green")
        table.add_column("Mismatched Chunks", justify="right", style="red")
        table.add_column("Status", justify="center")

        overall_mismatch = False

        for table_name, res in results.items():
            total = res["total_chunks"]
            matched = res["matched_chunks"]
            mismatched = res["mismatched_chunks"]

            src_rows = res["src_rows"]
            tgt_rows = res["tgt_rows"]
            row_match = (
                "[bold green]MATCH[/bold green]"
                if res["row_count_matched"]
                else "[bold red]MISMATCH[/bold red]"
            )

            src_idx_count = len(res["src_indexes"])
            tgt_idx_count = len(res["tgt_indexes"])
            idx_match = (
                "[bold green]MATCH[/bold green]"
                if res["index_matched"]
                else "[bold red]MISMATCH[/bold red]"
            )

            if mismatched > 0 or not res["row_count_matched"] or not res["index_matched"]:
                status = "[bold red]FAIL[/bold red]"
                overall_mismatch = True
            else:
                status = "[bold green]PASS[/bold green]"

            table.add_row(
                table_name,
                f"{src_rows:,}" if src_rows >= 0 else "N/A",
                f"{tgt_rows:,}" if tgt_rows >= 0 else "N/A",
                row_match,
                str(src_idx_count),
                str(tgt_idx_count),
                idx_match,
                str(total),
                str(matched),
                str(mismatched),
                status,
            )

        self.console.print(table)
        self.console.print()

        if overall_mismatch:
            self.print_error(
                f"Integrity check failed on some tables. Mismatch details logged to: [yellow]{mismatch_log_path}[/yellow]"
            )
        else:
            self.print_success(
                "Integrity verification completed successfully. All tables matched perfectly!"
            )
        self.console.print()
