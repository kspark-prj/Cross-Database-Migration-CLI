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
)
from rich.table import Table


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
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            TaskProgressColumn(),  # % 진행률 표시
            DownloadColumn(),  # 현재 용량 / 전체 용량 (예: 11.9/48.5 MB)
            TransferSpeedColumn(),  # 전송 속도 (예: 5.2 MB/s)
            TimeRemainingColumn(),  # 남은 예상 시간 (ETA)
            console=self.console,
        )

        tasks = {}
        for table in tables:
            total_bytes = table_sizes.get(table, 0)
            # total_bytes가 0인 경우 None으로 설정되어 dynamic 처리 가능하도록 조치
            tasks[table] = progress.add_task(
                f"Table: {table}", total=total_bytes if total_bytes > 0 else None
            )

        overall_total_bytes = sum(table_sizes.values())
        total_task = progress.add_task(
            "[bold yellow]Overall Progress[/bold yellow]",
            total=overall_total_bytes if overall_total_bytes > 0 else len(tables),
        )

        table_stats = {t: {"rows": 0, "bytes": 0} for t in tables}
        completed_tables = set()

        def update_callback(table_name, rows, bytes_size=0, skipped=False):
            if table_name not in tasks:
                return

            task_id = tasks[table_name]

            if skipped:
                cur_total = progress.tasks[task_id].total or bytes_size
                progress.update(
                    task_id,
                    description=f"[grey50]Table: {table_name} (Skipped - Done)[/grey50]",
                    completed=cur_total,
                    total=cur_total,
                )
                if table_name not in completed_tables:
                    completed_tables.add(table_name)
                    if overall_total_bytes > 0:
                        progress.advance(total_task, advance=cur_total)
                    else:
                        progress.advance(total_task, advance=1)
                return

            table_stats[table_name]["rows"] += rows
            table_stats[table_name]["bytes"] += bytes_size

            completed_bytes = table_stats[table_name]["bytes"]

            # total 수치가 지정되어 있지 않았을 경우 자동 상향 동적 조정
            cur_task_total = progress.tasks[task_id].total
            if cur_task_total is None or completed_bytes > cur_task_total:
                progress.update(task_id, completed=completed_bytes, total=completed_bytes)
            else:
                progress.update(task_id, completed=completed_bytes)

            if overall_total_bytes > 0:
                progress.advance(total_task, advance=bytes_size)

        with Live(progress, console=self.console, refresh_per_second=10):
            run_fn(update_callback, completed_tables, tasks, total_task, progress)

            for table in tables:
                if table not in completed_tables:
                    final_bytes = table_stats[table]["bytes"]
                    progress.update(
                        tasks[table],
                        completed=final_bytes,
                        total=final_bytes,
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
