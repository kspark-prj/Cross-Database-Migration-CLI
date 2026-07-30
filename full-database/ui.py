import time

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, ProgressColumn, SpinnerColumn, TextColumn
from rich.text import Text


class DynamicMetricsColumn(ProgressColumn):
    """실시간 정보 렌더링 (완료 시 초당 건수 완전 제거)"""

    def render(self, task):
        completed_rows = task.fields.get("completed_rows", 0)
        tps = task.fields.get("tps", None)
        is_completed = task.fields.get("completed", False)

        # completed가 True이거나, tps 정보가 없으면 완벽하게 건수만 출력
        if is_completed or tps is None:
            return Text(f"[현재 {completed_rows:,}건 완료]", style="bold green")
        else:
            return Text(f"[현재 {completed_rows:,}건 완료 | 초당 {tps:,.0f}건 처리 중]", style="bold cyan")


class CurrentSizeColumn(ProgressColumn):
    """현재 추출된 바이트 용량 렌더링"""

    def render(self, task):
        completed_bytes = task.fields.get("completed_bytes", 0)
        megabytes = completed_bytes / (1024 * 1024)
        return Text(f"{megabytes:.2f} MB", style="bold yellow")


class MigrationUI:
    def __init__(self):
        self.console = Console()

    def print_welcome(self, mode: str, source: str = None, target: str = None, output_dir: str = None):
        title = "[bold green]VeloxDB Cross-DB Migration CLI[/bold green]"
        info_lines = []
        if mode == "extract":
            info_lines.append("[bold cyan]Phase 1: Dump & Extract (Source DB Connection Mode)[/bold cyan]")
            info_lines.append(f"Source DB Dialect: [yellow]{source}[/yellow]")
            info_lines.append(f"Output Directory: [yellow]{output_dir}[/yellow]")
        else:
            info_lines.append("[bold cyan]Phase 2: Load & Validate (Target DB Connection Mode)[/bold cyan]")
            info_lines.append(f"Target DB Dialect: [yellow]{target}[/yellow]")
            info_lines.append(f"Data Source Directory: [yellow]{output_dir}[/yellow]")

        panel = Panel("\n".join(info_lines), title=title, border_style="green", padding=(1, 2))
        self.console.print(panel)
        self.console.print()

    def print_success(self, text: str):
        self.console.print(f"[bold green]✓[/bold green] {text}")

    def print_error(self, text: str):
        self.console.print(f"[bold red]✗[/bold red] {text}")

    def print_warning(self, text: str):
        self.console.print(f"[bold yellow]![/bold yellow] {text}")

    def run_with_progress(self, title: str, tables: list, table_sizes: dict, run_fn):
        is_extract = "Extracting" in title

        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            DynamicMetricsColumn(),
            CurrentSizeColumn(),
            console=self.console,
            refresh_per_second=10,
        )

        tasks = {}
        for table in tables:
            tasks[table] = progress.add_task(f"Table: {table}", total=None, completed_rows=0, completed_bytes=0, tps=0, completed=False)

        table_stats = {t: {"rows": 0, "bytes": 0, "start_time": time.time()} for t in tables}
        completed_tables = set()

        def update_callback(table_name, rows=0, bytes_size=0, skipped=False, finished=False, chunk_idx=None, total_chunks=None):
            if table_name not in tasks:
                return

            task_id = tasks[table_name]

            if skipped:
                progress.update(
                    task_id,
                    description=f"[grey50]Table: {table_name} (추출완료)[/grey50]"
                    if is_extract
                    else f"[grey50]Table: {table_name} (적재완료)[/grey50]",
                    completed_bytes=bytes_size,
                    completed=True,
                    tps=None,
                )
                if table_name not in completed_tables:
                    completed_tables.add(table_name)
                return

            # 일반 진행 중일 때만 데이터 누적
            if not finished:
                table_stats[table_name]["rows"] += rows
                table_stats[table_name]["bytes"] += bytes_size

            elapsed = max(0.001, time.time() - table_stats[table_name]["start_time"])
            current_rows = table_stats[table_name]["rows"]
            current_bytes = table_stats[table_name]["bytes"]

            # finished=True일 때 tps=None으로 밀어버리고 completed=True 설정!
            if finished:
                progress.update(
                    task_id,
                    description=f"[bold green]Table: {table_name} (추출완료 - {elapsed:.2f}초)[/bold green]"
                    if is_extract
                    else f"[bold green]Table: {table_name} (적재완료 - {elapsed:.2f}초)[/bold green]",
                    completed_rows=current_rows,
                    completed_bytes=current_bytes,
                    completed=True,
                    tps=None,  # <--- 핵심: tps 자체를 None으로 지워버림!
                )
                completed_tables.add(table_name)
            else:
                tps = current_rows / elapsed
                progress.update(
                    task_id, description=f"Table: {table_name}", completed_rows=current_rows, completed_bytes=current_bytes, tps=tps, completed=False
                )

        with Live(progress, console=self.console, refresh_per_second=10):
            run_fn(update_callback, completed_tables, tasks, table_stats, progress)
