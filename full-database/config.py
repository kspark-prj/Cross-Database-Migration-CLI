import argparse
import multiprocessing
import os
from urllib.parse import parse_qs, urlparse


class DBConfig:
    def __init__(self, uri: str):
        self.raw_uri = uri

        parsed = urlparse(uri)
        dialect = parsed.scheme.split("+")[0].lower()
        if dialect not in ("postgresql", "postgres", "mysql", "oracle"):
            raise ValueError(
                f"Unsupported database dialect: {parsed.scheme}. Supported: mysql, postgresql, oracle"
            )

        self.dialect = "postgresql" if dialect == "postgres" else dialect
        self.username = parsed.username or ""
        self.password = parsed.password or ""
        self.host = parsed.hostname or "localhost"

        if parsed.port:
            self.port = parsed.port
        else:
            if self.dialect == "postgresql":
                self.port = 5432
            elif self.dialect == "mysql":
                self.port = 3306
            elif self.dialect == "oracle":
                self.port = 1521
            else:
                self.port = 0

        path = parsed.path.lstrip("/")
        self.database = path
        self.query_params = parse_qs(parsed.query)

    def get_sqlalchemy_url(self) -> str:
        """Returns SQLAlchemy-compatible connection URL."""
        if self.dialect == "oracle":
            return f"oracle+oracledb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.dialect == "postgresql":
            return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.dialect == "mysql":
            return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        return self.raw_uri

    def get_connectorx_url(self) -> str:
        """Returns ConnectorX-compatible connection URL."""
        if self.dialect == "postgresql":
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.dialect == "mysql":
            return (
                f"mysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
            )
        elif self.dialect == "oracle":
            return (
                f"oracle://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
            )
        return self.raw_uri

    def get_duckdb_dsn(self) -> str:
        """Returns connection DSN string for DuckDB's postgres/mysql ATTACH command."""
        if self.dialect == "postgresql":
            return f"host={self.host} port={self.port} dbname={self.database} user={self.username} password={self.password}"
        elif self.dialect == "mysql":
            return f"host={self.host} port={self.port} database={self.database} user={self.username} password={self.password}"
        return ""


def parse_args():
    parser = argparse.ArgumentParser(
        description="VeloxDB: High-Performance Cross-DB Migration CLI Tool for 100GB+ Databases."
    )
    parser.add_argument(
        "--mode",
        choices=["extract", "load"],
        required=True,
        help="Migration mode: 'extract' or 'load'",
    )
    parser.add_argument(
        "--source-uri", help="Source DB Connection URI (required in 'extract' mode)."
    )
    parser.add_argument("--target-uri", help="Target DB Connection URI (required in 'load' mode).")
    parser.add_argument(
        "--output-dir",
        default="./migration_data",
        help="Output directory where DDL, metadata, Parquet files, and progress/checksum logs are stored.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100000,
        help="Number of rows per chunk for key range splitting or pagination (default: 100,000).",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=10,
        help="Delay in milliseconds between chunks (default: 10).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=max(1, multiprocessing.cpu_count() - 1),
        help="Maximum number of parallel worker threads/processes.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Enable resuming migration from last checkpoint.",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up temporary Parquet files after successful load.",
    )

    args = parser.parse_args()
    return args


def validate_args(args):
    if args.mode == "extract" and not args.source_uri:
        raise ValueError("--source-uri is required when --mode is 'extract'")
    if args.mode == "load" and not args.target_uri:
        raise ValueError("--target-uri is required when --mode is 'load'")
    return args
