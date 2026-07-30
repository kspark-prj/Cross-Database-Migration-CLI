import re

import sqlglot


class DialectConverter:
    def __init__(self, source_dialect: str, target_dialect: str):
        self.source = "postgres" if source_dialect == "postgresql" else source_dialect
        self.target = "postgres" if target_dialect == "postgresql" else target_dialect

    def translate_type(self, src_type: str) -> str:
        """Translates a single data type from source dialect to target dialect."""
        src_type_upper = src_type.upper().strip()

        if self.source == "oracle":
            if "VARCHAR2" in src_type_upper:
                return src_type_upper.replace("VARCHAR2", "VARCHAR")
            if "NUMBER" in src_type_upper:
                if src_type_upper == "NUMBER":
                    return "DOUBLE PRECISION" if self.target == "postgres" else "DOUBLE"
                match = re.match(r"NUMBER\((\d+)(?:,\s*(\d+))?\)", src_type_upper)
                if match:
                    p = int(match.group(1))
                    s = int(match.group(2)) if match.group(2) else 0
                    if s == 0:
                        if p <= 4:
                            return "SMALLINT"
                        elif p <= 9:
                            return "INTEGER"
                        else:
                            return "BIGINT"
                    else:
                        return (
                            f"NUMERIC({p},{s})"
                            if self.target == "postgres"
                            else f"DECIMAL({p},{s})"
                        )
            if src_type_upper == "DATE":
                return "TIMESTAMP" if self.target == "postgres" else "DATETIME"
            if src_type_upper == "CLOB":
                return "TEXT" if self.target == "postgres" else "LONGTEXT"
            if src_type_upper == "BLOB":
                return "BYTEA" if self.target == "postgres" else "LONGBLOB"

        if self.source == "mysql" and self.target == "postgres":
            if "TINYINT(1)" in src_type_upper or src_type_upper == "TINYINT":
                return "SMALLINT"
            if "DATETIME" in src_type_upper:
                return "TIMESTAMP"
            if any(b in src_type_upper for b in ["LONGBLOB", "MEDIUMBLOB", "BLOB"]):
                return "BYTEA"

        if self.source == "postgres" and self.target == "mysql":
            if src_type_upper == "BYTEA":
                return "LONGBLOB"
            if "JSONB" in src_type_upper or "JSON" in src_type_upper:
                return "JSON"
            if "UUID" in src_type_upper:
                return "VARCHAR(36)"
            if "TIMESTAMPTZ" in src_type_upper or "TIMESTAMP" in src_type_upper:
                return "DATETIME"

        try:
            exp = sqlglot.parse_one(src_type_upper, read=self.source)
            translated = exp.sql(dialect=self.target)
            return translated
        except Exception:
            return src_type

    def generate_skeleton_ddl(self, table_name: str, columns: list, primary_keys: list = None) -> str:
        """Generates DDL to create table with columns only, without PK/indexes/FKs."""
        col_defs = []
        for col in columns:
            name = col["name"]
            src_type = col["type"]

            target_type = self.translate_type(src_type)
            is_auto_inc = col.get("auto_increment", False)
            default_val = col.get("default")

            has_nextval = default_val is not None and "nextval" in str(default_val).lower()

            is_numeric_type = any(
                t in target_type.upper() for t in ["INT", "SERIAL", "NUMBER", "NUMERIC"]
            )
            if (is_auto_inc or has_nextval) and self.target == "postgres" and is_numeric_type:
                if "BIGINT" in target_type.upper() or "8" in target_type:
                    target_type = "BIGSERIAL"
                else:
                    target_type = "SERIAL"
                is_auto_inc = False
                default_val = None

            null_clause = "NULL" if col.get("nullable", True) else "NOT NULL"

            auto_inc_clause = ""
            if is_auto_inc and self.target == "mysql":
                auto_inc_clause = "AUTO_INCREMENT"

            default_clause = ""
            if default_val is not None:
                if (
                    self.target == "mysql"
                    and "now()" in str(default_val).lower()
                    or self.target == "postgres"
                    and "current_timestamp" in str(default_val).lower()
                ):
                    default_clause = "DEFAULT CURRENT_TIMESTAMP"
                else:
                    default_clause = f"DEFAULT {default_val}"

            col_def = (
                f'"{name}" {target_type} {null_clause} {default_clause} {auto_inc_clause}'.strip()
            )
            col_def = " ".join(col_def.split())
            col_defs.append(col_def)

        # PK는 데이터 적재 후 apply_post_load_ddl에서 생성
        # if primary_keys:
        #     pk_cols = ", ".join(f'"{ pk}"' for pk in primary_keys)
        #     col_defs.append(f"PRIMARY KEY ({pk_cols})")

        ddl = f'CREATE TABLE "{table_name}" (\n  ' + ",\n  ".join(col_defs) + "\n);"

        try:
            ddl = sqlglot.transpile(ddl, read=self.target, write=self.target)[0]
        except Exception:
            pass

        return ddl

    def generate_pk_ddls(self, table_name: str, primary_keys: list) -> list:
        """Generates ALTER TABLE ... ADD PRIMARY KEY DDL for post-load execution."""
        if not primary_keys:
            return []

        if self.target == "mysql":
            pk_cols = ", ".join(f"`{pk}`" for pk in primary_keys)
            ddl = f"ALTER TABLE `{table_name}` ADD PRIMARY KEY ({pk_cols});"
        else:
            pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)
            ddl = f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ({pk_cols});'

        return [ddl]

    def generate_index_ddls(self, table_name: str, indexes: list) -> list:
        ddls = []
        for idx in indexes:
            cols = idx.get("columns", [])
            if not cols:
                continue

            idx_name = idx.get("name")
            if not idx_name:
                clean_cols = "_".join(cols)
                idx_name = f"idx_{table_name}_{clean_cols}"

            if str(idx_name).upper() == "PRIMARY":
                continue

            is_unique = idx.get("unique", False)
            unique_clause = "UNIQUE " if is_unique else ""

            if self.target == "mysql":
                quoted_cols = ", ".join(f"`{col}`" for col in cols)
                ddl = f"CREATE {unique_clause}INDEX `{idx_name}` ON `{table_name}` ({quoted_cols});"
            else:
                quoted_cols = ", ".join(f'"{col}"' for col in cols)
                ddl = f'CREATE {unique_clause}INDEX IF NOT EXISTS "{idx_name}" ON "{table_name}" ({quoted_cols});'

            ddls.append(ddl)

        return ddls

    def generate_fk_ddls(self, table_name: str, foreign_keys: list) -> list:
        ddls = []
        for fk in foreign_keys:
            fk_name = fk["name"]
            cols = ", ".join(f'"{c}"' for c in fk["constrained_columns"])
            ref_table = fk["referred_table"]
            ref_cols = ", ".join(f'"{rc}"' for rc in fk["referred_columns"])

            ddl = (
                f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{fk_name}" '
                f'FOREIGN KEY ({cols}) REFERENCES "{ref_table}" ({ref_cols});'
            )
            try:
                ddl = sqlglot.transpile(ddl, read=self.target, write=self.target)[0]
            except Exception:
                pass
            ddls.append(ddl)
        return ddls
