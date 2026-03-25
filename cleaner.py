import pandas as pd
import json
import os
from datetime import datetime


class AuditedDataCleaner:
    def __init__(self, df: pd.DataFrame, source_name: str = "nyc_311"):
        self.df = df.copy()
        self.source_name = source_name
        self.audit_log = []
        self.started_at = datetime.utcnow().isoformat()

    def log_action(self, step, column, action, details, rows_affected=None):
        self.audit_log.append({
            "timestamp": datetime.utcnow().isoformat(),
            "step": step,
            "column": column,
            "action": action,
            "details": details,
            "rows_affected": rows_affected
        })

    def standardize_column_names(self):
        old_columns = self.df.columns.tolist()
        self.df.columns = (
            self.df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("/", "_", regex=False)
            .str.replace("-", "_", regex=False)
        )
        new_columns = self.df.columns.tolist()
        self.log_action(
            step="standardize_column_names",
            column="all",
            action="renamed_columns",
            details={"old_columns": old_columns, "new_columns": new_columns},
            rows_affected=None
        )

    def remove_duplicates(self):
        before = len(self.df)
        self.df = self.df.drop_duplicates()
        after = len(self.df)
        removed = before - after
        self.log_action(
            step="remove_duplicates",
            column="all",
            action="dropped_duplicate_rows",
            details=f"Removed {removed} duplicate rows",
            rows_affected=removed
        )

    def convert_date_columns(self, columns):
        for col in columns:
            if col in self.df.columns:
                before_nulls = self.df[col].isna().sum()
                self.df[col] = pd.to_datetime(self.df[col], errors="coerce")
                after_nulls = self.df[col].isna().sum()
                coerced = int(after_nulls - before_nulls) # replace numerical null values with some name
                self.log_action(
                    step="convert_date_columns",
                    column=col,
                    action="converted_to_datetime",
                    details=f"Converted to datetime; {coerced} invalid values coerced to NaT",
                    rows_affected=coerced if coerced > 0 else 0
                )

    def trim_text_columns(self):
        text_cols = self.df.select_dtypes(include="object").columns
        for col in text_cols:
            original = self.df[col].copy()
            self.df[col] = self.df[col].str.strip()  # safe — preserves NaN, no "nan" corruption
            changed = (original.fillna("") != self.df[col].fillna("")).sum()
            self.log_action(
                step="trim_text_columns",
                column=col,
                action="trimmed_whitespace",
                details="Removed leading/trailing spaces; NaN values preserved",
                rows_affected=int(changed)
            )

    def standardize_text_case(self, columns):
        for col in columns:
            if col in self.df.columns:
                original = self.df[col].copy()
                self.df[col] = self.df[col].str.strip().str.title()
                changed = (original.fillna("") != self.df[col].fillna("")).sum()
                self.log_action(
                    step="standardize_text_case",
                    column=col,
                    action="converted_to_title_case",
                    details="Standardized text to title case; NaN preserved",
                    rows_affected=int(changed)
                )

    def drop_high_null_columns(self, threshold=0.6):
        null_ratio = self.df.isnull().mean()
        cols_to_drop = null_ratio[null_ratio > threshold].index.tolist()
        for col in cols_to_drop:
            self.log_action(
                step="drop_high_null_columns",
                column=col,
                action="dropped_column",
                details=f"Null ratio exceeded threshold of {threshold}",
                rows_affected=int(self.df[col].isnull().sum())
            )
        self.df = self.df.drop(columns=cols_to_drop)

    def fill_missing_text(self, fill_value="Unknown"):
        text_cols = self.df.select_dtypes(include="object").columns
        for col in text_cols:
            missing = self.df[col].isnull().sum()
            if missing > 0:
                self.df[col] = self.df[col].fillna(fill_value)
                self.log_action(
                    step="fill_missing_text",
                    column=col,
                    action="filled_missing_values",
                    details=f"Filled {missing} nulls with '{fill_value}'",
                    rows_affected=int(missing)
                )

    def generate_quality_report(self):
        return pd.DataFrame({
            "column": self.df.columns,
            "dtype": self.df.dtypes.astype(str).values,
            "missing_count": self.df.isnull().sum().values,
            "missing_pct": (self.df.isnull().mean() * 100).round(2).values,
            "unique_values": self.df.nunique(dropna=False).values
        })

    def save_audit_log(self, filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        output = {
            "source": self.source_name,
            "pipeline_run_at": self.started_at,
            "pipeline_finished_at": datetime.utcnow().isoformat(),
            "total_steps": len(self.audit_log),
            "operations": self.audit_log
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=4, default=str)

    def save_cleaned_data(self, filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.df.to_csv(filepath, index=False)

    def get_cleaned_data(self):
        return self.df