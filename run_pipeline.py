import pandas as pd
import os
from src.cleaner import AuditedDataCleaner


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RAW_PATH = os.path.join(BASE_DIR, "nyc_311_raw.csv")
CLEANED_PATH = os.path.join(BASE_DIR, "nyc_311_cleaned.csv")
AUDIT_PATH = os.path.join(BASE_DIR, "audit_log.json")
QUALITY_REPORT_PATH = os.path.join(BASE_DIR, "data_quality_report.csv")


def main():
    print("Loading raw data...")
    df = pd.read_csv(RAW_PATH, low_memory=False)
    print(f"Raw data shape: {df.shape}")

    cleaner = AuditedDataCleaner(df, source_name="nyc_311_service_requests")

    print("Standardizing column names...")
    cleaner.standardize_column_names()

    print("Removing duplicates...")
    cleaner.remove_duplicates()

    print("Converting date columns...")
    cleaner.convert_date_columns([
        "created_date",
        "closed_date",
        "resolution_action_updated_date"
    ])

    print("Trimming text columns...")
    cleaner.trim_text_columns()

    print("Standardizing text case...")
    cleaner.standardize_text_case([
        "agency",
        "complaint_type",
        "borough",
        "status",
        "city"
    ])

    print("Dropping high-null columns...")
    cleaner.drop_high_null_columns(threshold=0.70)

    print("Filling missing text fields...")
    cleaner.fill_missing_text(fill_value="Unknown")

    print("Saving cleaned data...")
    cleaner.save_cleaned_data(CLEANED_PATH)

    print("Saving audit log...")
    cleaner.save_audit_log(AUDIT_PATH)

    print("Generating quality report...")
    quality_report = cleaner.generate_quality_report()
    quality_report.to_csv(QUALITY_REPORT_PATH, index=False)

    print("\nPipeline completed successfully!")
    print(f"  Cleaned data  → {CLEANED_PATH}")
    print(f"  Audit log     → {AUDIT_PATH}")
    print(f"  Quality report→ {QUALITY_REPORT_PATH}")


if __name__ == "__main__":
    main()