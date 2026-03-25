import streamlit as st
import pandas as pd
from cleaner import AuditedDataCleaner

st.set_page_config(
    page_title="Audited Data Cleaner",
    page_icon="🧹",
    layout="wide"
)

# -----------------------------
# Header
# -----------------------------
st.title("🧹 Audited Data Cleaning Pipeline")
st.markdown(
    """
    Upload a raw CSV file to run an audited data cleaning pipeline,  
    preview the cleaned output, inspect audit logs, and download results.
    """
)

# -----------------------------
# Sidebar
# -----------------------------
with st.sidebar:
    st.header("Project Overview")
    st.write(
        """
        This app helps clean messy CSV data using a structured pipeline.

        **Features:**
        - Standardizes column names
        - Removes duplicate rows
        - Trims text values
        - Fills missing text fields
        - Generates audit log
        - Creates data quality report
        """
    )

    st.info("Upload a CSV file and click **Run Cleaning Pipeline**.")

# -----------------------------
# File Upload
# -----------------------------
uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

if uploaded_file is not None:
    try:
        # Read raw data
        df = pd.read_csv(uploaded_file, low_memory=False)

        st.success("File uploaded successfully.")

        # Basic raw file info
        st.subheader("Raw File Overview")
        info_col1, info_col2, info_col3 = st.columns(3)
        info_col1.metric("Rows", len(df))
        info_col2.metric("Columns", len(df.columns))
        info_col3.metric("Missing Values", int(df.isnull().sum().sum()))

        with st.expander("View Raw Column Names"):
            st.write(list(df.columns))

        st.subheader("Raw Data Preview")
        st.dataframe(df.head(), use_container_width=True)

        # Run button
        if st.button("Run Cleaning Pipeline", use_container_width=True):
            cleaner = AuditedDataCleaner(df)

            # Apply cleaning steps
            cleaner.standardize_column_names()
            cleaner.remove_duplicates()
            cleaner.trim_text_columns()
            cleaner.fill_missing_text()

            # Outputs
            cleaned_df = cleaner.get_cleaned_data()
            audit_log = cleaner.audit_log
            quality_report = cleaner.generate_quality_report()

            st.success("Cleaning pipeline completed successfully.")

            # -----------------------------
            # Summary Metrics
            # -----------------------------
            st.subheader("Before vs After Summary")

            raw_missing = int(df.isnull().sum().sum())
            cleaned_missing = int(cleaned_df.isnull().sum().sum())
            missing_reduced = raw_missing - cleaned_missing

            metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
            metric_col1.metric("Raw Rows", len(df))
            metric_col2.metric("Cleaned Rows", len(cleaned_df))
            metric_col3.metric("Raw Missing Values", raw_missing)
            metric_col4.metric("Missing Values Reduced", missing_reduced)

            # -----------------------------
            # Preview Tabs
            # -----------------------------
            st.subheader("Data Preview")

            tab1, tab2 = st.tabs(["Raw Data", "Cleaned Data"])

            with tab1:
                st.dataframe(df.head(10), use_container_width=True)

            with tab2:
                st.dataframe(cleaned_df.head(10), use_container_width=True)

            # -----------------------------
            # Audit Log
            # -----------------------------
            st.subheader("Audit Log")
            with st.expander("View Audit Log Details"):
                st.json(audit_log)

            # -----------------------------
            # Quality Report
            # -----------------------------
            st.subheader("Data Quality Report")
            st.dataframe(quality_report, use_container_width=True)

            # -----------------------------
            # Downloads
            # -----------------------------
            st.subheader("Download Outputs")

            cleaned_csv = cleaned_df.to_csv(index=False).encode("utf-8")
            report_csv = quality_report.to_csv(index=False).encode("utf-8")

            download_col1, download_col2 = st.columns(2)

            with download_col1:
                st.download_button(
                    label="Download Cleaned CSV",
                    data=cleaned_csv,
                    file_name="cleaned_data.csv",
                    mime="text/csv",
                    use_container_width=True
                )

            with download_col2:
                st.download_button(
                    label="Download Quality Report",
                    data=report_csv,
                    file_name="data_quality_report.csv",
                    mime="text/csv",
                    use_container_width=True
                )

    except Exception as e:
        st.error(f"Error: {e}")
else:
    st.warning("Upload a CSV file to begin.")