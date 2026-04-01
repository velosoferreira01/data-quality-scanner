
CREATE SCHEMA IF NOT EXISTS dq_history;

CREATE TABLE IF NOT EXISTS dq_history.dq_run (
    run_id VARCHAR,
    run_timestamp TIMESTAMP,
    source_schema VARCHAR,
    datasets_count BIGINT,
    columns_scanned BIGINT,
    overall_score DOUBLE,
    overall_classification VARCHAR,
    report_excel_path VARCHAR,
    report_html_path VARCHAR,
    radar_html_path VARCHAR,
    history_html_path VARCHAR
);

CREATE TABLE IF NOT EXISTS dq_history.dq_dataset_score_history (
    run_id VARCHAR,
    run_timestamp TIMESTAMP,
    dataset_name VARCHAR,
    source_type VARCHAR,
    source_ref VARCHAR,
    object_name VARCHAR,
    score_overall DOUBLE,
    classification_overall VARCHAR,
    score_completeness DOUBLE,
    score_consistency DOUBLE,
    score_uniqueness DOUBLE,
    score_validity DOUBLE,
    score_timeliness DOUBLE,
    score_integrity DOUBLE,
    priority_index DOUBLE,
    critical_columns_count BIGINT
);

CREATE TABLE IF NOT EXISTS dq_history.dq_dimension_score_history (
    run_id VARCHAR,
    run_timestamp TIMESTAMP,
    dataset_name VARCHAR,
    dimension_name VARCHAR,
    dimension_score DOUBLE
);

CREATE TABLE IF NOT EXISTS dq_history.dq_ai_recommendations (
    recommendation_id VARCHAR,
    run_id VARCHAR,
    run_timestamp TIMESTAMP,
    dataset_name VARCHAR,
    column_name VARCHAR,
    recommendation_level VARCHAR,
    recommendation_type VARCHAR,
    root_cause_hypothesis VARCHAR,
    recommendation_text VARCHAR,
    expected_benefit VARCHAR,
    estimated_effort VARCHAR,
    estimated_impact VARCHAR,
    priority_score DOUBLE,
    priority_band VARCHAR,
    owner_suggestion VARCHAR,
    implementation_hint VARCHAR
);
