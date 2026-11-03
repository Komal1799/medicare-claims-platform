-- Bronze: raw CMS beneficiary (enrollment) files, loaded incrementally by Auto Loader.
-- All columns kept as strings; no cleaning happens in bronze.

CREATE OR REFRESH STREAMING TABLE beneficiary_raw
COMMENT 'Raw CMS beneficiary files, one row per beneficiary per year. All columns are strings.'
AS SELECT
  *,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _file_modified_ts,
  current_timestamp()              AS _ingest_ts,
  regexp_extract(_metadata.file_path, 'batch_(\\d{4})', 1) AS _batch_id
FROM STREAM read_files(
  '/Volumes/healthcare_dev/landing/raw/beneficiary/',
  format => 'csv',
  header => true,
  sep => '|',
  inferColumnTypes => false
);