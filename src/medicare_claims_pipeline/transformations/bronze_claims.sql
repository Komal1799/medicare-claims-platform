-- Bronze: raw CMS claims and Part D files, loaded incrementally by Auto Loader.
-- One streaming table per source file type. All columns kept as strings.
-- Claim files have one row per claim LINE (header fields repeat on every line).

CREATE OR REFRESH STREAMING TABLE carrier_raw
COMMENT 'Raw CMS carrier (physician) claims, one row per claim line. All columns are strings.'
AS SELECT *,
  _metadata.file_path AS _source_file, _metadata.file_modification_time AS _file_modified_ts,
  current_timestamp() AS _ingest_ts, regexp_extract(_metadata.file_path, 'batch_(\\d{4})', 1) AS _batch_id
FROM STREAM read_files('/Volumes/healthcare_dev/landing/raw/carrier/',
  format => 'csv', header => true, sep => '|', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE inpatient_raw
COMMENT 'Raw CMS inpatient hospital claims, one row per claim line. All columns are strings.'
AS SELECT *,
  _metadata.file_path AS _source_file, _metadata.file_modification_time AS _file_modified_ts,
  current_timestamp() AS _ingest_ts, regexp_extract(_metadata.file_path, 'batch_(\\d{4})', 1) AS _batch_id
FROM STREAM read_files('/Volumes/healthcare_dev/landing/raw/inpatient/',
  format => 'csv', header => true, sep => '|', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE outpatient_raw
COMMENT 'Raw CMS outpatient claims, one row per claim line. All columns are strings.'
AS SELECT *,
  _metadata.file_path AS _source_file, _metadata.file_modification_time AS _file_modified_ts,
  current_timestamp() AS _ingest_ts, regexp_extract(_metadata.file_path, 'batch_(\\d{4})', 1) AS _batch_id
FROM STREAM read_files('/Volumes/healthcare_dev/landing/raw/outpatient/',
  format => 'csv', header => true, sep => '|', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE snf_raw
COMMENT 'Raw CMS skilled nursing facility claims, one row per claim line. All columns are strings.'
AS SELECT *,
  _metadata.file_path AS _source_file, _metadata.file_modification_time AS _file_modified_ts,
  current_timestamp() AS _ingest_ts, regexp_extract(_metadata.file_path, 'batch_(\\d{4})', 1) AS _batch_id
FROM STREAM read_files('/Volumes/healthcare_dev/landing/raw/snf/',
  format => 'csv', header => true, sep => '|', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE hha_raw
COMMENT 'Raw CMS home health agency claims, one row per claim line. All columns are strings.'
AS SELECT *,
  _metadata.file_path AS _source_file, _metadata.file_modification_time AS _file_modified_ts,
  current_timestamp() AS _ingest_ts, regexp_extract(_metadata.file_path, 'batch_(\\d{4})', 1) AS _batch_id
FROM STREAM read_files('/Volumes/healthcare_dev/landing/raw/hha/',
  format => 'csv', header => true, sep => '|', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE hospice_raw
COMMENT 'Raw CMS hospice claims, one row per claim line. All columns are strings.'
AS SELECT *,
  _metadata.file_path AS _source_file, _metadata.file_modification_time AS _file_modified_ts,
  current_timestamp() AS _ingest_ts, regexp_extract(_metadata.file_path, 'batch_(\\d{4})', 1) AS _batch_id
FROM STREAM read_files('/Volumes/healthcare_dev/landing/raw/hospice/',
  format => 'csv', header => true, sep => '|', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE dme_raw
COMMENT 'Raw CMS durable medical equipment claims, one row per claim line. All columns are strings.'
AS SELECT *,
  _metadata.file_path AS _source_file, _metadata.file_modification_time AS _file_modified_ts,
  current_timestamp() AS _ingest_ts, regexp_extract(_metadata.file_path, 'batch_(\\d{4})', 1) AS _batch_id
FROM STREAM read_files('/Volumes/healthcare_dev/landing/raw/dme/',
  format => 'csv', header => true, sep => '|', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE pde_raw
COMMENT 'Raw CMS Part D prescription drug events, one row per fill. All columns are strings.'
AS SELECT *,
  _metadata.file_path AS _source_file, _metadata.file_modification_time AS _file_modified_ts,
  current_timestamp() AS _ingest_ts, regexp_extract(_metadata.file_path, 'batch_(\\d{4})', 1) AS _batch_id
FROM STREAM read_files('/Volumes/healthcare_dev/landing/raw/pde/',
  format => 'csv', header => true, sep => '|', inferColumnTypes => false);