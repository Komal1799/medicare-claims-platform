-- =====================================================================
-- Reference: CMS CCW 30 Chronic Conditions (revised 08/2026).
-- Source PDF converted to CSV by tools/parse_ccw.py: one row per condition per code.
-- code_role: include = code counts toward the condition; exclude = disqualifies the claim.
-- =====================================================================

-- Bronze: the CSV as delivered, all strings
CREATE OR REFRESH MATERIALIZED VIEW healthcare_dev.bronze.ref_ccw_raw
COMMENT 'Raw CCW chronic condition code list, one row per condition per code. Version taken from folder name.'
AS SELECT
  *,
  regexp_extract(_metadata.file_path, '/ccw/([^/]+)/', 1)  AS ccw_version,
  _metadata.file_path                                       AS _source_file
FROM read_files(
  '/Volumes/healthcare_dev/landing/reference/ccw/*/*.csv',
  format => 'csv',
  header => true,
  sep => '|',
  inferColumnTypes => false
);


-- Silver: typed, with a short condition key for joins and flags
CREATE OR REFRESH MATERIALIZED VIEW healthcare_dev.silver.ref_ccw_condition_code (
  CONSTRAINT valid_code_role     EXPECT (code_role IN ('include', 'exclude'))           ON VIOLATION FAIL UPDATE,
  CONSTRAINT has_condition       EXPECT (condition_name IS NOT NULL AND condition_name <> ''),
  CONSTRAINT valid_icd10_format  EXPECT (icd10cm_code RLIKE '^[A-Z][0-9][0-9A-Z]{1,5}$')
)
COMMENT 'CCW chronic condition definitions, one row per condition per ICD-10-CM code (no dots). Join to silver.claim_diagnosis on icd10cm_code.'
AS SELECT
  condition_name,
  regexp_replace(regexp_replace(lower(condition_name), '[^a-z0-9]+', '_'), '^_|_$', '') AS condition_key,
  icd10cm_code,
  icd10cm_code_dotted,
  code_role,
  try_cast(reference_period_years AS INT)  AS reference_period_years,
  qualifying_claims_rule,
  ccw_version,
  _source_file
FROM healthcare_dev.bronze.ref_ccw_raw;