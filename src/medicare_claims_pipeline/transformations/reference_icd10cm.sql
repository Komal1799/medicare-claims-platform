-- =====================================================================
-- Reference: ICD-10-CM diagnosis codes (CMS order file, fixed width).
-- Materialized views: the code list is replaced by new versions, not appended to.
-- Layout (per CMS spec): 1-5 order number, 7-13 code (no dots),
-- 15 billable flag (1 = valid for claims, 0 = header), 17-76 short desc, 78+ long desc.
-- =====================================================================

-- Bronze: one row per raw line, unparsed
CREATE OR REFRESH MATERIALIZED VIEW healthcare_dev.bronze.ref_icd10cm_raw
COMMENT 'Raw lines from CMS ICD-10-CM order files, one row per line. Version taken from folder name.'
AS SELECT
  value                                                     AS raw_line,
  regexp_extract(_metadata.file_path, '/(fy\\d{4})/', 1)    AS code_set_version,
  _metadata.file_path                                       AS _source_file
FROM read_files(
  '/Volumes/healthcare_dev/landing/reference/icd10cm/*/*.txt',
  format => 'text'
);


-- Silver: parsed codes with category description
CREATE OR REFRESH MATERIALIZED VIEW healthcare_dev.silver.ref_icd10cm (
  CONSTRAINT valid_code          EXPECT (icd10cm_code IS NOT NULL AND icd10cm_code <> '')  ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_billable_flag EXPECT (billable_flag IN ('0', '1')),
  CONSTRAINT has_description     EXPECT (long_description IS NOT NULL AND long_description <> '')
)
COMMENT 'ICD-10-CM codes, one row per code per version. is_billable = valid for claims. category_code is the 3-character parent.'
AS
WITH parsed AS (
  SELECT
    try_cast(substring(raw_line, 1, 5) AS INT)   AS order_number,
    trim(substring(raw_line, 7, 7))              AS icd10cm_code,
    substring(raw_line, 15, 1)                   AS billable_flag,
    trim(substring(raw_line, 17, 60))            AS short_description,
    trim(substring(raw_line, 78))                AS long_description,
    code_set_version,
    _source_file
  FROM healthcare_dev.bronze.ref_icd10cm_raw
)
SELECT
  p.icd10cm_code,
  p.billable_flag,
  p.billable_flag = '1'                          AS is_billable,
  p.short_description,
  p.long_description,
  left(p.icd10cm_code, 3)                        AS category_code,
  c.long_description                             AS category_description,
  p.order_number,
  p.code_set_version,
  p._source_file
FROM parsed p
LEFT JOIN parsed c
  ON  c.icd10cm_code     = left(p.icd10cm_code, 3)
  AND c.code_set_version = p.code_set_version;