-- =====================================================================
-- Silver: claim diagnoses. One row per distinct diagnosis per claim.
-- Built from the principal diagnosis plus diagnosis slots 1-12 (professional)
-- or 1-25 (institutional). Blanks and duplicates removed; principal flagged.
-- Principal is NOT always the same as slot 1, so it is added explicitly.
-- =====================================================================

CREATE OR REFRESH STREAMING TABLE healthcare_dev.silver.claim_diagnosis (
  claim_id            STRING,
  claim_type          STRING,
  bene_id             STRING,
  service_year        INT,
  diagnosis_position  INT,
  diagnosis_code      STRING,
  is_principal        BOOLEAN,
  _source_file        STRING,
  _ingest_ts          TIMESTAMP,
  CONSTRAINT valid_claim_id      EXPECT (claim_id IS NOT NULL)                         ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_icd10_format  EXPECT (diagnosis_code RLIKE '^[A-Z][0-9][0-9A-Z]{1,5}$')
)
COMMENT 'Claim diagnoses, one row per distinct diagnosis per claim. is_principal marks the claim''s principal diagnosis. ICD-10-CM codes without the decimal point.';


-- Diagnosis flow 1: carrier (12 slots)
CREATE FLOW carrier_to_claim_diagnosis
AS INSERT INTO healthcare_dev.silver.claim_diagnosis BY NAME
SELECT claim_id, claim_type, bene_id, service_year,
       pos + 1 AS diagnosis_position, dx AS diagnosis_code,
       coalesce(dx = principal, false) AS is_principal,
       _source_file, _ingest_ts
FROM (
  SELECT CLM_ID AS claim_id, 'carrier' AS claim_type, BENE_ID AS bene_id,
         year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')) AS service_year,
         trim(PRNCPAL_DGNS_CD) AS principal,
         array_distinct(filter(transform(array(
           PRNCPAL_DGNS_CD,
           ICD_DGNS_CD1, ICD_DGNS_CD2, ICD_DGNS_CD3, ICD_DGNS_CD4, ICD_DGNS_CD5, ICD_DGNS_CD6,
           ICD_DGNS_CD7, ICD_DGNS_CD8, ICD_DGNS_CD9, ICD_DGNS_CD10, ICD_DGNS_CD11, ICD_DGNS_CD12
         ), x -> trim(x)), x -> x IS NOT NULL AND x <> '')) AS dx_list,
         _source_file, _ingest_ts
  FROM STREAM(healthcare_dev.bronze.carrier_raw)
  WHERE LINE_NUM = '1'
)
LATERAL VIEW posexplode(dx_list) d AS pos, dx;


-- Diagnosis flow 2: dme (12 slots)
CREATE FLOW dme_to_claim_diagnosis
AS INSERT INTO healthcare_dev.silver.claim_diagnosis BY NAME
SELECT claim_id, claim_type, bene_id, service_year,
       pos + 1 AS diagnosis_position, dx AS diagnosis_code,
       coalesce(dx = principal, false) AS is_principal,
       _source_file, _ingest_ts
FROM (
  SELECT CLM_ID AS claim_id, 'dme' AS claim_type, BENE_ID AS bene_id,
         year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')) AS service_year,
         trim(PRNCPAL_DGNS_CD) AS principal,
         array_distinct(filter(transform(array(
           PRNCPAL_DGNS_CD,
           ICD_DGNS_CD1, ICD_DGNS_CD2, ICD_DGNS_CD3, ICD_DGNS_CD4, ICD_DGNS_CD5, ICD_DGNS_CD6,
           ICD_DGNS_CD7, ICD_DGNS_CD8, ICD_DGNS_CD9, ICD_DGNS_CD10, ICD_DGNS_CD11, ICD_DGNS_CD12
         ), x -> trim(x)), x -> x IS NOT NULL AND x <> '')) AS dx_list,
         _source_file, _ingest_ts
  FROM STREAM(healthcare_dev.bronze.dme_raw)
  WHERE LINE_NUM = '1'
)
LATERAL VIEW posexplode(dx_list) d AS pos, dx;


-- Diagnosis flow 3: inpatient (25 slots)
CREATE FLOW inpatient_to_claim_diagnosis
AS INSERT INTO healthcare_dev.silver.claim_diagnosis BY NAME
SELECT claim_id, claim_type, bene_id, service_year,
       pos + 1 AS diagnosis_position, dx AS diagnosis_code,
       coalesce(dx = principal, false) AS is_principal,
       _source_file, _ingest_ts
FROM (
  SELECT CLM_ID AS claim_id, 'inpatient' AS claim_type, BENE_ID AS bene_id,
         year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')) AS service_year,
         trim(PRNCPAL_DGNS_CD) AS principal,
         array_distinct(filter(transform(array(
           PRNCPAL_DGNS_CD,
           ICD_DGNS_CD1,  ICD_DGNS_CD2,  ICD_DGNS_CD3,  ICD_DGNS_CD4,  ICD_DGNS_CD5,
           ICD_DGNS_CD6,  ICD_DGNS_CD7,  ICD_DGNS_CD8,  ICD_DGNS_CD9,  ICD_DGNS_CD10,
           ICD_DGNS_CD11, ICD_DGNS_CD12, ICD_DGNS_CD13, ICD_DGNS_CD14, ICD_DGNS_CD15,
           ICD_DGNS_CD16, ICD_DGNS_CD17, ICD_DGNS_CD18, ICD_DGNS_CD19, ICD_DGNS_CD20,
           ICD_DGNS_CD21, ICD_DGNS_CD22, ICD_DGNS_CD23, ICD_DGNS_CD24, ICD_DGNS_CD25
         ), x -> trim(x)), x -> x IS NOT NULL AND x <> '')) AS dx_list,
         _source_file, _ingest_ts
  FROM STREAM(healthcare_dev.bronze.inpatient_raw)
  WHERE CLM_LINE_NUM = '1'
)
LATERAL VIEW posexplode(dx_list) d AS pos, dx;


-- Diagnosis flow 4: outpatient (25 slots)
CREATE FLOW outpatient_to_claim_diagnosis
AS INSERT INTO healthcare_dev.silver.claim_diagnosis BY NAME
SELECT claim_id, claim_type, bene_id, service_year,
       pos + 1 AS diagnosis_position, dx AS diagnosis_code,
       coalesce(dx = principal, false) AS is_principal,
       _source_file, _ingest_ts
FROM (
  SELECT CLM_ID AS claim_id, 'outpatient' AS claim_type, BENE_ID AS bene_id,
         year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')) AS service_year,
         trim(PRNCPAL_DGNS_CD) AS principal,
         array_distinct(filter(transform(array(
           PRNCPAL_DGNS_CD,
           ICD_DGNS_CD1,  ICD_DGNS_CD2,  ICD_DGNS_CD3,  ICD_DGNS_CD4,  ICD_DGNS_CD5,
           ICD_DGNS_CD6,  ICD_DGNS_CD7,  ICD_DGNS_CD8,  ICD_DGNS_CD9,  ICD_DGNS_CD10,
           ICD_DGNS_CD11, ICD_DGNS_CD12, ICD_DGNS_CD13, ICD_DGNS_CD14, ICD_DGNS_CD15,
           ICD_DGNS_CD16, ICD_DGNS_CD17, ICD_DGNS_CD18, ICD_DGNS_CD19, ICD_DGNS_CD20,
           ICD_DGNS_CD21, ICD_DGNS_CD22, ICD_DGNS_CD23, ICD_DGNS_CD24, ICD_DGNS_CD25
         ), x -> trim(x)), x -> x IS NOT NULL AND x <> '')) AS dx_list,
         _source_file, _ingest_ts
  FROM STREAM(healthcare_dev.bronze.outpatient_raw)
  WHERE CLM_LINE_NUM = '1'
)
LATERAL VIEW posexplode(dx_list) d AS pos, dx;


-- Diagnosis flow 5: snf (25 slots)
CREATE FLOW snf_to_claim_diagnosis
AS INSERT INTO healthcare_dev.silver.claim_diagnosis BY NAME
SELECT claim_id, claim_type, bene_id, service_year,
       pos + 1 AS diagnosis_position, dx AS diagnosis_code,
       coalesce(dx = principal, false) AS is_principal,
       _source_file, _ingest_ts
FROM (
  SELECT CLM_ID AS claim_id, 'snf' AS claim_type, BENE_ID AS bene_id,
         year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')) AS service_year,
         trim(PRNCPAL_DGNS_CD) AS principal,
         array_distinct(filter(transform(array(
           PRNCPAL_DGNS_CD,
           ICD_DGNS_CD1,  ICD_DGNS_CD2,  ICD_DGNS_CD3,  ICD_DGNS_CD4,  ICD_DGNS_CD5,
           ICD_DGNS_CD6,  ICD_DGNS_CD7,  ICD_DGNS_CD8,  ICD_DGNS_CD9,  ICD_DGNS_CD10,
           ICD_DGNS_CD11, ICD_DGNS_CD12, ICD_DGNS_CD13, ICD_DGNS_CD14, ICD_DGNS_CD15,
           ICD_DGNS_CD16, ICD_DGNS_CD17, ICD_DGNS_CD18, ICD_DGNS_CD19, ICD_DGNS_CD20,
           ICD_DGNS_CD21, ICD_DGNS_CD22, ICD_DGNS_CD23, ICD_DGNS_CD24, ICD_DGNS_CD25
         ), x -> trim(x)), x -> x IS NOT NULL AND x <> '')) AS dx_list,
         _source_file, _ingest_ts
  FROM STREAM(healthcare_dev.bronze.snf_raw)
  WHERE CLM_LINE_NUM = '1'
)
LATERAL VIEW posexplode(dx_list) d AS pos, dx;


-- Diagnosis flow 6: hha (25 slots)
CREATE FLOW hha_to_claim_diagnosis
AS INSERT INTO healthcare_dev.silver.claim_diagnosis BY NAME
SELECT claim_id, claim_type, bene_id, service_year,
       pos + 1 AS diagnosis_position, dx AS diagnosis_code,
       coalesce(dx = principal, false) AS is_principal,
       _source_file, _ingest_ts
FROM (
  SELECT CLM_ID AS claim_id, 'hha' AS claim_type, BENE_ID AS bene_id,
         year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')) AS service_year,
         trim(PRNCPAL_DGNS_CD) AS principal,
         array_distinct(filter(transform(array(
           PRNCPAL_DGNS_CD,
           ICD_DGNS_CD1,  ICD_DGNS_CD2,  ICD_DGNS_CD3,  ICD_DGNS_CD4,  ICD_DGNS_CD5,
           ICD_DGNS_CD6,  ICD_DGNS_CD7,  ICD_DGNS_CD8,  ICD_DGNS_CD9,  ICD_DGNS_CD10,
           ICD_DGNS_CD11, ICD_DGNS_CD12, ICD_DGNS_CD13, ICD_DGNS_CD14, ICD_DGNS_CD15,
           ICD_DGNS_CD16, ICD_DGNS_CD17, ICD_DGNS_CD18, ICD_DGNS_CD19, ICD_DGNS_CD20,
           ICD_DGNS_CD21, ICD_DGNS_CD22, ICD_DGNS_CD23, ICD_DGNS_CD24, ICD_DGNS_CD25
         ), x -> trim(x)), x -> x IS NOT NULL AND x <> '')) AS dx_list,
         _source_file, _ingest_ts
  FROM STREAM(healthcare_dev.bronze.hha_raw)
  WHERE CLM_LINE_NUM = '1'
)
LATERAL VIEW posexplode(dx_list) d AS pos, dx;


-- Diagnosis flow 7: hospice (25 slots)
CREATE FLOW hospice_to_claim_diagnosis
AS INSERT INTO healthcare_dev.silver.claim_diagnosis BY NAME
SELECT claim_id, claim_type, bene_id, service_year,
       pos + 1 AS diagnosis_position, dx AS diagnosis_code,
       coalesce(dx = principal, false) AS is_principal,
       _source_file, _ingest_ts
FROM (
  SELECT CLM_ID AS claim_id, 'hospice' AS claim_type, BENE_ID AS bene_id,
         year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')) AS service_year,
         trim(PRNCPAL_DGNS_CD) AS principal,
         array_distinct(filter(transform(array(
           PRNCPAL_DGNS_CD,
           ICD_DGNS_CD1,  ICD_DGNS_CD2,  ICD_DGNS_CD3,  ICD_DGNS_CD4,  ICD_DGNS_CD5,
           ICD_DGNS_CD6,  ICD_DGNS_CD7,  ICD_DGNS_CD8,  ICD_DGNS_CD9,  ICD_DGNS_CD10,
           ICD_DGNS_CD11, ICD_DGNS_CD12, ICD_DGNS_CD13, ICD_DGNS_CD14, ICD_DGNS_CD15,
           ICD_DGNS_CD16, ICD_DGNS_CD17, ICD_DGNS_CD18, ICD_DGNS_CD19, ICD_DGNS_CD20,
           ICD_DGNS_CD21, ICD_DGNS_CD22, ICD_DGNS_CD23, ICD_DGNS_CD24, ICD_DGNS_CD25
         ), x -> trim(x)), x -> x IS NOT NULL AND x <> '')) AS dx_list,
         _source_file, _ingest_ts
  FROM STREAM(healthcare_dev.bronze.hospice_raw)
  WHERE CLM_LINE_NUM = '1'
)
LATERAL VIEW posexplode(dx_list) d AS pos, dx;