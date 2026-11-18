-- Silver: conformed claim headers. One row per claim, across all fee-for-service claim types.
-- Each claim type feeds this table through its own append flow.
-- Header fields repeat on every line in bronze, so each flow reads only line 1.

CREATE OR REFRESH STREAMING TABLE healthcare_dev.silver.claim_header (
  claim_id                  STRING,
  bene_id                   STRING,
  claim_type                STRING,
  claim_from_date           DATE,
  claim_thru_date           DATE,
  service_year              INT,
  admission_date            DATE,
  discharge_date            DATE,
  provider_number           STRING,
  billing_npi               STRING,
  attending_npi             STRING,
  drg_code                  STRING,
  principal_diagnosis_code  STRING,
  medicare_paid_amt         DECIMAL(12,2),
  primary_payer_paid_amt    DECIMAL(12,2),
  total_charge_amt          DECIMAL(12,2),
  utilization_days          INT,
  _source_file              STRING,
  _ingest_ts                TIMESTAMP,
  CONSTRAINT valid_claim_id        EXPECT (claim_id IS NOT NULL)          ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_from_date       EXPECT (claim_from_date IS NOT NULL),
  CONSTRAINT valid_date_range      EXPECT (claim_thru_date >= claim_from_date),
  CONSTRAINT non_negative_payment  EXPECT (medicare_paid_amt >= 0)
)
COMMENT 'Conformed claim headers, one row per claim, all fee-for-service claim types. Claim-level amounts live here, never summed from lines.';


-- Flow 1: carrier (physician) claims
CREATE FLOW carrier_to_claim_header
AS INSERT INTO healthcare_dev.silver.claim_header BY NAME
SELECT
  CLM_ID                                                   AS claim_id,
  BENE_ID                                                  AS bene_id,
  'carrier'                                                AS claim_type,
  try_to_date(CLM_FROM_DT, 'dd-MMM-yyyy')                  AS claim_from_date,
  try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')                  AS claim_thru_date,
  year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy'))            AS service_year,
  CAST(NULL AS DATE)                                       AS admission_date,
  CAST(NULL AS DATE)                                       AS discharge_date,
  CAST(NULL AS STRING)                                     AS provider_number,
  CARR_CLM_BLG_NPI_NUM                                     AS billing_npi,
  CAST(NULL AS STRING)                                     AS attending_npi,
  CAST(NULL AS STRING)                                     AS drg_code,
  PRNCPAL_DGNS_CD                                          AS principal_diagnosis_code,
  try_cast(CLM_PMT_AMT                 AS DECIMAL(12,2))   AS medicare_paid_amt,
  try_cast(CARR_CLM_PRMRY_PYR_PD_AMT   AS DECIMAL(12,2))   AS primary_payer_paid_amt,
  try_cast(NCH_CARR_CLM_SBMTD_CHRG_AMT AS DECIMAL(12,2))   AS total_charge_amt,
  CAST(NULL AS INT)                                        AS utilization_days,
  _source_file,
  _ingest_ts
FROM STREAM(healthcare_dev.bronze.carrier_raw)
WHERE LINE_NUM = '1';

-- Flow 2: inpatient hospital claims
CREATE FLOW inpatient_to_claim_header
AS INSERT INTO healthcare_dev.silver.claim_header BY NAME
SELECT
  CLM_ID AS claim_id, BENE_ID AS bene_id, 'inpatient' AS claim_type,
  try_to_date(CLM_FROM_DT, 'dd-MMM-yyyy')                AS claim_from_date,
  try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')                AS claim_thru_date,
  year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy'))          AS service_year,
  try_to_date(CLM_ADMSN_DT, 'dd-MMM-yyyy')               AS admission_date,
  try_to_date(NCH_BENE_DSCHRG_DT, 'dd-MMM-yyyy')         AS discharge_date,
  PRVDR_NUM AS provider_number, ORG_NPI_NUM AS billing_npi, AT_PHYSN_NPI AS attending_npi,
  CLM_DRG_CD AS drg_code, PRNCPAL_DGNS_CD AS principal_diagnosis_code,
  try_cast(CLM_PMT_AMT              AS DECIMAL(12,2))    AS medicare_paid_amt,
  try_cast(NCH_PRMRY_PYR_CLM_PD_AMT AS DECIMAL(12,2))    AS primary_payer_paid_amt,
  try_cast(CLM_TOT_CHRG_AMT         AS DECIMAL(12,2))    AS total_charge_amt,
  try_cast(CLM_UTLZTN_DAY_CNT       AS INT)              AS utilization_days,
  _source_file, _ingest_ts
FROM STREAM(healthcare_dev.bronze.inpatient_raw)
WHERE CLM_LINE_NUM = '1';


-- Flow 3: outpatient facility claims
CREATE FLOW outpatient_to_claim_header
AS INSERT INTO healthcare_dev.silver.claim_header BY NAME
SELECT
  CLM_ID AS claim_id, BENE_ID AS bene_id, 'outpatient' AS claim_type,
  try_to_date(CLM_FROM_DT, 'dd-MMM-yyyy')                AS claim_from_date,
  try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')                AS claim_thru_date,
  year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy'))          AS service_year,
  CAST(NULL AS DATE) AS admission_date, CAST(NULL AS DATE) AS discharge_date,
  PRVDR_NUM AS provider_number, ORG_NPI_NUM AS billing_npi, AT_PHYSN_NPI AS attending_npi,
  CAST(NULL AS STRING) AS drg_code, PRNCPAL_DGNS_CD AS principal_diagnosis_code,
  try_cast(CLM_PMT_AMT              AS DECIMAL(12,2))    AS medicare_paid_amt,
  try_cast(NCH_PRMRY_PYR_CLM_PD_AMT AS DECIMAL(12,2))    AS primary_payer_paid_amt,
  try_cast(CLM_TOT_CHRG_AMT         AS DECIMAL(12,2))    AS total_charge_amt,
  CAST(NULL AS INT) AS utilization_days,
  _source_file, _ingest_ts
FROM STREAM(healthcare_dev.bronze.outpatient_raw)
WHERE CLM_LINE_NUM = '1';


-- Flow 4: skilled nursing facility claims
CREATE FLOW snf_to_claim_header
AS INSERT INTO healthcare_dev.silver.claim_header BY NAME
SELECT
  CLM_ID AS claim_id, BENE_ID AS bene_id, 'snf' AS claim_type,
  try_to_date(CLM_FROM_DT, 'dd-MMM-yyyy')                AS claim_from_date,
  try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')                AS claim_thru_date,
  year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy'))          AS service_year,
  try_to_date(CLM_ADMSN_DT, 'dd-MMM-yyyy')               AS admission_date,
  try_to_date(NCH_BENE_DSCHRG_DT, 'dd-MMM-yyyy')         AS discharge_date,
  PRVDR_NUM AS provider_number, ORG_NPI_NUM AS billing_npi, AT_PHYSN_NPI AS attending_npi,
  CLM_DRG_CD AS drg_code, PRNCPAL_DGNS_CD AS principal_diagnosis_code,
  try_cast(CLM_PMT_AMT              AS DECIMAL(12,2))    AS medicare_paid_amt,
  try_cast(NCH_PRMRY_PYR_CLM_PD_AMT AS DECIMAL(12,2))    AS primary_payer_paid_amt,
  try_cast(CLM_TOT_CHRG_AMT         AS DECIMAL(12,2))    AS total_charge_amt,
  try_cast(CLM_UTLZTN_DAY_CNT       AS INT)              AS utilization_days,
  _source_file, _ingest_ts
FROM STREAM(healthcare_dev.bronze.snf_raw)
WHERE CLM_LINE_NUM = '1';


-- Flow 5: home health agency claims
CREATE FLOW hha_to_claim_header
AS INSERT INTO healthcare_dev.silver.claim_header BY NAME
SELECT
  CLM_ID AS claim_id, BENE_ID AS bene_id, 'hha' AS claim_type,
  try_to_date(CLM_FROM_DT, 'dd-MMM-yyyy')                AS claim_from_date,
  try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')                AS claim_thru_date,
  year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy'))          AS service_year,
  try_to_date(CLM_ADMSN_DT, 'dd-MMM-yyyy')               AS admission_date,
  CAST(NULL AS DATE) AS discharge_date,
  PRVDR_NUM AS provider_number, ORG_NPI_NUM AS billing_npi, AT_PHYSN_NPI AS attending_npi,
  CAST(NULL AS STRING) AS drg_code, PRNCPAL_DGNS_CD AS principal_diagnosis_code,
  try_cast(CLM_PMT_AMT              AS DECIMAL(12,2))    AS medicare_paid_amt,
  try_cast(NCH_PRMRY_PYR_CLM_PD_AMT AS DECIMAL(12,2))    AS primary_payer_paid_amt,
  try_cast(CLM_TOT_CHRG_AMT         AS DECIMAL(12,2))    AS total_charge_amt,
  CAST(NULL AS INT) AS utilization_days,
  _source_file, _ingest_ts
FROM STREAM(healthcare_dev.bronze.hha_raw)
WHERE CLM_LINE_NUM = '1';


-- Flow 6: hospice claims (hospice start date used as admission date)
CREATE FLOW hospice_to_claim_header
AS INSERT INTO healthcare_dev.silver.claim_header BY NAME
SELECT
  CLM_ID AS claim_id, BENE_ID AS bene_id, 'hospice' AS claim_type,
  try_to_date(CLM_FROM_DT, 'dd-MMM-yyyy')                AS claim_from_date,
  try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')                AS claim_thru_date,
  year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy'))          AS service_year,
  try_to_date(CLM_HOSPC_START_DT_ID, 'dd-MMM-yyyy')      AS admission_date,
  try_to_date(NCH_BENE_DSCHRG_DT, 'dd-MMM-yyyy')         AS discharge_date,
  PRVDR_NUM AS provider_number, ORG_NPI_NUM AS billing_npi, AT_PHYSN_NPI AS attending_npi,
  CAST(NULL AS STRING) AS drg_code, PRNCPAL_DGNS_CD AS principal_diagnosis_code,
  try_cast(CLM_PMT_AMT              AS DECIMAL(12,2))    AS medicare_paid_amt,
  try_cast(NCH_PRMRY_PYR_CLM_PD_AMT AS DECIMAL(12,2))    AS primary_payer_paid_amt,
  try_cast(CLM_TOT_CHRG_AMT         AS DECIMAL(12,2))    AS total_charge_amt,
  try_cast(CLM_UTLZTN_DAY_CNT       AS INT)              AS utilization_days,
  _source_file, _ingest_ts
FROM STREAM(healthcare_dev.bronze.hospice_raw)
WHERE CLM_LINE_NUM = '1';


-- Flow 7: durable medical equipment claims (professional layout, like carrier)
CREATE FLOW dme_to_claim_header
AS INSERT INTO healthcare_dev.silver.claim_header BY NAME
SELECT
  CLM_ID AS claim_id, BENE_ID AS bene_id, 'dme' AS claim_type,
  try_to_date(CLM_FROM_DT, 'dd-MMM-yyyy')                AS claim_from_date,
  try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy')                AS claim_thru_date,
  year(try_to_date(CLM_THRU_DT, 'dd-MMM-yyyy'))          AS service_year,
  CAST(NULL AS DATE) AS admission_date, CAST(NULL AS DATE) AS discharge_date,
  PRVDR_NUM AS provider_number, PRVDR_NPI AS billing_npi, CAST(NULL AS STRING) AS attending_npi,
  CAST(NULL AS STRING) AS drg_code, PRNCPAL_DGNS_CD AS principal_diagnosis_code,
  try_cast(CLM_PMT_AMT                 AS DECIMAL(12,2)) AS medicare_paid_amt,
  try_cast(CARR_CLM_PRMRY_PYR_PD_AMT   AS DECIMAL(12,2)) AS primary_payer_paid_amt,
  try_cast(NCH_CARR_CLM_SBMTD_CHRG_AMT AS DECIMAL(12,2)) AS total_charge_amt,
  CAST(NULL AS INT) AS utilization_days,
  _source_file, _ingest_ts
FROM STREAM(healthcare_dev.bronze.dme_raw)
WHERE LINE_NUM = '1';