-- Silver: cleaned, typed beneficiary enrollment. One row per beneficiary per year.
-- Reads incrementally from bronze; keeps only analytically useful columns.

CREATE OR REFRESH STREAMING TABLE healthcare_dev.silver.beneficiary_year (
  CONSTRAINT valid_bene_id             EXPECT (bene_id IS NOT NULL)        ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_birth_date          EXPECT (birth_date IS NOT NULL),
  CONSTRAINT year_matches_batch        EXPECT (enrollment_year = batch_year),
  CONSTRAINT part_b_months_consistent  EXPECT (part_b_months = part_b_months_from_monthly)
)
COMMENT 'Typed beneficiary enrollment, one row per beneficiary per year. Source: bronze.beneficiary_raw. Use part_b_months_from_monthly for Part B member months; the CMS annual field is inconsistent for some members.'
AS SELECT
  BENE_ID                                         AS bene_id,
  try_cast(BENE_ENROLLMT_REF_YR AS INT)           AS enrollment_year,
  STATE_CODE                                      AS state_code,
  COUNTY_CD                                       AS county_code,
  ZIP_CD                                          AS zip_code,
  try_to_date(BENE_BIRTH_DT, 'dd-MMM-yyyy')       AS birth_date,
  try_to_date(BENE_DEATH_DT, 'dd-MMM-yyyy')       AS death_date,
  try_cast(AGE_AT_END_REF_YR AS INT)              AS age_at_year_end,
  SEX_IDENT_CD                                    AS sex_code,
  ENTLMT_RSN_ORIG                                 AS entitlement_reason_orig,
  ENTLMT_RSN_CURR                                 AS entitlement_reason_curr,
  ESRD_IND                                        AS esrd_indicator,
  try_cast(BENE_HI_CVRAGE_TOT_MONS  AS INT)       AS part_a_months,
  try_cast(BENE_SMI_CVRAGE_TOT_MONS AS INT)       AS part_b_months,
  size(filter(
    array(MDCR_ENTLMT_BUYIN_IND_01, MDCR_ENTLMT_BUYIN_IND_02, MDCR_ENTLMT_BUYIN_IND_03,
          MDCR_ENTLMT_BUYIN_IND_04, MDCR_ENTLMT_BUYIN_IND_05, MDCR_ENTLMT_BUYIN_IND_06,
          MDCR_ENTLMT_BUYIN_IND_07, MDCR_ENTLMT_BUYIN_IND_08, MDCR_ENTLMT_BUYIN_IND_09,
          MDCR_ENTLMT_BUYIN_IND_10, MDCR_ENTLMT_BUYIN_IND_11, MDCR_ENTLMT_BUYIN_IND_12),
    x -> x IN ('2','3','B','C')
  ))                                              AS part_b_months_from_monthly,
  try_cast(PTD_PLAN_CVRG_MONS       AS INT)       AS part_d_months,
  try_cast(DUAL_ELGBL_MONS          AS INT)       AS dual_eligible_months,
  try_to_date(COVSTART, 'dd-MMM-yyyy')            AS coverage_start_date,
  try_cast(_batch_id AS INT)                      AS batch_year,
  _source_file,
  _ingest_ts
FROM STREAM(healthcare_dev.bronze.beneficiary_raw);


-- Silver: monthly enrollment. One row per beneficiary per month.
-- Unpivots the 12 monthly entitlement and Part D columns into rows.

CREATE OR REFRESH STREAMING TABLE healthcare_dev.silver.beneficiary_month (
  CONSTRAINT valid_month             EXPECT (month BETWEEN 1 AND 12)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT known_entitlement_code  EXPECT (entitlement_code IS NULL OR entitlement_code IN ('0','1','2','3','A','B','C'))
)
COMMENT 'Monthly Medicare and Part D enrollment, one row per beneficiary per month. Source: bronze.beneficiary_raw.'
AS SELECT
  bene_id,
  enrollment_year,
  month,
  make_date(enrollment_year, month, 1)                    AS month_start_date,
  entitlement_code,
  coalesce(entitlement_code IN ('1','3','A','C'), false)  AS has_part_a,
  coalesce(entitlement_code IN ('2','3','B','C'), false)  AS has_part_b,
  CASE WHEN partd_contract_id IS NULL
         OR partd_contract_id = ''
         OR left(partd_contract_id, 1) IN ('0','N','X')
       THEN NULL ELSE partd_contract_id END               AS partd_contract_id,
  NOT (partd_contract_id IS NULL
       OR partd_contract_id = ''
       OR left(partd_contract_id, 1) IN ('0','N','X'))    AS has_part_d,
  _source_file,
  _ingest_ts
FROM (
  SELECT
    BENE_ID                                AS bene_id,
    try_cast(BENE_ENROLLMT_REF_YR AS INT)  AS enrollment_year,
    _source_file,
    _ingest_ts,
    stack(12,
       1, MDCR_ENTLMT_BUYIN_IND_01, PTD_CNTRCT_ID_01,
       2, MDCR_ENTLMT_BUYIN_IND_02, PTD_CNTRCT_ID_02,
       3, MDCR_ENTLMT_BUYIN_IND_03, PTD_CNTRCT_ID_03,
       4, MDCR_ENTLMT_BUYIN_IND_04, PTD_CNTRCT_ID_04,
       5, MDCR_ENTLMT_BUYIN_IND_05, PTD_CNTRCT_ID_05,
       6, MDCR_ENTLMT_BUYIN_IND_06, PTD_CNTRCT_ID_06,
       7, MDCR_ENTLMT_BUYIN_IND_07, PTD_CNTRCT_ID_07,
       8, MDCR_ENTLMT_BUYIN_IND_08, PTD_CNTRCT_ID_08,
       9, MDCR_ENTLMT_BUYIN_IND_09, PTD_CNTRCT_ID_09,
      10, MDCR_ENTLMT_BUYIN_IND_10, PTD_CNTRCT_ID_10,
      11, MDCR_ENTLMT_BUYIN_IND_11, PTD_CNTRCT_ID_11,
      12, MDCR_ENTLMT_BUYIN_IND_12, PTD_CNTRCT_ID_12
    ) AS (month, entitlement_code, partd_contract_id)
  FROM STREAM(healthcare_dev.bronze.beneficiary_raw)
);