-- Silver: cleaned, typed beneficiary enrollment. One row per beneficiary per year.
-- Reads incrementally from bronze; keeps only analytically useful columns.

CREATE OR REFRESH STREAMING TABLE healthcare_dev.silver.beneficiary_year (
  CONSTRAINT valid_bene_id         EXPECT (bene_id IS NOT NULL)        ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_birth_date      EXPECT (birth_date IS NOT NULL),
  CONSTRAINT year_matches_batch    EXPECT (enrollment_year = batch_year)
)
COMMENT 'Typed beneficiary enrollment, one row per beneficiary per year. Source: bronze.beneficiary_raw.'
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
  try_cast(PTD_PLAN_CVRG_MONS       AS INT)       AS part_d_months,
  try_cast(DUAL_ELGBL_MONS          AS INT)       AS dual_eligible_months,
  try_to_date(COVSTART, 'dd-MMM-yyyy')            AS coverage_start_date,
  try_cast(_batch_id AS INT)                      AS batch_year,
  _source_file,
  _ingest_ts
FROM STREAM(healthcare_dev.bronze.beneficiary_raw);