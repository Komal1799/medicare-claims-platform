-- =====================================================================
-- Silver: Part D prescription drug events. One row per fill.
-- Typed amounts, NDC drug code, and a cost-balance rule on every fill.
-- Coverage check (fill during a Part D enrollment month) is cross-table: see quality step.
-- =====================================================================

CREATE OR REFRESH STREAMING TABLE healthcare_dev.silver.rx_event (
  CONSTRAINT valid_pde_id            EXPECT (pde_id IS NOT NULL)                    ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_service_date      EXPECT (service_date IS NOT NULL),
  CONSTRAINT valid_ndc_format        EXPECT (ndc_code RLIKE '^[0-9]{11}$'),
  CONSTRAINT positive_days_supply    EXPECT (days_supply > 0),
  CONSTRAINT cost_components_balance EXPECT (
    total_cost_amt = patient_paid_amt + plan_covered_paid_amt + plan_noncovered_paid_amt
                     + low_income_subsidy_amt + other_troop_amt + patient_liability_reduction_amt
                     + gap_discount_amt
  )
)
COMMENT 'Part D prescription drug events, one row per fill. total_cost_amt is the full cost; plan_paid_amt is what the Part D plan paid.'
AS SELECT
  PDE_ID                                                   AS pde_id,
  BENE_ID                                                  AS bene_id,
  try_to_date(SRVC_DT, 'dd-MMM-yyyy')                      AS service_date,
  year(try_to_date(SRVC_DT, 'dd-MMM-yyyy'))                AS service_year,
  try_to_date(PD_DT, 'dd-MMM-yyyy')                        AS paid_date,
  PROD_SRVC_ID                                             AS ndc_code,
  PRSCRBR_ID                                               AS prescriber_id,
  PLAN_CNTRCT_REC_ID                                       AS plan_contract_id,
  BRND_GNRC_CD                                             AS brand_generic_code,
  try_cast(FILL_NUM          AS INT)                       AS fill_number,
  try_cast(QTY_DSPNSD_NUM    AS DECIMAL(12,3))             AS quantity_dispensed,
  try_cast(DAYS_SUPLY_NUM    AS INT)                       AS days_supply,
  try_cast(PTNT_PAY_AMT        AS DECIMAL(12,2))           AS patient_paid_amt,
  try_cast(CVRD_D_PLAN_PD_AMT  AS DECIMAL(12,2))           AS plan_covered_paid_amt,
  try_cast(NCVRD_PLAN_PD_AMT   AS DECIMAL(12,2))           AS plan_noncovered_paid_amt,
  coalesce(try_cast(CVRD_D_PLAN_PD_AMT AS DECIMAL(12,2)), 0)
    + coalesce(try_cast(NCVRD_PLAN_PD_AMT AS DECIMAL(12,2)), 0) AS plan_paid_amt,
  coalesce(try_cast(LICS_AMT            AS DECIMAL(12,2)), 0) AS low_income_subsidy_amt,
  coalesce(try_cast(OTHR_TROOP_AMT      AS DECIMAL(12,2)), 0) AS other_troop_amt,
  coalesce(try_cast(PLRO_AMT            AS DECIMAL(12,2)), 0) AS patient_liability_reduction_amt,
  coalesce(try_cast(RPTD_GAP_DSCNT_NUM  AS DECIMAL(12,2)), 0) AS gap_discount_amt,
  try_cast(TOT_RX_CST_AMT      AS DECIMAL(12,2))           AS total_cost_amt,
  _source_file,
  _ingest_ts
FROM STREAM(healthcare_dev.bronze.pde_raw);