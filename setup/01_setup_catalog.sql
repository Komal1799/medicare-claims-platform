-- =========================================================
-- 01_setup_catalog.sql
-- Creates the Unity Catalog structure for the Medicare claims platform.
-- Safe to rerun: every statement uses IF NOT EXISTS.
-- =========================================================

-- Catalog: one per environment (dev now, prod later via CI/CD)
CREATE CATALOG IF NOT EXISTS healthcare_dev
COMMENT 'Medicare claims platform, development environment';

USE CATALOG healthcare_dev;

-- Schemas: one per pipeline layer
CREATE SCHEMA IF NOT EXISTS landing COMMENT 'Raw files as received from CMS';
CREATE SCHEMA IF NOT EXISTS bronze  COMMENT 'Raw tables, strings only, replayable';
CREATE SCHEMA IF NOT EXISTS silver  COMMENT 'Typed, cleaned, conformed';
CREATE SCHEMA IF NOT EXISTS gold    COMMENT 'Star schema and data products, de-identified';
CREATE SCHEMA IF NOT EXISTS metrics COMMENT 'Marts feeding dashboards';
CREATE SCHEMA IF NOT EXISTS ops     COMMENT 'Data quality, reconciliation, audit';

-- Volume: landing zone for raw CMS files
CREATE VOLUME IF NOT EXISTS healthcare_dev.landing.raw
COMMENT 'CMS synthetic RIF files, organized by batch';

-- Verification
SHOW SCHEMAS IN healthcare_dev;
SHOW VOLUMES IN healthcare_dev.landing;