# Medicare Claims Data Platform

A governed lakehouse on Databricks that turns raw CMS Medicare claims extracts into trusted, documented data products for cost, utilization, and quality analytics.

## Problem

Health plans receive Medicare data from CMS in Research Identifiable File (RIF) format: yearly enrollment files plus claims across seven care settings and Part D pharmacy. The raw files are wide, line-level, and coded, and they contain quality issues that must be caught before reporting. This project builds a pipeline that ingests them incrementally, validates and reconciles them, models them into a star schema, and protects PHI.

## Tech stack

Databricks · Delta Lake · Unity Catalog · Lakeflow Declarative Pipelines · PySpark · Spark SQL · Lakeflow Jobs · Databricks Asset Bundles · GitHub Actions · AI/BI Dashboards

## Data

[CMS Synthetic Medicare Enrollment, Fee-for-Service Claims, and Prescription Drug Event PUFs](https://data.cms.gov/collection/synthetic-medicare-enrollment-fee-for-service-claims-and-prescription-drug-event): synthetic data for about 10,000 beneficiaries, 2015 to 2023, in CMS RIF format. No real patient data is used.

Data files are not stored in this repo. Download them from the link above.

## Architecture

Medallion architecture: landing (raw files) → bronze (raw tables) → silver (cleaned, conformed) → gold (star schema) → metrics (dashboard marts).

*Diagram coming soon.*

## Repository structure

*Coming soon.*