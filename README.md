# Ontario Mining Claims Lakehouse (Microsoft Fabric)

This project builds an end-to-end Microsoft Fabric lakehouse over Ontario's public mining-claims register:

- a metadata-driven Data Factory pipeline that loads the province's daily extracts,
- PySpark medallion layers with data-quality quarantine,
- a gold star schema that keeps claim status and ownership history with SCD Type 2,
- a Direct Lake on OneLake semantic model with dynamic RLS and OLS,
- an analytical report and a paginated report,
- Dev → Test deployment with fabric-cicd behind an approval gate.

**Status:** specification stage. See [SPEC.md](SPEC.md) for the design, the plan and the metrics that will be recorded.

## Stack

Microsoft Fabric (Data Factory pipelines, Lakehouse, PySpark notebooks, Direct Lake semantic models, Power BI, paginated reports), GitHub, Azure DevOps Pipelines, [fabric-cicd](https://microsoft.github.io/fabric-cicd/latest/).

## Results

Measured numbers will be added here from real runs (SPEC section 13). No estimates.

## Data and attribution

Source: Government of Ontario, OGSEarth / Mining Lands Administration System (MLAS) GIS data. © King's Printer for Ontario.

This repository contains **code and documentation only**. Source data is never committed; it is downloaded at run time under the publisher's terms of use. Nothing here is an authoritative record of mining tenure; check the official register before relying on any of it.
