# Schema Drift Detection and Auto-Resolution Agent

## Overview

This project is a prototype agent that detects **schema drift** in Delta Lake tables and uses a **Large Language Model (LLM)** to generate code suggestions that resolve the drift. It’s built entirely in **Databricks** using **PySpark**, with the goal of minimizing ETL breakages due to schema changes.

## Key Features

- Detects schema changes (e.g., added/removed columns, data type changes).
- Generates structured schema diffs.
- Crafts LLM prompts from schema changes.
- Returns PySpark or SQL code suggestions to fix ETL jobs.
- Runs inside Databricks notebooks for easy testing and demonstration.

## Why It Matters

- Reduces manual ETL maintenance
- Minimizes downtime from schema-related failures
- Demonstrates practical use of AI in data engineering