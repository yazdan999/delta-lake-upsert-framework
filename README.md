# Delta Lake Upsert Framework (PySpark)

Production-ready Delta Lake upsert framework using PySpark, supporting:

- Hash-based change detection
- Null-safe merge conditions
- Insert / Update logic separation
- Config-driven behaviour
- Logging and observability integration
- Reusable modular design

---

## Overview

This repository demonstrates an enterprise-style implementation of a reusable Delta Lake upsert pattern.

The framework is designed for:

- Large-scale lakehouse environments
- CI/CD-driven deployments
- Reliable incremental data processing
- Controlled update detection using row-level hashing

---

## Key Features

- Delta `MERGE INTO` logic
- `eqNullSafe` merge conditions
- Row-hash comparison to detect updates
- Insert / update record counting
- Configurable key columns
- Optional surrogate key support
- Structured logging integration

---

## Repository Structure

```
delta-lake-upsert-framework/
│
├── framework/
│ ├── upsert_engine.py
│ ├── hash_utils.py
│ └── logging_utils.py
│
├── example/
│ └── sample_upsert_usage.py
│
└── tests/
└── test_upsert_logic.py
```

---

## Technology Stack

- Python (PySpark)
- Apache Spark
- Delta Lake
- SQL
- Git

---

## Engineering Design Highlights

- Null-safe merge using Spark `<=>`
- Hash-based update detection to minimise unnecessary writes
- Modular framework structure for reusability
- Separation of concerns (engine / hash / logging)
- CI/CD-ready repository layout

---

## Purpose

This project showcases production-grade Delta Lake merge logic and engineering discipline suitable for enterprise lakehouse implementations.
