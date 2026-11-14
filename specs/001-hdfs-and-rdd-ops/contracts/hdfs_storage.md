# Component Contract: HDFS Storage Architecture

**Purpose**: Defines the three-layer storage hierarchy on Hadoop Distributed File System (HDFS).  
**Constitution Alignment**: Enforces Principle I (Three-Layer HDFS Storage Separation).

---

## 1. Directory Structure

```text
hdfs://localhost:9000/
├── raw/                      # Immutable source ingestion tier
│   ├── halfhourly_dataset/   # Raw CSV blocks (block_0.csv ... block_N.csv)
│   ├── informations_households.csv
│   └── weather_hourly_darksky.csv
│
├── cleaned/                  # Spec 02: Validated, outlier-free Parquet files
│   └── (populated in Spec 02)
│
└── analytics/                # Spec 03: Final Spark SQL aggregated tables
    └── (populated in Spec 03)
```

## 2. Invariants & Storage Rules

1. **Immutability of `/raw`**:
   - Files uploaded to `/raw` must never be altered, renamed, or overwritten by processing jobs.
   - Any job reading from `/raw` treats the data as read-only.
2. **Replication Factor**:
   - For Google Colab single-node execution, replication is set to `dfs.replication = 1`.
3. **Format Separation**:
   - `/raw`: Strict CSV format.
   - `/cleaned`: Parquet with Snappy compression (enforced in Spec 02).
   - `/analytics`: Parquet / CSV summary extracts bounded to analytical reporting (enforced in Spec 03).
