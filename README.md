# SeriesVault: Disk-Backed Time-Series Store

**SeriesVault** is a high-performance, disk-backed Key-Value store designed specifically for researchers, economists, and data engineers who need to share and manage large local datasets.

## 🧠 The Approach

When collaborating on economic or time-series data, standard Python objects (especially massive DataFrames) have a high memory overhead. Loading an entire database into a standard Python dictionary can quickly cause segmentation faults.

**SeriesVault solves this via a hybrid storage model:**

1. **Directory as a Database:** A "database" is a structured directory containing partitioned files.
2. **Scalars in RAM:** Small, single-value variables (floats, strings, ints) are cached in memory and persisted to a single `scalars.json` file for instant access.
3. **Time-Series on Disk (Lazy Loading):** DataFrames are written directly to disk as individual `.parquet` files. When requested, they are read from disk on-the-fly.
4. **Update = Replace:** To avoid complex transaction logs, updating a series safely replaces the underlying Parquet file.

## 🛠️ Setup & Writing Data

```bash
pip install seriesvault
