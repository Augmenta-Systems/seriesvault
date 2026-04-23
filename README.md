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
```

*Dependencies: `polars>=0.19.0`, `pyarrow>=10.0.0`*

---

## 🚀 Quickstart

### 1. Writing Data

Initialize the store, write a scalar, and write a time-series using Polars or Pandas. SeriesVault handles the conversion automatically.

```python
import polars as pl
from seriesvault import ParquetStore 

# Initialize the database (reset=True clears existing data in the target folder)
db = ParquetStore("/path/to/history_db", reset=True, verbose=True)

# Write a Scalar (Instant cache & JSON save)
db["GDP_GROWTH_EST"] = 0.024

# Write a Time Series (Saves directly as TS1.parquet)
ts_data = pl.DataFrame({
    "Date": ["2024:Q1", "2024:Q2", "2024:Q3"],  
    "Value": [1.66, 1.67, 1.68]  
})
db["TS1"] = ts_data

print("Data successfully committed to disk.")
```

### 2. Reading Data

Read data using standard Python dictionary syntax.

```python
from seriesvault import ParquetStore

# Open the existing database
db = ParquetStore("/path/to/history_db")

print(f"Database loaded. Contains {len(db.keys())} items.")

# Retrieve a Scalar
if "GDP_GROWTH_EST" in db:
    print(f"Scalar Value: {db['GDP_GROWTH_EST']}")

# Retrieve a Time Series (Loads the Parquet file into a Polars DataFrame)
if "TS1" in db:
    print(db["TS1"].head())
    
# Safely get a value with a default fallback
missing_series = db.get("NON_EXISTENT_SERIES", default=None)
```

---

## 🤝 Collaboration & Sharing

Because SeriesVault outputs a directory of highly compressed Parquet files rather than a single database binary, sharing data with analysts and end-users is highly flexible.

### Option A: Shared Network Directory (Internal Teams)
If your end-users have access to a shared network drive or cloud mount (e.g., a mapped drive, Databricks DBFS, or shared Linux mount), they do not need to download anything. 

They simply point the `ParquetStore` reader to that exact directory path.

```python
db = ParquetStore("/shared/network/path/to/history_db")
```

### Option B: Zipped Archive (Local Workflows)
If end-users need to work locally on their own laptops, you can zip the output directory (e.g., `history_db.zip`) and distribute it.

**SeriesVault handles zip files automatically.** When initialized with a `.zip` path, it will silently extract the contents to an adjacent folder and load the data.

```python
# Points directly to the downloaded zip file
db = ParquetStore("./history_db.zip", verbose=True)
```

*Note: If a user modifies data loaded from a zip file, the changes are saved to the newly extracted local folder, not the original `.zip` archive.*

---

## ⚙️ Advanced Behavior

* **Graceful `None` Handling:** If a data pipeline passes a `None` value to the store, SeriesVault gracefully skips the assignment to prevent `TypeError` crashes.
* **Duck-Typing Pandas:** If you pass a Pandas DataFrame, SeriesVault will detect the `.to_parquet` method and automatically convert it to Polars for optimized storage.

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
```
