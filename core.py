#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import zipfile
import shutil
import json
import os
from pathlib import Path
from typing import Union, Any, List, Dict

class ParquetStore:
    """
    A high-performance, disk-backed Key-Value store for time-series and scalar data.

    Designed for economists, researchers, and data engineers who need to share 
    local databases seamlessly. It solves the high-memory overhead of loading 
    massive single-file databases into Python by utilizing a hybrid storage model:
    
    1. Directory as a Database: Data is partitioned across a folder.
    2. Scalars in RAM: Small variables (floats, strings) are cached and backed by JSON.
    3. Time-Series on Disk: DataFrames are saved as individual Parquet files 
       and read on-the-fly (lazy loaded equivalent).
    4. Auto-Unzipping: Automatically handles distributed `.zip` archives.

    Attributes:
        db_path (Path): The active directory containing the database files.
        verbose (bool): Whether to print operational logs to stdout.
        series_path (Path): The subdirectory where Parquet files are stored.
        scalar_path (Path): The file path to the JSON scalar cache.
        scalars (Dict[str, Any]): In-memory cache of scalar values.
    """

    def __init__(self, db_path: Union[str, Path], reset: bool = False, verbose: bool = False):
        """
        Initializes the ParquetStore database connection.

        If the provided path is a .zip archive, it automatically extracts 
        it to an adjacent directory and updates the connection to point 
        to the extracted folder.

        Args:
            db_path (Union[str, Path]): Path to the database directory or .zip file.
            reset (bool, optional): If True, deletes the existing database directory 
                                    before initializing. Defaults to False.
            verbose (bool, optional): If True, prints status messages. Defaults to False.
        """
        self.db_path = Path(db_path)
        self.verbose = verbose

        # Auto-Unzip Logic: Extracts archive for local researcher use
        if self.db_path.suffix == ".zip" and self.db_path.exists():
            if self.verbose:
                print(f"Detected zip archive: {self.db_path}")
                print("Extracting... (this may take a moment)")
            
            # Define extraction path (remove .zip extension)
            extract_path = self.db_path.with_suffix('')
            
            with zipfile.ZipFile(self.db_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path.parent)
                
            # Update db_path to point to the extracted folder
            self.db_path = extract_path
            if self.verbose:
                print(f"Extracted to: {self.db_path}")

        self.series_path = self.db_path / "series"
        self.scalar_path = self.db_path / "scalars.json"
        
        # Initialize storage
        if reset and self.db_path.exists():
            shutil.rmtree(self.db_path)
        
        self.series_path.mkdir(parents=True, exist_ok=True)
        
        # Load Scalars into Memory
        self.scalars: Dict[str, Any] = {}
        if self.scalar_path.exists():
            with open(self.scalar_path, 'r', encoding='utf-8') as f:
                self.scalars = json.load(f)

    def __setitem__(self, key: str, value: Any):
        """
        Writes or updates an item in the database.

        Updates completely replace the underlying Parquet file or JSON entry.
        Gracefully handles `None` values by skipping the assignment to prevent
        pipeline crashes. Converts Pandas DataFrames to Polars automatically.

        Args:
            key (str): The identifier for the data.
            value (Any): A scalar (int, float, str, bool) or DataFrame (Polars/Pandas).

        Raises:
            TypeError: If the value type is unsupported or Pandas conversion fails.
        """
        # 1. Handle None (Gracefully Skip)
        if value is None:
            if self.verbose:
                print(f"Warning: Assignment to '{key}' skipped because value is None.")
            return

        # 2. Handle Scalars (Float, Int, Str, Bool)
        if isinstance(value, (int, float, str, bool)):
            self.scalars[key] = value
            self._save_scalars()
            
        # 3. Handle Polars DataFrames
        elif isinstance(value, pl.DataFrame):
            self._write_series(key, value)
            
        # 4. Handle Pandas DataFrames (Duck-typing conversion to Polars)
        elif hasattr(value, "to_parquet"): 
            try:
                self._write_series(key, pl.from_pandas(value))
            except Exception as e:
                raise TypeError(f"Failed to convert Pandas object for key '{key}': {e}")
            
        else:
            raise TypeError(f"Unsupported type for key '{key}': {type(value)}")

    def __getitem__(self, key: str) -> Union[float, int, str, bool, pl.DataFrame]:
        """
        Retrieves an item from the database.

        Scalars are served instantly from RAM. DataFrames are read directly
        from disk via Parquet.

        Args:
            key (str): The identifier of the data to retrieve.

        Returns:
            Union[float, int, str, bool, pl.DataFrame]: The requested data.

        Raises:
            KeyError: If the key does not exist in the scalar cache or on disk.
        """
        # 1. Check Scalars
        if key in self.scalars:
            return self.scalars[key]
        
        # 2. Check Series on Disk
        file_path = self.series_path / f"{key}.parquet"
        if file_path.exists():
            return pl.read_parquet(file_path)
        
        raise KeyError(f"Key '{key}' not found in Store.")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Safely retrieves an item, returning a default value if not found.

        Args:
            key (str): The identifier of the data.
            default (Any, optional): The value to return if key is missing. Defaults to None.

        Returns:
            Any: The requested data or the default value.
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str) -> bool:
        """
        Checks if a key exists within the database.

        Args:
            key (str): The identifier to check.

        Returns:
            bool: True if the key exists in scalars or as a Parquet file, False otherwise.
        """
        return (key in self.scalars) or (self.series_path / f"{key}.parquet").exists()

    def keys(self) -> List[str]:
        """
        Lists all available keys in the database.

        Returns:
            List[str]: A combined list of scalar keys and time-series file names.
        """
        series_keys = [p.stem for p in self.series_path.glob("*.parquet")]
        return list(self.scalars.keys()) + series_keys

    def _save_scalars(self):
        """
        Internal method to persist the in-memory scalar cache to disk.
        """
        with open(self.scalar_path, 'w', encoding='utf-8') as f:
            json.dump(self.scalars, f, indent=2)

    def _write_series(self, key: str, df: pl.DataFrame):
        """
        Internal method to write a Polars DataFrame to a Parquet file.

        Args:
            key (str): The file name (without extension).
            df (pl.DataFrame): The DataFrame to save.
        """
        file_path = self.series_path / f"{key}.parquet"
        df.write_parquet(file_path)
        if self.verbose:
            print(f"Saved series '{key}' to disk.")
