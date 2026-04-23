import pytest
import polars as pl
from seriesvault import ParquetStore

@pytest.fixture
def db(tmp_path):
    """Fixture to provide a clean ParquetStore instance for each test."""
    # tmp_path is a built-in pytest fixture that provides a temporary directory path
    return ParquetStore(db_path=tmp_path, reset=True)

def test_scalar_operations(db):
    """Test saving and retrieving scalar values."""
    db["gdp_growth"] = 0.024
    db["country"] = "Canada"
    db["is_active"] = True

    assert db["gdp_growth"] == 0.024
    assert db["country"] == "Canada"
    assert db["is_active"] is True
    
    # Verify persistence to JSON
    assert db.scalar_path.exists()
    assert "gdp_growth" in db.keys()

def test_polars_dataframe_operations(db):
    """Test writing and reading a Polars DataFrame."""
    df = pl.DataFrame({
        "Date": ["2024:Q1", "2024:Q2"],
        "Value": [1.5, 1.6]
    })
    
    db["TS1"] = df
    
    # Verify file exists on disk
    expected_file = db.series_path / "TS1.parquet"
    assert expected_file.exists()
    
    # Verify data integrity
    loaded_df = db["TS1"]
    assert loaded_df.shape == (2, 2)
    assert loaded_df["Value"][0] == 1.5

def test_graceful_none_handling(db):
    """Ensure passing None does not crash the pipeline."""
    db["empty_series"] = None
    assert "empty_series" not in db
    
    with pytest.raises(KeyError):
        _ = db["empty_series"]

def test_get_with_default(db):
    """Test the .get() fallback method."""
    db["exists"] = 100
    
    assert db.get("exists", default=0) == 100
    assert db.get("missing_key", default=-999) == -999

def test_unsupported_type_raises_error(db):
    """Ensure passing an unsupported object raises a TypeError."""
    class CustomObject:
        pass
    
    with pytest.raises(TypeError, match="Unsupported type"):
        db["bad_data"] = CustomObject()

def test_contains_magic_method(db):
    """Test the 'in' operator."""
    db["scalar_val"] = 1
    db["df_val"] = pl.DataFrame({"A": [1]})
    
    assert "scalar_val" in db
    assert "df_val" in db
    assert "missing_val" not in db
