from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sales_project import *
import pandas as pd
if __name__ == "__main__":
    assert all(pandas_checks(pd.read_csv(SOURCE)).values())
    s=read_json("validation/spark_validation.json")
    assert s["accepted_rows"]==100000 and s["parquet_exact_roundtrip"]
    p=read_json("validation/postgres_validation.json")
    assert p["status"]=="SUCCESS" and p["exact_source_fields_match"]
    assert len(list((OUT/"charts").glob("*.png")))>=3
    print("PASS: source, Spark, Parquet, PostgreSQL and chart outputs")
