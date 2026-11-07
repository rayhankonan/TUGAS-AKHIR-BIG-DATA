"""Reproducible retail analytics: Pandas/SQL -> Spark -> Parquet/PostgreSQL.

All money comparisons use an absolute Rp0.02 tolerance, never a relative tolerance.
The small lab export uses Arrow on the driver to avoid Windows Hadoop DLLs.
"""
from pathlib import Path
from functools import reduce
import argparse
import hashlib
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"
SOURCE = ROOT / "data/sales_synthetic_100k.csv"
COLS = ["transaction_id", "transaction_date", "branch", "category", "product_name",
        "quantity", "unit_price", "discount_rate", "gross_sales", "net_sales",
        "payment_method", "customer_segment"]
STRINGS = ["branch", "category", "product_name", "payment_method", "customer_segment"]

def save_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

def read_json(name):
    return json.loads((OUT / name).read_text(encoding="utf-8"))

def metadata():
    import pandas as pd
    import numpy as np
    import pyarrow
    import psutil
    return dict(executed_at_utc=datetime.now(timezone.utc).isoformat(),
                python=sys.version, platform=platform.platform(), cpu=platform.processor(),
                logical_cpus=os.cpu_count(), ram_gib=round(psutil.virtual_memory().total/(1024**3),2), pandas=pd.__version__, numpy=np.__version__,
                pyarrow=pyarrow.__version__, source_sha256=hashlib.sha256(SOURCE.read_bytes()).hexdigest())

def pandas_checks(df):
    import pandas as pd
    import numpy as np
    finite = np.isfinite(df[["quantity", "unit_price", "discount_rate", "gross_sales", "net_sales"]]).all().all()
    return {"rows_100000": len(df) == 100000,
            "at_least_10_columns": len(df.columns) >= 10,
            "unique_ids": df.transaction_id.is_unique,
            "no_nulls": not df.isna().any().any(),
            "valid_dates": pd.to_datetime(df.transaction_date, errors="coerce").notna().all(),
            "nonempty_dimensions": all(df[c].astype("string").str.strip().ne("").all() for c in STRINGS),
            "finite_numbers": finite,
            "positive_integer_quantity": ((df.quantity > 0) & (df.quantity % 1 == 0)).all(),
            "nonnegative_price": (df.unit_price >= 0).all(),
            "discount_range": df.discount_rate.between(0, 1).all(),
            "gross_formula": np.allclose(df.gross_sales, (df.quantity * df.unit_price).round(2), rtol=0, atol=.02),
            "net_formula": np.allclose(df.net_sales, (df.quantity * df.unit_price * (1-df.discount_rate)).round(2), rtol=0, atol=.02)}

def exploration():
    import pandas as pd
    import sqlite3
    df = pd.read_csv(SOURCE)
    checks = {k: bool(v) for k, v in pandas_checks(df).items()}
    save_json(OUT / "validation/pandas_validation.json", checks)
    assert all(checks.values()), checks
    profile = pd.DataFrame({"column": df.columns, "dtype": df.dtypes.astype(str).values,
                            "nulls": df.isna().sum().values, "distinct": df.nunique().values})
    (OUT / "profiling").mkdir(parents=True, exist_ok=True)
    profile.to_csv(OUT / "profiling/column_profile.csv", index=False)
    # SQLite is used for source profiling only; required warehouse uses PostgreSQL 15.
    with sqlite3.connect(":memory:") as conn:
        df.to_sql("raw_sales", conn, index=False)
        sql = """SELECT substr(transaction_date,1,7) AS month,
                 COUNT(*) AS transactions, SUM(quantity) AS units_sold,
                 ROUND(SUM(net_sales),2) AS net_sales
                 FROM raw_sales GROUP BY 1 ORDER BY 1"""
        monthly = pd.read_sql_query(sql, conn)
    monthly.to_csv(OUT / "profiling/sql_source_monthly.csv", index=False)
    (ROOT / "sql/source_profiling.sql").write_text(sql + ";\n", encoding="utf-8")
    save_json(OUT / "runtime.json", metadata())
    print(profile.to_string(index=False))
    print("Pandas checks:", checks)
    return df, profile

def spark_session():
    from pyspark.sql import SparkSession
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = (SparkSession.builder.master("local[2]").appName("RetailSalesAssignment")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.driver.memory", "2g")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def spark_transform(spark, source=SOURCE):
    from pyspark.sql import functions as F, types as T
    # Read every field as text so invalid casts remain visible for quarantine.
    raw = spark.read.option("header", True).schema(T.StructType([
        T.StructField(c, T.StringType(), True) for c in COLS])).csv(str(source))
    df = raw.withColumn("_source_integer_format", F.col("transaction_id").rlike(r"^[0-9]+$") & F.col("quantity").rlike(r"^[0-9]+$"))
    for c in STRINGS:
        df = df.withColumn(c, F.trim(F.regexp_replace(F.col(c), r"\s+", " ")))
    df = (df.withColumn("transaction_id", F.col("transaction_id").cast("long"))
          .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
          .withColumn("quantity", F.col("quantity").cast("decimal(14,2)")))
    for c in ["unit_price", "gross_sales", "net_sales"]:
        df = df.withColumn(c, F.col(c).cast("decimal(18,2)"))
    df = df.withColumn("discount_rate", F.col("discount_rate").cast("decimal(7,4)"))
    df = (df.withColumn("calculated_gross_sales", F.round(F.col("quantity") * F.col("unit_price"), 2))
          .withColumn("calculated_net_sales", F.round(F.col("calculated_gross_sales") * (1-F.col("discount_rate")), 2))
          .withColumn("discount_amount", F.col("calculated_gross_sales") - F.col("calculated_net_sales"))
          .withColumn("month", F.date_format("transaction_date", "yyyy-MM"))
          .withColumn("basket_band", F.when(F.col("calculated_net_sales") >= 1000000, "High")
                      .when(F.col("calculated_net_sales") >= 500000, "Medium").otherwise("Low")))
    required_bad = reduce(lambda a,b: a|b, [F.col(c).isNull() for c in COLS])
    blank = reduce(lambda a,b: a|b, [F.col(c) == "" for c in STRINGS])
    invalid = (required_bad | blank | ~F.col("_source_integer_format") | (F.col("transaction_id") <= 0) |
               (F.col("quantity") <= 0) | (F.col("quantity") != F.floor("quantity")) |
               (F.col("unit_price") < 0) | ~F.col("discount_rate").between(0, 1) |
               (F.abs(F.col("gross_sales")-F.col("calculated_gross_sales")) > .02) |
               (F.abs(F.col("net_sales")-F.col("calculated_net_sales")) > .02))
    duplicate_keys = df.groupBy("transaction_id").count().filter("count > 1").select("transaction_id")
    # Reject all occurrences of duplicate keys; do not select an arbitrary winner.
    duplicate_rows = df.join(duplicate_keys, "transaction_id", "inner")
    rejected = df.filter(invalid)
    accepted = (df.filter(~invalid).join(duplicate_keys, "transaction_id", "left_anti")
                .withColumn("quantity", F.col("quantity").cast("int")).drop("_source_integer_format"))
    return raw, accepted, rejected, duplicate_rows

def aggregates(df):
    from pyspark.sql import functions as F
    return {dim: df.groupBy(dim).agg(F.count("*").alias("transactions"),
            F.sum("quantity").alias("units_sold"),
            F.sum("calculated_net_sales").cast("decimal(20,2)").alias("net_sales"))
            .orderBy(dim) for dim in ["month", "branch", "category", "payment_method"]}

def spark_stage():
    import pandas as pd
    import numpy as np
    from pyspark.sql import functions as F
    spark = spark_session()
    try:
        raw, clean, rejected, duplicates = spark_transform(spark)
        clean = clean.cache()
        stats = {"source_rows": raw.count(), "accepted_rows": clean.count(),
                 "invalid_rows": rejected.count(), "duplicate_rows": duplicates.count()}
        assert stats == dict(source_rows=100000, accepted_rows=100000, invalid_rows=0, duplicate_rows=0), stats
        (OUT / "validation").mkdir(parents=True, exist_ok=True)
        rejected.toPandas().to_csv(OUT / "validation/quarantine_invalid.csv", index=False)
        duplicates.toPandas().to_csv(OUT / "validation/quarantine_duplicate.csv", index=False)
        clean.explain(mode="formatted")
        (OUT / "profiling/spark_physical_plan.txt").write_text(
            clean._jdf.queryExecution().toString(), encoding="utf-8")
        (OUT / "aggregates").mkdir(parents=True, exist_ok=True)
        tables = {}
        for dim, table in aggregates(clean).items():
            p = table.toPandas()
            p["net_sales"] = p.net_sales.astype(float)
            tables[dim] = p
            p.to_csv(OUT / f"aggregates/by_{dim}.csv", index=False)
        expected = pd.read_csv(ROOT / "data/expected_monthly_aggregate.csv")
        actual = tables["month"]
        ref_match = actual[["month", "transactions", "units_sold"]].equals(expected[["month", "transactions", "units_sold"]]) and np.allclose(actual.net_sales, expected.net_sales, rtol=0, atol=.02)
        sql_profile = pd.read_csv(OUT / "profiling/sql_source_monthly.csv")
        sql_match = actual[["month", "transactions", "units_sold"]].equals(sql_profile[["month", "transactions", "units_sold"]]) and np.allclose(actual.net_sales, sql_profile.net_sales, rtol=0, atol=.02)
        assert ref_match and sql_match, "Monthly reference / SQL mismatch"
        collected = clean.orderBy("transaction_id").toPandas()
        # Standard Parquet Snappy, written in chunks by Arrow on the lab driver.
        # Spark is still responsible for all business transformations and aggregations.
        t0 = time.perf_counter()
        dest = OUT / "parquet/sales_transactions"
        dest.mkdir(parents=True, exist_ok=True)
        collected.to_parquet(dest / "part-00000.parquet", index=False, compression="snappy")
        write_s = time.perf_counter()-t0
        again = pd.read_parquet(dest)
        parquet_same = collected.equals(again)
        assert parquet_same, "Parquet round-trip differs"
        # Also prove Spark can read the stored standard Parquet data.
        back = spark.read.parquet(str(dest / "part-00000.parquet"))
        spark_parquet_rows = back.count()
        assert spark_parquet_rows == stats["accepted_rows"]
        for dim, table in tables.items():
            table.to_parquet(OUT / f"parquet/by_{dim}.parquet", index=False, compression="snappy")
        kpi = {"transactions": len(collected), "units_sold": int(collected.quantity.sum()),
               "gross_sales": float(collected.calculated_gross_sales.sum()),
               "net_sales": float(collected.calculated_net_sales.sum()),
               "discount_amount": float(collected.discount_amount.sum()),
               "average_transaction": float(collected.calculated_net_sales.sum())/len(collected),
               "date_min": str(collected.transaction_date.min()), "date_max": str(collected.transaction_date.max())}
        save_json(OUT / "kpi.json", kpi)
        stats.update(reference_monthly_match=bool(ref_match), sql_profile_match=bool(sql_match),
                     parquet_exact_roundtrip=bool(parquet_same), spark_parquet_rows=spark_parquet_rows,
                     parquet_write_seconds=write_s, spark_version=spark.version,
                     transformations=["Cast dates and numeric types", "Normalize dimension whitespace",
                         "Calculate gross/net sales and discount", "Derive month and basket band",
                         "Quarantine invalid and duplicate records"])
        save_json(OUT / "validation/spark_validation.json", stats)
        charts(tables)
        print(json.dumps(stats, indent=2))
        print(kpi)
        return stats
    finally:
        spark.stop()

def charts(tables):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    folder = OUT / "charts"
    folder.mkdir(exist_ok=True)
    monthly = tables["month"]
    fig, ax = plt.subplots(figsize=(11,4.5))
    ax.plot(pd.to_datetime(monthly.month), monthly.net_sales/1e9, color="#176b87", marker="o", markersize=3)
    ax.set(title="Tren penjualan bersih bulanan | data sintetis", ylabel="Penjualan bersih (miliar Rp)", xlabel="Bulan")
    ax.grid(alpha=.2)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(folder / "01_tren_bulanan.png", dpi=160)
    plt.close(fig)
    fig, axes = plt.subplots(1,2,figsize=(12,5))
    for ax, dim, title in zip(axes, ["branch","category"], ["Kontribusi cabang", "Kontribusi kategori"]):
        t = tables[dim].sort_values("net_sales")
        ax.barh(t[dim], t.net_sales/1e9, color="#176b87" if dim=="branch" else "#c87533")
        ax.set(title=title+" | data sintetis", xlabel="Penjualan bersih (miliar Rp)")
        ax.grid(axis="x", alpha=.2)
    fig.tight_layout()
    fig.savefig(folder / "02_cabang_kategori.png", dpi=160)
    plt.close(fig)
    payment = tables["payment_method"].sort_values("transactions", ascending=False)
    fig, ax = plt.subplots(figsize=(8,4.5))
    bars = ax.bar(payment.payment_method, payment.transactions, color="#176b87")
    ax.bar_label(bars, labels=[f"{x:,}" for x in payment.transactions], padding=4)
    ax.set(title="Metode pembayaran | data sintetis", ylabel="Jumlah transaksi")
    ax.set_ylim(0,payment.transactions.max()*1.17)
    fig.tight_layout()
    fig.savefig(folder / "03_pembayaran.png", dpi=160)
    plt.close(fig)

def db_connection():
    from dotenv import load_dotenv
    import psycopg2
    load_dotenv(ROOT / ".env")
    return psycopg2.connect(host=os.environ.get("PGHOST", "127.0.0.1"),
        port=os.environ.get("PGPORT", "5432"), dbname=os.environ.get("PGDATABASE", "big_data_sales_lab"),
        user=os.environ.get("PGUSER", "postgres"), password=os.environ.get("PGPASSWORD", ""),
        sslmode=os.environ.get("PGSSLMODE", "prefer"), connect_timeout=10)

def postgres_stage():
    import pandas as pd
    from psycopg2.extras import execute_values
    data = pd.read_parquet(OUT / "parquet/sales_transactions")
    result = {}
    conn = db_connection()
    try:
        with conn.cursor() as cur:
            # DDL contains schema/table statements only; database creation is separate.
            cur.execute((ROOT / "sql/schema.sql").read_text(encoding="utf-8"))
            cur.execute("INSERT INTO sales_lab.etl_audit(job_name,started_at,input_rows,status) VALUES (%s,now(),%s,'RUNNING') RETURNING audit_id", ("verified_sales_etl",len(data)))
            audit = cur.fetchone()[0]
        conn.commit()
        started = time.perf_counter()
        try:
            with conn.cursor() as cur:
                # Replace only this project's tables, atomically; rollback preserves the old data.
                cur.execute("TRUNCATE sales_lab.sales_transactions")
                records = list(data[COLS].itertuples(index=False, name=None))
                execute_values(cur, "INSERT INTO sales_lab.sales_transactions ("+", ".join(COLS)+") VALUES %s", records, page_size=5000)
                for dim in ["month", "branch", "category", "payment_method"]:
                    agg = pd.read_csv(OUT / f"aggregates/by_{dim}.csv")
                    cur.execute(f"CREATE TABLE IF NOT EXISTS sales_lab.by_{dim} ({dim} TEXT PRIMARY KEY, transactions BIGINT NOT NULL, units_sold BIGINT NOT NULL, net_sales NUMERIC(20,2) NOT NULL)")
                    cur.execute(f"TRUNCATE sales_lab.by_{dim}")
                    execute_values(cur, f"INSERT INTO sales_lab.by_{dim} VALUES %s", list(agg.itertuples(index=False,name=None)))
                cur.execute("SELECT version(), count(*), sum(quantity), sum(net_sales) FROM sales_lab.sales_transactions")
                version, rows, units, net = cur.fetchone()
                k = read_json("kpi.json")
                assert rows == k["transactions"] and units == k["units_sold"] and abs(net-Decimal(str(k["net_sales"]))) <= Decimal(".02")
                cur.execute("SELECT count(*) FROM sales_lab.sales_transactions WHERE abs(net_sales-round(quantity*unit_price*(1-discount_rate),2)) > 0.02")
                mismatch = cur.fetchone()[0]
                assert mismatch == 0
                # Compare every stored field (excluding load timestamp), not only totals.
                cur.execute("SELECT "+", ".join(COLS)+" FROM sales_lab.sales_transactions ORDER BY transaction_id")
                stored = cur.fetchall()
                expected = sorted(records, key=lambda r:r[0])
                assert stored == expected, "PostgreSQL field-by-field reconciliation failed"
                dimension_matches = {}
                for dim in ["month", "branch", "category", "payment_method"]:
                    expression = "to_char(transaction_date,'YYYY-MM')" if dim == "month" else dim
                    cur.execute(f"SELECT {expression}, count(*),sum(quantity),sum(net_sales) FROM sales_lab.sales_transactions GROUP BY 1 ORDER BY 1")
                    calculated = cur.fetchall()
                    pd.DataFrame(calculated, columns=[dim,"transactions","units_sold","net_sales"]).to_csv(OUT / f"validation/postgres_by_{dim}.csv", index=False)
                    cur.execute(f"SELECT * FROM sales_lab.by_{dim} ORDER BY 1")
                    dimension_matches[dim] = calculated == cur.fetchall()
                assert all(dimension_matches.values())
                result = dict(server_version=version, rows=rows, units_sold=units, net_sales=str(net),
                              formula_mismatch=mismatch, exact_source_fields_match=True,
                              aggregate_matches=dimension_matches, load_and_validation_seconds=time.perf_counter()-started,
                              audit_id=audit, status="SUCCESS")
                cur.execute("UPDATE sales_lab.etl_audit SET finished_at=clock_timestamp(),output_rows=%s,status='SUCCESS',details=%s WHERE audit_id=%s", (rows,json.dumps(result),audit))
            conn.commit()
        except Exception as exc:
            conn.rollback()
            with conn.cursor() as cur:
                cur.execute("UPDATE sales_lab.etl_audit SET finished_at=clock_timestamp(),status='FAILED',details=%s WHERE audit_id=%s", (type(exc).__name__,audit))
            conn.commit()
            raise
        with conn.cursor() as cur:
            cur.execute("SELECT audit_id,job_name,started_at,finished_at,input_rows,output_rows,status FROM sales_lab.etl_audit ORDER BY audit_id")
            audit_data = cur.fetchall()
            pd.DataFrame(audit_data, columns=[d.name for d in cur.description]).to_csv(OUT / "validation/postgres_audit.csv", index=False)
        save_json(OUT / "validation/postgres_validation.json", result)
        print(json.dumps(result, indent=2))
        return result
    finally:
        conn.close()

def benchmark(sizes=(10000,100000,1000000), repeats=3):
    import pandas as pd
    import numpy as np
    from pyspark.sql import functions as F
    folder = ROOT / "work/benchmark"
    folder.mkdir(parents=True, exist_ok=True)
    source = pd.read_csv(SOURCE)
    spark = spark_session()
    results=[]
    try:
        spark.range(1000).agg(F.sum("id")).collect()  # JVM warmup, excluded
        for size in sizes:
            scaled = pd.concat([source] * ((size+len(source)-1)//len(source)), ignore_index=True).iloc[:size].copy()
            scaled["transaction_id"] = np.arange(1,size+1)
            path = folder / f"sales_{size}.csv"
            scaled.to_csv(path,index=False)
            for rep in range(1,repeats+1):
                for engine in ["pandas", "spark"]:
                    times={}
                    t=time.perf_counter()
                    if engine=="pandas":
                        df=pd.read_csv(path)
                    else:
                        # Explicit schema; cache+count makes the CSV read an action.
                        df=spark.read.option("header",True).schema("transaction_id long, transaction_date string, branch string, category string, product_name string, quantity int, unit_price double, discount_rate double, gross_sales double, net_sales double, payment_method string, customer_segment string").csv(str(path)).cache()
                        assert df.count()==size
                    times["read_seconds"]=time.perf_counter()-t
                    t=time.perf_counter()
                    if engine=="pandas":
                        df["month"]=pd.to_datetime(df.transaction_date).dt.strftime("%Y-%m")
                        df["calculated_net_sales"]=(df.quantity*df.unit_price*(1-df.discount_rate)).round(2)
                        ag=df.groupby(["month","branch","category"],as_index=False).agg(net_sales=("calculated_net_sales","sum"))
                    else:
                        transformed=df.withColumn("month",F.date_format(F.to_date("transaction_date"),"yyyy-MM")).withColumn("calculated_net_sales",F.round(F.col("quantity")*F.col("unit_price")*(1-F.col("discount_rate")),2)).cache()
                        transformed.count()
                        ag=transformed.groupBy("month","branch","category").agg(F.sum("calculated_net_sales").alias("net_sales")).collect()
                    times["transform_aggregate_seconds"]=time.perf_counter()-t
                    t=time.perf_counter()
                    materialized=df if engine=="pandas" else transformed.toPandas()
                    times["driver_collect_seconds"]=time.perf_counter()-t
                    target=folder/f"{engine}_{size}.parquet"
                    t=time.perf_counter()
                    materialized.to_parquet(target,index=False,compression="snappy")
                    times["write_seconds"]=time.perf_counter()-t
                    t=time.perf_counter()
                    back=pd.read_parquet(target)
                    assert len(back)==size
                    assert abs(back.calculated_net_sales.sum()-materialized.calculated_net_sales.sum()) < .02
                    times["reread_seconds"]=time.perf_counter()-t
                    times["total_seconds"]=sum(times.values())
                    results.append(dict(engine=engine,rows=size,repeat=rep,**times))
                    print(results[-1], flush=True)
                    if engine=="spark":
                        df.unpersist(); transformed.unpersist()
            path.unlink()
        report=pd.DataFrame(results)
        report.to_csv(OUT/"performance_runs.csv",index=False)
        report.groupby(["engine","rows"],as_index=False).median(numeric_only=True).drop(columns="repeat").to_csv(OUT/"performance_median.csv",index=False)
        save_json(OUT/"performance_method.json",dict(repeats=repeats, sizes=list(sizes), spark_version=spark.version,
            memory_note="Spark driver 2 GiB; 2 local worker threads; shared Windows machine; filesystem cache not flushed.",
            scaling="10k first rows; 100k original; 1m repeated source rows with reassigned unique IDs. Scaling data is not new independent observations.",
            protocol="One warmup action excluded. Timed CSV read, date/net transformation plus month-branch-category aggregation, driver collection, Arrow Parquet write, Pandas reread. Spark cache/count/collect forces computation. Sequential runs; median of 3. Same storage writer for both engines. Small-data local lab, not cluster throughput.",
            runtime=metadata()))
    finally:
        spark.stop()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("stage",choices=["explore","spark","postgres","benchmark","all"])
    args=parser.parse_args()
    if args.stage in ["explore","all"]: exploration()
    if args.stage in ["spark","all"]: spark_stage()
    if args.stage in ["postgres","all"]: postgres_stage()
    if args.stage in ["benchmark","all"]: benchmark()

if __name__ == "__main__":
    main()
