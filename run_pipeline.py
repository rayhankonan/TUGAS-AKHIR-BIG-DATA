"""Run all required stages; PostgreSQL connection must be configured."""
from sales_project import exploration,spark_stage,postgres_stage,benchmark
if __name__ == "__main__":
    exploration()
    spark_stage()
    postgres_stage()
    benchmark()
