"""Load transformed Parquet and verify PostgreSQL, transactionally."""
from sales_project import postgres_stage
if __name__ == "__main__":
    postgres_stage()
