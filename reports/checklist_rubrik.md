# Checklist terhadap tugas

| Persyaratan | Bukti |
|---|---|
| Kasus 01 retail; rumusan masalah | laporan_akhir.pdf halaman 1 |
| 100k baris, >=10 kolom | data/sales_synthetic_100k.csv; profiling dan validasi |
| Pandas dan SQL profiling | notebook 01; sql/source_profiling.sql; output/profiling |
| >=3 transformasi PySpark | sales_project.py:spark_transform; notebook 02 |
| >=3 quality checks | 12 checks Pandas; Spark gates; fixture rusak |
| >=3 agregasi, waktu + 2 dimensi bisnis | output/aggregates: month, branch, category, payment_method |
| Parquet dan PostgreSQL lab | notebook 03; output/parquet; postgres_validation.json |
| 3 notebook | notebooks/01_exploration.ipynb, 02_pyspark_transform.ipynb, 03_etl_validation.ipynb |
| Schema dan query SQL | sql/schema.sql; analysis_queries.sql |
| Laporan validasi | output/validation; laporan_akhir.pdf |
| Uji performa | performance_runs.csv; performance_median.csv; performance_method.json |
| 3 grafik dan >=3 insight numerik | output/charts; laporan_akhir.pdf halaman 3-5 |
| Masalah, metode, hasil, insight, batasan, rekomendasi | reports/laporan_akhir.pdf dan .md |
| Dokumentasi dan dukungan presentasi | README; MULAI_DI_SINI; outline_presentasi.md |

Nama/NIM/kelompok perlu diisi sebelum pengumpulan. Data sintetis dan tidak ada kredensial database pribadi. Folder PostgreSQL runtime adalah alat lab, bukan dump database kantor.
