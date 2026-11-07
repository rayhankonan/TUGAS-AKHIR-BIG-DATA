# Panduan Dosen dan Mahasiswa

## Studi kasus
Manajemen ingin mengetahui tren penjualan, performa cabang, kategori produk yang berkontribusi besar, dan pola pembayaran. Data dibuat secara sintetis.

## Tahapan praktikum
### Tahap 1 — Data understanding (Notebook 01)
- Periksa schema, tipe data, jumlah baris, nilai kosong, dan duplikasi.
- Hitung total penjualan bersih, unit terjual, dan rata-rata transaksi.
- Visualisasikan tren bulanan, penjualan per cabang, dan kategori.

### Tahap 2 — Transformasi PySpark (Notebook 02)
- Baca CSV dengan inferensi schema dimatikan bila ingin lebih terkontrol.
- Tetapkan tipe data eksplisit.
- Buat kolom `month` dan `calculated_net_sales`.
- Validasi jumlah baris, duplikasi ID, rentang diskon, dan formula.
- Agregasi menurut bulan, cabang, dan kategori.

### Tahap 3 — ETL (Notebook 03)
- Baca CSV dengan PySpark.
- Simpan dataset tertransformasi ke Parquet dengan kompresi Snappy.
- Baca kembali Parquet dan bandingkan jumlah baris/agregasi.
- Tulis ke PostgreSQL 15 lab sebagai storage wajib dan validasi hasilnya.
- Catat jumlah input/output dan status pada audit log bila role memiliki izin.

### Tahap 4 — Analisis dan presentasi
Jawab pertanyaan:
1. Bulan mana yang memiliki net sales tertinggi?
2. Cabang mana yang menghasilkan net sales terbesar?
3. Kategori mana yang menyumbang penjualan tertinggi?
4. Metode pembayaran mana yang paling sering digunakan?
5. Apa rekomendasi bisnis berdasarkan hasil?

## Rancangan arsitektur
CSV sintetis → PySpark ETL → validasi → Parquet data lake + PostgreSQL data mart → Matplotlib/dashboard.

## Rubrik penilaian (100%)
| Komponen | Bobot |
|---|---:|
| Rumusan masalah dan pemahaman dataset | 10% |
| SQL/Pandas dan data quality | 15% |
| Transformasi PySpark | 25% |
| ETL dan penyimpanan PostgreSQL/Parquet | 20% |
| Validasi dan uji performa | 10% |
| Visualisasi dan insight | 10% |
| Dokumentasi dan presentasi | 10% |

## Eksperimen performa
- Uji pada 10.000, 100.000, lalu (opsional) 1.000.000 baris.
- Ukur waktu baca CSV, transformasi, tulis Parquet, dan baca ulang.
- Jalankan beberapa kali; catat median, jangan hanya membandingkan satu run.
- Bandingkan Pandas dan PySpark secara hati-hati. Untuk 100k baris, Pandas dapat lebih cepat karena overhead Spark; tujuan PySpark adalah belajar pola pemrosesan yang dapat diskalakan.
- Catat spesifikasi runtime Colab karena CPU/RAM bersifat dinamis.

## Catatan keamanan
- Jangan menaruh password di notebook.
- Jangan membuka port PostgreSQL 5432 ke internet secara luas.
- Gunakan database praktikum, role minimal, TLS/VPN atau JupyterHub privat yang dikelola institusi.
- Gunakan data sintetis/anonymized, bukan data sensitif produksi.
