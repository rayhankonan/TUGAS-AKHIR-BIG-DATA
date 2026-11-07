# Outline presentasi dan catatan bicara

1. Masalah: jawab tren penjualan, kontribusi cabang/kategori, dan metode pembayaran. Jelaskan data sintetis.

2. Dataset: 100.000 transaksi, 12 kolom. Jelaskan satuan transaksi dan rentang tanggal.

3. Arsitektur: Pandas/SQL -> PySpark -> Parquet/PostgreSQL -> validasi -> Matplotlib.

4. Transformasi: tipe data, dimensi, formula net, bulan/basket band, karantina. Jelaskan mengapa uang memakai Decimal.

5. Hasil: Puncak penjualan bulanan terjadi pada 2024-10, sebesar Rp 1.783.478.200,00 dari 3.158 transaksi. Makassar menghasilkan penjualan bersih terbesar, Rp 6.996.060.300,00, atau 12,75% dari total. Kategori Elektronik menyumbang Rp 17.545.252.850,00 atau 31,98% dari total pendapatan bersih. Metode Transfer digunakan pada 35.267 transaksi (35,27%).

6. Validasi: tampilkan jumlah baris, rekonsiliasi field dan agregat, serta uji 9 baris rusak.

7. Performa: tunjukkan tabel median 10k/100k/1m. Jelaskan action Spark, overhead, dan batas collect ke driver.

8. Rekomendasi dan keterbatasan: Gunakan Makassar sebagai titik awal kajian praktik operasional cabang. Bandingkan jumlah transaksi dan nilai rata-rata sebelum menyalin strategi; penjualan bukan laba. Prioritaskan tinjauan ketersediaan produk Elektronik karena kontribusinya besar. Keputusan pembelian stok perlu data margin, stok, lead time, dan permintaan yang belum tersedia. Pastikan kanal pembayaran Transfer tetap mudah digunakan dan dipantau. Evaluasi biaya, tingkat gagal, dan waktu settlement sebelum menentukan promosi pembayaran. Gunakan tren bulanan untuk merencanakan evaluasi kampanye. Jangan menyimpulkan pola musiman berulang dari satu bulan puncak atau membandingkan tahun parsial dengan tahun penuh.

Pertanyaan dosen yang perlu dikuasai: Mengapa Spark bisa lebih lambat? Apa beda gross/net/profit? Mengapa PostgreSQL dan Parquet dipakai bersama? Apa yang terjadi jika load gagal? Bagaimana menangani duplikat dan data kosong? Mengapa 1 juta baris hasil pengulangan tidak menambah informasi bisnis?