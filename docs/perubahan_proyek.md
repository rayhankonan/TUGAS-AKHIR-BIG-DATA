# Perubahan dari repositori sumber

- Runner memakai transformasi PySpark dan PostgreSQL sebagai tahap wajib.
- Path notebook menjadi relatif terhadap proyek; instalasi package tidak dilakukan di setiap sel.
- Validasi mata uang memakai toleransi absolut, dengan pemeriksaan finite dan tanggal.
- Transformasi dimensi, metrik gross/net/diskon, month, dan basket band.
- Kebijakan karantina invalid dan semua kemunculan ID duplikat; diuji dengan input rusak.
- Empat agregasi disimpan sebagai CSV, Parquet, dan tabel PostgreSQL.
- Penyimpanan Parquet Windows menggunakan Arrow atas hasil Spark; batas collect driver dijelaskan.
- PostgreSQL load atomik dengan rollback, audit status, rekonsiliasi field serta agregasi.
- Tidak ada password hardcoded dalam loader; konfigurasi melalui environment atau .env.
- Benchmark 10k / 100k / 1m dengan tiga ulangan dan action Spark eksplisit.
- Tiga notebook dieksekusi; tiga grafik, laporan PDF/Markdown, checklist, dan outline presentasi dibuat dari hasil aktual.

Asal sumber dan hash dicatat pada SOURCE_PROVENANCE.json. Dataset sumber tidak diubah.
