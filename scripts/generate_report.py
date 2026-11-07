from pathlib import Path
import json
import csv
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from html import escape
import sys

root=Path(__file__).resolve().parents[1]
out=root/'output'
reports=root/'reports'
reports.mkdir(exist_ok=True)
def load(p): return json.loads((out/p).read_text(encoding='utf-8'))
def rows(p):
    with (out/p).open(encoding='utf-8') as f:return list(csv.DictReader(f))
def rp(x):return 'Rp '+f'{float(x):,.2f}'.replace(',','X').replace('.',',').replace('X','.')
def num(x):return f'{int(x):,}'.replace(',','.')
def pct(x):return f'{x:.2f}%'.replace('.',',')
k=load('kpi.json'); s=load('validation/spark_validation.json'); db=load('validation/postgres_validation.json')
p=load('validation/pandas_validation.json'); tests=load('validation/quality_policy_test.json')
monthly=rows('aggregates/by_month.csv'); branch=rows('aggregates/by_branch.csv'); category=rows('aggregates/by_category.csv'); payment=rows('aggregates/by_payment_method.csv')
bestmonth=max(monthly,key=lambda x:float(x['net_sales']))
bestbranch=max(branch,key=lambda x:float(x['net_sales']))
bestcategory=max(category,key=lambda x:float(x['net_sales']))
bestpayment=max(payment,key=lambda x:int(x['transactions']))
bench=rows('performance_median.csv')
now=datetime.now(timezone(timedelta(hours=7))).strftime('%d-%m-%Y %H:%M WIB')
insights=[
f"Puncak penjualan bulanan terjadi pada {bestmonth['month']}, sebesar {rp(bestmonth['net_sales'])} dari {num(bestmonth['transactions'])} transaksi.",
f"{bestbranch['branch']} menghasilkan penjualan bersih terbesar, {rp(bestbranch['net_sales'])}, atau {pct(float(bestbranch['net_sales'])/k['net_sales']*100)} dari total.",
f"Kategori {bestcategory['category']} menyumbang {rp(bestcategory['net_sales'])} atau {pct(float(bestcategory['net_sales'])/k['net_sales']*100)} dari total pendapatan bersih.",
f"Metode {bestpayment['payment_method']} digunakan pada {num(bestpayment['transactions'])} transaksi ({pct(int(bestpayment['transactions'])/k['transactions']*100)})."
]
recommendations=[
f"Gunakan {bestbranch['branch']} sebagai titik awal kajian praktik operasional cabang. Bandingkan jumlah transaksi dan nilai rata-rata sebelum menyalin strategi; penjualan bukan laba.",
f"Prioritaskan tinjauan ketersediaan produk {bestcategory['category']} karena kontribusinya besar. Keputusan pembelian stok perlu data margin, stok, lead time, dan permintaan yang belum tersedia.",
f"Pastikan kanal pembayaran {bestpayment['payment_method']} tetap mudah digunakan dan dipantau. Evaluasi biaya, tingkat gagal, dan waktu settlement sebelum menentukan promosi pembayaran.",
"Gunakan tren bulanan untuk merencanakan evaluasi kampanye. Jangan menyimpulkan pola musiman berulang dari satu bulan puncak atau membandingkan tahun parsial dengan tahun penuh."
]
limitations=[
"Seluruh data sintetis: hasil menguji alur analitik dan tidak menggambarkan pasar retail Indonesia yang sebenarnya.",
f"Rentang observasi {k['date_min']} sampai {k['date_max']}; perbandingan tahun harus memperhatikan cakupan bulan yang berbeda.",
"Tidak tersedia harga pokok, margin, persediaan, target, atau identitas pelanggan. Penjualan tertinggi tidak berarti laba tertinggi dan penyebab perbedaan belum dapat dibuktikan.",
"Spark berjalan pada satu laptop dengan dua thread. Pengumpulan ke driver dan penulisan Arrow sesuai lab, tetapi membatasi skala memori. Produksi besar memerlukan penulisan Spark terdistribusi.",
"Uji 1 juta baris mengulang sumber dengan ID baru; tidak menambah keragaman bisnis. Benchmark mencakup cache OS yang tidak dibersihkan dan proses lain pada komputer."
]

validation_lines=[f"- {name}: {'PASS' if ok else 'FAIL'}" for name,ok in p.items()]
validation_lines += [f"- Spark: {s['source_rows']} input, {s['accepted_rows']} diterima, {s['invalid_rows']} invalid, {s['duplicate_rows']} duplikat.",
"- Referensi bulanan, profiling SQL sumber, serta baca ulang Parquet: PASS.",
f"- PostgreSQL: {db['rows']} baris; perbandingan setiap field sumber dan empat agregasi: PASS; formula mismatch: {db['formula_mismatch']}.",
f"- Uji input rusak: {tests['input_rows']} input, {tests['accepted_rows']} diterima, {tests['invalid_rows']} invalid, {tests['duplicate_rows']} duplikat; whitespace dinormalisasi dan selisih Rp1 ditolak."]
benchmark_md='| Engine | Baris | Baca CSV (s) | Transformasi (s) | Collect (s) | Tulis (s) | Baca ulang (s) | Total (s) |\n|---|---:|---:|---:|---:|---:|---:|---:|\n'
for b in bench:
    benchmark_md+='| '+b['engine']+' | '+num(b['rows'])+' | '+' | '.join(f"{float(b[c]):.3f}" for c in ['read_seconds','transform_aggregate_seconds','driver_collect_seconds','write_seconds','reread_seconds','total_seconds'])+' |\n'
report=f'''# Laporan Studi Kasus 01 - Retail Sales Analytics

Disusun dari eksekusi aktual pada {now}. Data sintetis, bukan data pribadi.

## Ringkasan

Proyek memproses {num(k['transactions'])} transaksi dengan {num(k['units_sold'])} unit dan penjualan bersih {rp(k['net_sales'])}. Pipeline menggabungkan Pandas/SQL, transformasi PySpark {s['spark_version']}, Parquet Snappy, PostgreSQL 15, validasi, serta tiga grafik Matplotlib.

## Rumusan masalah dan pemahaman data

1. Bagaimana tren penjualan bersih bulanan?
2. Cabang dan kategori mana yang memberi kontribusi penjualan terbesar?
3. Metode pembayaran mana yang paling sering digunakan?

Sumber: dataset sintetis dalam repositori ruhyana/big-data-sales-analytics, diunduh dari GitHub. Dataset memiliki 100.000 baris dan 12 kolom: transaction_id, transaction_date, branch, category, product_name, quantity, unit_price, discount_rate, gross_sales, net_sales, payment_method, customer_segment. Satu baris adalah satu catatan transaksi produk; rata-rata penjualan dihitung per catatan transaction_id, bukan per pelanggan unik. Rentang tanggal: {k['date_min']} sampai {k['date_max']}.

## Metode

CSV -> profiling Pandas dan SQL sumber (SQLite) -> transformasi PySpark -> Parquet Snappy dan PostgreSQL 15 -> rekonsiliasi -> grafik dan rekomendasi. PostgreSQL wajib dan telah diuji; SQLite hanya alat profiling sumber.

Transformasi: tipe tanggal dan angka eksplisit; normalisasi whitespace dimensi; gross = quantity x unit_price; net = gross x (1-discount_rate); discount_amount = gross-net; month; basket_band (High >= Rp1 juta, Medium >= Rp500 ribu, selain itu Low). Metrik uang memakai desimal di Spark dan NUMERIC di PostgreSQL. Toleransi perbandingan adalah absolut Rp0,02.

Baris dengan nilai wajib kosong/invalid, tanggal gagal parse, jumlah tidak positif/noninteger, harga negatif, diskon di luar 0-1, atau formula salah dikarantina. Semua kemunculan ID duplikat ditolak. Tidak dilakukan imputasi nilai uang. Sumber utama bersih; kebijakan diuji dengan fixture rusak yang terpisah.

Empat agregasi dihitung menurut bulan, cabang, kategori, serta metode pembayaran. Setiap tabel berisi jumlah transaksi, unit, dan penjualan bersih. Hasil transformasi dikumpulkan ke driver dan ditulis melalui Arrow, lalu dibaca ulang dengan Pandas dan Spark. Tabel PostgreSQL dimuat dalam satu transaksi; gagal validasi menyebabkan rollback. Audit mencatat status, waktu, dan jumlah baris.

## Hasil dan insight

- Gross sales: {rp(k['gross_sales'])}
- Diskon total: {rp(k['discount_amount'])}
- Net sales: {rp(k['net_sales'])}
- Rata-rata per transaksi: {rp(k['average_transaction'])}

'''+ '\n'.join(f'{i+1}. {v}' for i,v in enumerate(insights))+'''

![Tren bulanan](../output/charts/01_tren_bulanan.png)

![Cabang dan kategori](../output/charts/02_cabang_kategori.png)

![Pembayaran](../output/charts/03_pembayaran.png)

## Validasi

'''+ '\n'.join(validation_lines)+f'''

Server uji: {db['server_version']}. Bukti terstruktur terdapat di output/validation/. Tidak ada kredensial dalam laporan.

## Uji performa

Tiga ulangan untuk 10 ribu, 100 ribu, dan 1 juta baris; angka berikut adalah median. Spark memakai local[2] dan driver 2 GiB. Satu warmup JVM dikecualikan. Waktu baca CSV dipaksa dengan cache/count, transformasi mencakup agregasi bulan-cabang-kategori, lalu pengumpulan ke driver, penulisan Arrow Parquet, serta baca ulang Pandas. Kerja Spark dipicu melalui action, bukan hanya definisi transformasi. Engine dijalankan berurutan; cache OS tidak dibersihkan. Median total dihitung dari total per ulangan, sehingga tidak wajib sama dengan jumlah median setiap fase.

'''+benchmark_md+'''
Data 10k mengambil baris pertama, 100k adalah sumber asli, sedangkan 1 juta adalah pengulangan sumber dengan ID baru. Hasil ini menggambarkan biaya alur lab lokal; bukan bukti throughput cluster. Biaya scheduling, JVM, dan collect dapat membuat Spark lebih lambat pada data ini. Pilihan Spark dibenarkan oleh pembelajaran transformasi terdistribusi, bukan klaim kecepatan pada 100 ribu baris. Rincian semua ulangan dan spesifikasi tersimpan di output/performance_runs.csv, output/performance_method.json, dan output/runtime.json.

## Rekomendasi

'''+ '\n'.join(f'{i+1}. {v}' for i,v in enumerate(recommendations))+'''

## Keterbatasan

'''+ '\n'.join('- '+v for v in limitations)+'''

## Reproduksi dan berkas

Lihat README.md dan MULAI_DI_SINI.md. Jalankan tiga notebook berurutan, atau python run_pipeline.py setelah konfigurasi Java 17 dan PostgreSQL 15. Gunakan database lab khusus. Skrip load mengganti isi tabel sales_lab milik proyek. Identitas nama/NIM/kelompok perlu diisi sendiri sebelum pengumpulan.

Referensi: Tugas_studi_kasus_big_data.pdf, halaman 2-4 dan 14-17; https://github.com/ruhyana/big-data-sales-analytics . Commit sumber dicatat pada SOURCE_PROVENANCE.json. Formula dan insight dihitung ulang dari data, bukan disalin dari README lama.
'''
(reports/'laporan_akhir.md').write_text(report,encoding='utf-8')

from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,Image,PageBreak,KeepTogether
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet,ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
styles=getSampleStyleSheet()
styles.add(ParagraphStyle(name='TitleCustom',fontName='Helvetica-Bold',fontSize=27,leading=32,textColor=colors.HexColor('#123c50'),spaceAfter=16))
styles.add(ParagraphStyle(name='Sub',fontName='Helvetica-Bold',fontSize=15,leading=20,textColor=colors.HexColor('#176b87'),spaceAfter=11))
styles.add(ParagraphStyle(name='TextCustom',fontName='Helvetica',fontSize=10,leading=15,spaceAfter=9))
styles.add(ParagraphStyle(name='SmallCustom',fontName='Helvetica',fontSize=8,leading=11,spaceAfter=6))
story=[]
def para(t,style='TextCustom'):story.append(Paragraph(escape(str(t)),styles[style]))
def title(t):para(t,'Sub')
def bullet(t):para('- '+t)
def image(name,width=17*cm):
    p=out/'charts'/name
    from PIL import Image as PILImage
    with PILImage.open(p) as im:w,h=im.size
    story.append(Image(str(p),width=width,height=width*h/w))
def table(data,widths=None):
    t=Table([[Paragraph(escape(str(c)),styles['SmallCustom']) for c in row] for row in data],colWidths=widths,repeatRows=1,hAlign='LEFT')
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#dcebf0')),('VALIGN',(0,0),(-1,-1),'TOP'),('GRID',(0,0),(-1,-1),.3,colors.HexColor('#cbd4d9')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f5f8f9')]),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),5)]))
    story.append(t);story.append(Spacer(1,.3*cm))
def page():story.append(PageBreak())
para('STUDI KASUS 01 | BIG DATA ANALYTICS','Sub')
para('Retail Sales\nAnalytics'.replace('\n',' '),'TitleCustom')
para('Pandas & SQL / PySpark / Parquet / PostgreSQL 15 / Matplotlib','Sub')
para(f'Disusun dari eksekusi aktual: {now}')
story.append(Spacer(1,.6*cm))
title('Ringkasan proyek')
para(f'Pipeline memproses {num(k["transactions"])} transaksi sintetis, dengan {num(k["units_sold"])} unit dan penjualan bersih {rp(k["net_sales"])}. Semua tahap inti dijalankan dan hasil penyimpanan direkonsiliasi.')
table([['Indikator','Hasil'],['Baris / kolom sumber','100.000 / 12'],['Rentang tanggal',k['date_min']+' sampai '+k['date_max']],['Gross sales',rp(k['gross_sales'])],['Net sales',rp(k['net_sales'])],['Rata-rata per transaksi',rp(k['average_transaction'])]], [6*cm,11*cm])
title('Rumusan masalah')
for v in ['Bagaimana tren penjualan bersih bulanan?','Cabang dan kategori mana yang paling berkontribusi?','Metode pembayaran mana yang paling sering digunakan?']:bullet(v)
para('Data ini sintetis. Hasil merupakan demonstrasi analitik dan tidak mewakili kondisi pasar retail sebenarnya. Nama, NIM, dan anggota kelompok diisi pada berkas identitas sebelum dikumpulkan.','SmallCustom')
page()
title('01 / Data dan metode')
para('Sumber data: repositori ruhyana/big-data-sales-analytics. Satu baris merupakan satu catatan transaksi produk. ID transaksi unik; tidak tersedia ID pelanggan. Rata-rata dihitung per catatan transaksi, bukan per pelanggan.')
para('Alur: CSV -> profiling Pandas dan SQL sumber (SQLite) -> transformasi PySpark -> Parquet Snappy dan PostgreSQL 15 -> rekonsiliasi -> Matplotlib dan rekomendasi. SQLite hanya untuk profiling sumber; PostgreSQL tetap menjadi storage wajib.')
table([['Kelompok kolom','Isi'],['Identitas dan waktu','transaction_id, transaction_date'],['Dimensi bisnis','branch, category, product_name, payment_method, customer_segment'],['Kuantitas dan harga','quantity, unit_price, discount_rate'],['Nilai penjualan sumber','gross_sales, net_sales']], [5*cm,12*cm])
title('Transformasi dan metrik')
for v in ['Cast tanggal, ID, kuantitas, harga, dan diskon dengan skema eksplisit; normalisasi whitespace dimensi.',
          'Hitung gross = quantity x unit_price; net = gross x (1-discount_rate); discount_amount = gross-net.',
          'Turunkan month dan basket_band: High >= Rp1 juta; Medium >= Rp500 ribu; Low di bawah Rp500 ribu.',
          'Agregasikan transaksi, unit, dan net sales menurut bulan, cabang, kategori, serta metode pembayaran.']:bullet(v)
title('Penanganan nilai kosong dan invalid')
para('Baris wajib kosong/invalid, tanggal tidak valid, kuantitas tidak positif atau pecahan, harga negatif, diskon di luar 0-1, serta formula salah dikarantina. Semua kemunculan ID duplikat ditolak. Nilai uang tidak diimputasi. Sumber utama sudah bersih; fixture rusak menguji perilaku penolakan.')
para('Uang menggunakan Decimal di Spark dan NUMERIC di PostgreSQL. Semua rekonsiliasi uang memakai toleransi absolut Rp0,02. Tidak digunakan toleransi relatif.')
para('Pada Windows lab, hasil Spark dikumpulkan ke driver lalu ditulis melalui Arrow. Parquet dibaca ulang oleh Pandas dan Spark. Untuk skala produksi, gunakan penulisan Spark terdistribusi di Linux/cluster.','SmallCustom')
page()
title('02 / Tren penjualan bulanan')
para(insights[0])
image('01_tren_bulanan.png')
para(f'Total diskon pada periode pengamatan adalah {rp(k["discount_amount"])}. Penjualan bersih sudah memperhitungkan diskon, tetapi belum mengurangi harga pokok maupun biaya operasional.')
title('Interpretasi')
para('Puncak bulanan menunjukkan periode dengan nilai penjualan terbesar pada data yang diamati. Angka ini tidak membuktikan pengaruh kampanye, hari besar, atau penyebab tertentu karena variabel tersebut tidak tersedia.')
para(f'Rentang data {k["date_min"]} sampai {k["date_max"]}. Perbandingan tahunan wajib memakai cakupan bulan yang sebanding. Satu bulan puncak tidak cukup untuk menyatakan musim berulang.')
title('Implikasi')
para(recommendations[3])
page()
title('03 / Cabang dan kategori')
image('02_cabang_kategori.png')
para(insights[1]);para(insights[2])
title('Rekomendasi berbasis kontribusi')
para(recommendations[0]);para(recommendations[1])
para('Istilah teratas pada grafik mengacu pada nilai penjualan bersih. Peringkat berdasarkan jumlah unit atau laba dapat berbeda. Data margin belum tersedia, sehingga laporan tidak menyebut cabang atau kategori paling menguntungkan.')
page()
title('04 / Pembayaran dan keputusan bisnis')
image('03_pembayaran.png',16*cm)
para(insights[3]);para(recommendations[2])
title('Prioritas tindak lanjut')
table([['Prioritas','Tindakan','Data tambahan / ukuran keberhasilan'],['1','Tinjau stok kategori kontributor utama','Stockout rate, margin, lead time, penjualan hilang'],['2','Bandingkan praktik cabang','Jumlah transaksi, nilai rata-rata, biaya operasional'],['3','Pantau kanal pembayaran populer','Failure rate, biaya transaksi, settlement'],['4','Evaluasi kampanye per bulan','Kalender promo dan perbandingan periode setara']], [1.5*cm,6*cm,9.5*cm])
page()
title('05 / Validasi dan integritas storage')
table([['Pemeriksaan','Hasil'],*[[name,'PASS' if ok else 'FAIL'] for name,ok in p.items()]], [12*cm,5*cm])
para(f'Spark: {num(s["source_rows"])} input; {num(s["accepted_rows"])} diterima; {s["invalid_rows"]} invalid; {s["duplicate_rows"]} duplikat. Agregat bulanan cocok dengan data acuan dan SQL sumber.')
para(f'Parquet: round-trip seluruh field cocok; Spark membaca {num(s["spark_parquet_rows"])} baris. PostgreSQL: {num(db["rows"])} baris, field sumber cocok seluruhnya, empat agregasi cocok dengan perhitungan SQL, dan formula mismatch = {db["formula_mismatch"]}.')
para(f'Uji input rusak: {tests["input_rows"]} baris -> {tests["accepted_rows"]} diterima, {tests["invalid_rows"]} invalid, dan {tests["duplicate_rows"]} duplikat. Spasi dinormalisasi dan selisih Rp1 ditolak.')
para('Pemuatan PostgreSQL atomik: penggantian isi tabel proyek dan validasi dilakukan sebelum commit. Kegagalan menyebabkan rollback dan audit FAILED. Pengulangan tidak menambah duplikat. Bukti ada pada output/validation/.')
para('Versi server: '+db['server_version'],'SmallCustom')
page()
title('06 / Uji performa aktual')
para('Tiga ulangan per skala; tabel berisi median detik. Spark local[2], driver 2 GiB. Satu warmup JVM dikecualikan. Kedua engine memakai writer Arrow dan reader Pandas yang sama untuk storage.')
data=[['Engine / baris','CSV','Transform','Collect','Tulis','Baca ulang','Total']]
for b in bench:data.append([b['engine']+' / '+num(b['rows'])]+[f'{float(b[c]):.3f}' for c in ['read_seconds','transform_aggregate_seconds','driver_collect_seconds','write_seconds','reread_seconds','total_seconds']])
table(data,[3.7*cm,2*cm,2.6*cm,2*cm,2*cm,2.5*cm,2.2*cm])
para('Transformasi mencakup tanggal, net sales, dan agregasi bulan-cabang-kategori. Spark dipaksa melakukan count/collect agar waktu mencakup pekerjaan nyata. Collect ke driver diukur terpisah. Total adalah median total tiap ulangan dan dapat berbeda dari penjumlahan median fase.')
para('10k memakai baris pertama; 100k sumber asli; 1 juta mengulang sumber dengan ID baru. Data skala besar tidak menambah observasi bisnis independen. Cache OS tidak dibersihkan dan mesin dipakai bersama proses lain.')
title('Interpretasi performa')
para('Biaya JVM, scheduling, serialisasi, dan collect dapat mendominasi data kecil. Pengujian ini menilai alur lab pada satu komputer dan tidak membuktikan throughput cluster. Spark dipakai untuk mempelajari pola pemrosesan terdistribusi, bukan untuk menjamin lebih cepat pada 100 ribu baris.')
rt=load('runtime.json')
para('Runtime: '+rt['platform']+'; Python '+rt['python'].split()[0]+f'; CPU logis {rt["logical_cpus"]}; Spark '+s['spark_version']+'.','SmallCustom')
para('Data semua ulangan: output/performance_runs.csv. Protokol dan versi: output/performance_method.json. Waktu PostgreSQL dan audit disimpan terpisah dari benchmark analitik.','SmallCustom')
page()
title('07 / Keterbatasan dan reproduksi')
for v in limitations:bullet(v)
title('Cara menjalankan')
for v in ['Gunakan Python 3.12, Java 17, dan PostgreSQL 15. Pasang requirements.txt dalam virtual environment.',
          'Jalankan tools/start_lab.ps1 untuk database portabel di localhost, atau isi .env untuk database lab sendiri.',
          'Jalankan notebook 01_exploration, 02_pyspark_transform, dan 03_etl_validation secara berurutan.',
          'Alternatif: python run_pipeline.py. Kemudian python scripts/validate_outputs.py. Benchmark memiliki 3 ulangan pada 3 skala.',
          'Skrip ETL mengganti isi tabel sales_lab milik proyek. Gunakan database khusus, bukan database lain.']:bullet(v)
title('Berkas pengumpulan')
para('Notebook dengan output eksekusi, data sumber, SQL, agregasi CSV/Parquet, tiga grafik PNG, validasi JSON/CSV, benchmark, laporan PDF/Markdown, dan outline presentasi. Checklist persyaratan ada di reports/checklist_rubrik.md.')
title('Referensi dan asal data')
para('Tugas_studi_kasus_big_data.pdf, halaman 2-4 dan 14-17. Repositori: https://github.com/ruhyana/big-data-sales-analytics. Commit dan hash sumber tercatat pada SOURCE_PROVENANCE.json dan output/runtime.json. Hasil dihitung ulang dari data; tidak disalin dari ringkasan README awal.','SmallCustom')
def footer(canvas,doc):
    canvas.setStrokeColor(colors.HexColor('#cbd4d9'));canvas.line(1.8*cm,1.5*cm,A4[0]-1.8*cm,1.5*cm)
    canvas.setFont('Helvetica',8);canvas.setFillColor(colors.HexColor('#526570'))
    canvas.drawString(1.8*cm,1.05*cm,'RETAIL SALES ANALYTICS | Data sintetis')
    canvas.drawRightString(A4[0]-1.8*cm,1.05*cm,str(doc.page))
SimpleDocTemplate(str(reports/'laporan_akhir.pdf'),pagesize=A4,rightMargin=1.8*cm,leftMargin=1.8*cm,topMargin=1.6*cm,bottomMargin=1.9*cm,title='Retail Sales Analytics - Laporan Akhir',author='Big Data Analytics Project').build(story,onFirstPage=footer,onLaterPages=footer)

outline='# Outline presentasi dan catatan bicara\n\n'+ '\n\n'.join([
'1. Masalah: jawab tren penjualan, kontribusi cabang/kategori, dan metode pembayaran. Jelaskan data sintetis.',
'2. Dataset: 100.000 transaksi, 12 kolom. Jelaskan satuan transaksi dan rentang tanggal.',
'3. Arsitektur: Pandas/SQL -> PySpark -> Parquet/PostgreSQL -> validasi -> Matplotlib.',
'4. Transformasi: tipe data, dimensi, formula net, bulan/basket band, karantina. Jelaskan mengapa uang memakai Decimal.',
'5. Hasil: '+ ' '.join(insights),
'6. Validasi: tampilkan jumlah baris, rekonsiliasi field dan agregat, serta uji 9 baris rusak.',
'7. Performa: tunjukkan tabel median 10k/100k/1m. Jelaskan action Spark, overhead, dan batas collect ke driver.',
'8. Rekomendasi dan keterbatasan: '+ ' '.join(recommendations),
'Pertanyaan dosen yang perlu dikuasai: Mengapa Spark bisa lebih lambat? Apa beda gross/net/profit? Mengapa PostgreSQL dan Parquet dipakai bersama? Apa yang terjadi jika load gagal? Bagaimana menangani duplikat dan data kosong? Mengapa 1 juta baris hasil pengulangan tidak menambah informasi bisnis?'])
(reports/'outline_presentasi.md').write_text(outline,encoding='utf-8')
checklist='''# Checklist terhadap tugas

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
'''
(reports/'checklist_rubrik.md').write_text(checklist,encoding='utf-8')
print('Created report PDF, Markdown, presentation outline and rubric checklist')
