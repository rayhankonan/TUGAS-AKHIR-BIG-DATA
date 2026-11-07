param([int]$Port = 55439)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$postgresRoot = Join-Path $projectRoot '.runtime/postgres'
$dataRoot = Join-Path $projectRoot '.runtime/pgdata'
$logPath = Join-Path $projectRoot '.runtime/postgres.log'
if (-not (Test-Path (Join-Path $postgresRoot 'bin/initdb.exe'))) {
    throw 'Portable PostgreSQL tidak ditemukan. Gunakan folder kerja lengkap, atau PostgreSQL 15 sendiri dan isi .env sesuai README.'
}
if (-not (Test-Path (Join-Path $dataRoot 'PG_VERSION'))) {
    & (Join-Path $postgresRoot 'bin/initdb.exe') -D $dataRoot -U postgres -A trust --encoding=UTF8 --locale=C
    if ($LASTEXITCODE -ne 0) { throw 'initdb gagal' }
}
& (Join-Path $postgresRoot 'bin/pg_ctl.exe') -D $dataRoot status
if ($LASTEXITCODE -ne 0) {
    & (Join-Path $postgresRoot 'bin/pg_ctl.exe') -D $dataRoot -l $logPath -o "-h 127.0.0.1 -p $Port" -w start
    if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL gagal start' }
}
$env:PGHOST='127.0.0.1'
$env:PGPORT="$Port"
$env:PGUSER='postgres'
$env:PGDATABASE='big_data_sales_lab'
$env:PGPASSWORD=''
$env:PGSSLMODE='disable'
$python = Join-Path $projectRoot '.venv/Scripts/python.exe'
if (-not (Test-Path $python)) { throw 'Jalankan tools/setup_python.ps1 lebih dahulu.' }
& $python -c "import psycopg2; c=psycopg2.connect(host='127.0.0.1',port=$Port,user='postgres',dbname='postgres'); c.autocommit=True; q=c.cursor(); q.execute('SELECT 1 FROM pg_database WHERE datname=%s',('big_data_sales_lab',)); exists=q.fetchone(); q.execute('CREATE DATABASE big_data_sales_lab') if not exists else None; c.close()"
if ($LASTEXITCODE -ne 0) { throw 'Inisialisasi database gagal' }
Write-Output 'Database lab siap. Jalankan tools/run_all.ps1 pada terminal ini atau terminal baru.'
