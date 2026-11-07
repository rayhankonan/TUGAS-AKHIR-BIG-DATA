$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$java = Get-ChildItem (Join-Path $projectRoot '.runtime/java') -Directory | Select-Object -First 1
if ($java) { $env:JAVA_HOME=$java.FullName }
$env:PGHOST='127.0.0.1'
$env:PGPORT='55439'
$env:PGUSER='postgres'
$env:PGDATABASE='big_data_sales_lab'
$env:PGPASSWORD=''
$env:PGSSLMODE='disable'
$python = Join-Path $projectRoot '.venv/Scripts/python.exe'
$env:PYSPARK_PYTHON=$python
Push-Location $projectRoot
try {
    & $python run_pipeline.py
    if ($LASTEXITCODE -ne 0) { throw 'Pipeline gagal; baca pesan error.' }
    & $python scripts/test_quality_policy.py
    if ($LASTEXITCODE -ne 0) { throw 'Uji kualitas gagal.' }
    & $python scripts/validate_outputs.py
    if ($LASTEXITCODE -ne 0) { throw 'Validasi output gagal.' }
} finally { Pop-Location }
