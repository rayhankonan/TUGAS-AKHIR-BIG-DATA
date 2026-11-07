param([string]$Python = 'py')
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$venvRoot = Join-Path $projectRoot '.venv'
if ($Python -eq 'py') { & py -3.12 -m venv $venvRoot }
else { & $Python -m venv $venvRoot }
if ($LASTEXITCODE -ne 0) { throw 'Python 3.12 diperlukan. Pasang Python 3.12 atau berikan -Python dengan path python.exe versi 3.12.' }
$requirements = Join-Path $projectRoot 'requirements-lock.txt'
if (-not (Test-Path $requirements)) { $requirements = Join-Path $projectRoot 'requirements.txt' }
& (Join-Path $venvRoot 'Scripts/python.exe') -m pip install -r $requirements
if ($LASTEXITCODE -ne 0) { throw 'Instalasi dependensi gagal' }
Write-Output 'Dependensi siap.'
