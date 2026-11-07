$projectRoot = Split-Path -Parent $PSScriptRoot
& (Join-Path $projectRoot '.runtime/postgres/bin/pg_ctl.exe') -D (Join-Path $projectRoot '.runtime/pgdata') -m fast -w stop
