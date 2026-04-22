Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $repoRoot '.venv\Scripts\python.exe'

if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found at $pythonExe"
}

Push-Location $repoRoot
try {
    & $pythonExe (Join-Path $repoRoot 'scripts\sync_hosted_trade_journal_to_local.py') --env-file (Join-Path $repoRoot '.env') --local-db (Join-Path $repoRoot 'instance\horme_trades.db')
    $env:DELPHI_RUNTIME_TARGET = 'local'
    $env:APP_HOST = '127.0.0.1'
    $env:APP_PORT = '5001'
    $env:APP_DISPLAY_NAME = 'Delphi 7.1 Local'
    $env:APP_PAGE_KICKER = 'Delphi 7.1 Local'
    $env:APP_VERSION_LABEL = 'Version 7.1'
    $env:SESSION_COOKIE_NAME = 'delphi4_dev_session'
    $env:OAUTH_SESSION_NAMESPACE = 'delphi4'
    $env:HOSTED_PUBLIC_BASE_URL = ''
    $env:SCHWAB_REDIRECT_URI = 'https://127.0.0.1:5001/callback'
    & $pythonExe app.py
}
finally {
    Pop-Location
}