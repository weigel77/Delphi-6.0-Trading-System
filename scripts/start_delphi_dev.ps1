Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $repoRoot '.venv\Scripts\python.exe'

if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found at $pythonExe"
}

Push-Location $repoRoot
try {
    $env:DELPHI_RUNTIME_TARGET = 'local'
    $env:APP_HOST = '127.0.0.1'
    $env:APP_PORT = '5001'
    $env:APP_DISPLAY_NAME = 'Delphi 4.3 Dev'
    $env:APP_PAGE_KICKER = 'Delphi 4.3 Dev'
    $env:APP_VERSION_LABEL = 'Version 4.3 Dev'
    $env:SESSION_COOKIE_NAME = 'delphi4_dev_session'
    $env:OAUTH_SESSION_NAMESPACE = 'delphi4'
    & $pythonExe app.py
}
finally {
    Pop-Location
}