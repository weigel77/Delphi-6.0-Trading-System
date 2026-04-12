Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$devRoot = Join-Path $repoRoot 'parallel\delphi-4.0-dev'
$pythonExe = Join-Path $repoRoot 'venv\Scripts\python.exe'

if (-not (Test-Path $devRoot)) {
    throw "Dev clone not found at $devRoot. Run .\scripts\setup_delphi4_dev_clone.ps1 first."
}

if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found at $pythonExe"
}

Push-Location $devRoot
try {
    & $pythonExe app.py
}
finally {
    Pop-Location
}