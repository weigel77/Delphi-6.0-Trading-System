Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$devRoot = Join-Path $repoRoot 'parallel\delphi-4.0-dev'

New-Item -ItemType Directory -Path $devRoot -Force | Out-Null

$robocopyArgs = @(
    $repoRoot,
    $devRoot,
    '/E',
    '/R:1',
    '/W:1',
    '/XD', '.git', 'venv', '.venv', '__pycache__', 'instance', 'artifacts', 'parallel', '.pytest_cache', '.mypy_cache',
    '/XF', '.env', 'schwab_token.json', 'market_lookup.log'
)

& robocopy @robocopyArgs | Out-Null

$sourceEnv = Join-Path $repoRoot '.env'
$targetEnv = Join-Path $devRoot '.env'
if (Test-Path $sourceEnv) {
    Copy-Item $sourceEnv $targetEnv -Force
} elseif (-not (Test-Path $targetEnv)) {
    New-Item -ItemType File -Path $targetEnv -Force | Out-Null
}

$devOverrides = @(
    'APP_PORT=5001',
    'APP_DISPLAY_NAME=Delphi 4.0 Dev',
    'APP_PAGE_KICKER=Delphi 4.0 Dev',
    'APP_VERSION_LABEL=Version 4.0 Dev',
    'SESSION_COOKIE_NAME=delphi4_dev_session',
    'KAIROS_REPLAY_STORAGE_DIR=instance/kairos_replays',
    'APP_LOG_PATH=instance/logs/market_lookup.dev.log',
    'SCHWAB_REDIRECT_URI=https://127.0.0.1:5001/callback',
    'SCHWAB_TOKEN_PATH=instance/schwab_token.dev.json'
)

$existingLines = if (Test-Path $targetEnv) { Get-Content $targetEnv } else { @() }
$filteredLines = foreach ($line in $existingLines) {
    if ($line -match '^(APP_PORT|APP_DISPLAY_NAME|APP_PAGE_KICKER|APP_VERSION_LABEL|SESSION_COOKIE_NAME|KAIROS_REPLAY_STORAGE_DIR|APP_LOG_PATH|SCHWAB_REDIRECT_URI|SCHWAB_TOKEN_PATH)=') {
        continue
    }
    $line
}

$normalizedContent = @($filteredLines + '' + '# Delphi 4.0 Dev parallel-instance overrides' + $devOverrides)
Set-Content -Path $targetEnv -Value $normalizedContent

New-Item -ItemType Directory -Path (Join-Path $devRoot 'instance') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $devRoot 'instance\kairos_replays') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $devRoot 'instance\trade_import_previews') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $devRoot 'instance\logs') -Force | Out-Null

Write-Host "Delphi 4.0 Dev clone refreshed at $devRoot"