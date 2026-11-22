# Laptop A/B that survives thermal noise: fewer threads, cool-down, paired diffs only.
# Usage (from repo root):
#   .\scripts\ab_local.ps1
# Optional:
#   .\scripts\ab_local.ps1 -BaseExe .\rust\target\release\shopsess-233-rebuild.exe -CandExe .\rust\target\release\shopsess.exe

param(
  [string]$BaseExe = ".\rust\target\release\shopsess-233-rebuild.exe",
  [string]$CandExe = ".\rust\target\release\shopsess.exe",
  [string]$DataDir = "$env:TEMP\ss4m",
  [int]$Ncpu = 4,
  [int]$Pairs = 6,
  [int]$CoolSeconds = 20
)

$ErrorActionPreference = "Stop"
if (-not (Test-Path $BaseExe)) { throw "missing base exe: $BaseExe" }
if (-not (Test-Path $CandExe)) { throw "missing cand exe: $CandExe" }
if (-not (Test-Path (Join-Path $DataDir "manifest.json"))) { throw "missing data: $DataDir" }

$out = Join-Path $env:TEMP "ss-ab-local-out"
$env:NCPU = "$Ncpu"
$env:DATA_DIR = $DataDir
$env:OUTPUT_DIR = $out

function Invoke-Once([string]$bin) {
  Remove-Item -Recurse -Force $out -ErrorAction SilentlyContinue
  New-Item -ItemType Directory -Force -Path $out | Out-Null
  $stderr = & $bin 2>&1 | Out-String
  if ($stderr -match "total ([0-9.]+)ms") { return [double]$Matches[1] / 1000.0 }
  if ($stderr -match "total ([0-9.]+)s") { return [double]$Matches[1] }
  throw "no total in: $stderr"
}

Write-Output "cooling ${CoolSeconds}s, NCPU=$Ncpu"
Start-Sleep -Seconds $CoolSeconds
[void](Invoke-Once $BaseExe)
[void](Invoke-Once $CandExe)

$bases = @()
$cands = @()
for ($i = 1; $i -le $Pairs; $i++) {
  Start-Sleep -Seconds 3
  $b = Invoke-Once $BaseExe
  $c = Invoke-Once $CandExe
  $bases += $b
  $cands += $c
  "{0}: BASE={1:N3}s CAND={2:N3}s diff={3:N3}s" -f $i, $b, $c, ($b - $c) | Write-Output
}

$mb = ($bases | Sort-Object)[[int](($bases.Count - 1) / 2)]
$mc = ($cands | Sort-Object)[[int](($cands.Count - 1) / 2)]
$diffs = 0..($Pairs - 1) | ForEach-Object { $bases[$_] - $cands[$_] }
$md = ($diffs | Sort-Object)[[int](($diffs.Count - 1) / 2)]
"median BASE={0:N3}s CAND={1:N3}s median_diff={2:N3}s ({3:N1}%)" -f $mb, $mc, $md, (100.0 * $md / $mb) | Write-Output
$pass = $md -ge (0.05 * $mb)
"GATE_PASS=$pass"
if (-not $pass) { exit 1 }
