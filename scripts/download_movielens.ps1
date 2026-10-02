Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve project root from: <project>/scripts/download_movielens.ps1
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$DataRoot = Join-Path $ProjectRoot "data"
$DatasetDir = Join-Path $DataRoot "ml-32m"
$ZipPath = Join-Path $DataRoot "ml-32m.zip"

$DatasetUrl = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"

$RequiredFiles = @(
    "ratings.csv",
    "movies.csv",
    "tags.csv",
    "links.csv",
    "README.txt",
    "checksums.txt"
)

Write-Host "========================================"
Write-Host " MovieLens 32M Downloader"
Write-Host "========================================"
Write-Host "Project root : $ProjectRoot"
Write-Host "Dataset path : $DatasetDir"
Write-Host ""

New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null

function Test-DatasetComplete {
    if (-not (Test-Path $DatasetDir)) {
        return $false
    }

    foreach ($file in $RequiredFiles) {
        if (-not (Test-Path (Join-Path $DatasetDir $file))) {
            return $false
        }
    }

    return $true
}

function Test-Checksums {
    $ChecksumFile = Join-Path $DatasetDir "checksums.txt"

    if (-not (Test-Path $ChecksumFile)) {
        throw "checksums.txt not found: $ChecksumFile"
    }

    Write-Host "Verifying MD5 checksums..."

    $Checked = 0

    foreach ($line in Get-Content $ChecksumFile) {
        $trimmed = $line.Trim()

        if ($trimmed -match '^([0-9a-fA-F]{32})\s+\*?(.+)$') {
            $expected = $Matches[1].ToUpperInvariant()
            $fileName = $Matches[2].Trim()
            $filePath = Join-Path $DatasetDir $fileName

            if (-not (Test-Path $filePath)) {
                throw "Checksum target not found: $fileName"
            }

            $actual = (Get-FileHash $filePath -Algorithm MD5).Hash.ToUpperInvariant()

            if ($actual -ne $expected) {
                throw "MD5 mismatch for $fileName. Expected=$expected Actual=$actual"
            }

            Write-Host "  [OK] $fileName"
            $Checked++
        }
    }

    if ($Checked -eq 0) {
        throw "No checksum entries could be parsed from checksums.txt"
    }

    Write-Host "Checksum verification completed successfully."
}

if (Test-DatasetComplete) {
    Write-Host "MovieLens 32M already exists. Skipping download."
    Test-Checksums
    Write-Host ""
    Write-Host "Dataset is ready at:"
    Write-Host "  $DatasetDir"
    exit 0
}

if (Test-Path $DatasetDir) {
    Write-Host "Incomplete dataset directory detected. Removing it before re-download..."
    Remove-Item -Recurse -Force $DatasetDir
}

Write-Host "Downloading official MovieLens 32M dataset..."
Write-Host "Source: $DatasetUrl"

Invoke-WebRequest `
    -Uri $DatasetUrl `
    -OutFile $ZipPath

Write-Host "Download completed:"
Write-Host "  $ZipPath"
Write-Host ""

Write-Host "Extracting dataset..."
Expand-Archive `
    -Path $ZipPath `
    -DestinationPath $DataRoot `
    -Force

foreach ($file in $RequiredFiles) {
    $filePath = Join-Path $DatasetDir $file

    if (-not (Test-Path $filePath)) {
        throw "Required file missing after extraction: $file"
    }
}

Test-Checksums

Write-Host ""
Write-Host "Removing downloaded ZIP..."
Remove-Item -Force $ZipPath

Write-Host ""
Write-Host "========================================"
Write-Host " MovieLens 32M is ready"
Write-Host "========================================"
Write-Host "Location:"
Write-Host "  $DatasetDir"
Write-Host ""
Write-Host "Next step:"
Write-Host "  docker compose up -d"
Write-Host "  .\scripts\upload_to_hdfs.ps1"
