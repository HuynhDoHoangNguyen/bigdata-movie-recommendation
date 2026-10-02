Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve project root from: <project>/scripts/upload_to_hdfs.ps1
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$DatasetDir = Join-Path $ProjectRoot "data\ml-32m"

$RequiredCsvFiles = @(
    "ratings.csv",
    "movies.csv",
    "tags.csv",
    "links.csv"
)

$HdfsBase = "/project/movielens"
$HdfsRaw = "$HdfsBase/raw"
$HdfsStandard = "$HdfsBase/standard"
$HdfsOutput = "$HdfsBase/output"
$ContainerTemp = "/tmp/ml-32m"

Write-Host "========================================"
Write-Host " MovieLens -> HDFS Uploader"
Write-Host "========================================"
Write-Host "Project root : $ProjectRoot"
Write-Host "Dataset path : $DatasetDir"
Write-Host ""

foreach ($file in $RequiredCsvFiles) {
    $filePath = Join-Path $DatasetDir $file

    if (-not (Test-Path $filePath)) {
        throw "Missing local dataset file: $filePath`nRun .\scripts\download_movielens.ps1 first."
    }
}

try {
    docker version | Out-Null
}
catch {
    throw "Docker is not available. Start Docker Desktop and try again."
}

$runningContainers = docker ps --format "{{.Names}}"

if ($runningContainers -notcontains "namenode") {
    throw "Container 'namenode' is not running. Run: docker compose up -d"
}

Write-Host "Creating HDFS directories..."

docker exec namenode hdfs dfs -mkdir -p $HdfsRaw
if ($LASTEXITCODE -ne 0) { throw "Failed to create $HdfsRaw" }

docker exec namenode hdfs dfs -mkdir -p $HdfsStandard
if ($LASTEXITCODE -ne 0) { throw "Failed to create $HdfsStandard" }

docker exec namenode hdfs dfs -mkdir -p $HdfsOutput
if ($LASTEXITCODE -ne 0) { throw "Failed to create $HdfsOutput" }

Write-Host ""
Write-Host "Preparing temporary container directory..."
docker exec namenode rm -rf $ContainerTemp
if ($LASTEXITCODE -ne 0) { throw "Failed to clean $ContainerTemp" }

Write-Host "Copying local MovieLens dataset into NameNode container..."
docker cp $DatasetDir "namenode:$ContainerTemp"
if ($LASTEXITCODE -ne 0) { throw "docker cp failed" }

Write-Host ""
Write-Host "Uploading CSV files to HDFS RAW layer..."

docker exec namenode bash -c "hdfs dfs -put -f $ContainerTemp/*.csv $HdfsRaw/"
if ($LASTEXITCODE -ne 0) { throw "HDFS upload failed" }

Write-Host ""
Write-Host "HDFS RAW contents:"
docker exec namenode hdfs dfs -ls -h $HdfsRaw
if ($LASTEXITCODE -ne 0) { throw "Unable to list $HdfsRaw" }

Write-Host ""
Write-Host "Checking ratings.csv HDFS blocks and replication..."
docker exec namenode hdfs fsck "$HdfsRaw/ratings.csv" -files -blocks -locations
if ($LASTEXITCODE -ne 0) { throw "HDFS fsck failed" }

Write-Host ""
Write-Host "Removing temporary files from NameNode container..."
docker exec namenode rm -rf $ContainerTemp
if ($LASTEXITCODE -ne 0) { throw "Failed to remove temporary container data" }

Write-Host ""
Write-Host "========================================"
Write-Host " HDFS RAW layer is ready"
Write-Host "========================================"
Write-Host "Path:"
Write-Host "  $HdfsRaw"
Write-Host ""
Write-Host "Next step:"
Write-Host "  Run the Spark inspection / validation pipeline."
