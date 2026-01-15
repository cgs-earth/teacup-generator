$rootDir = Get-Location
$releaseDir = "$rootDir\release\_PROGRAM"
$projects = "DroughtDataDownloaderV2", "TeacupV2", "RiseTeacupsLib", "FetchHistoricalData"

if (!(Test-Path -Path $releaseDir)) {
    New-Item -Path $releaseDir -ItemType "Directory"
}

foreach ($project in $projects) {
    Write-Host "--- Building $project"
    $projectDir = "$rootDir\src\$project"
    $buildDir = "$projectDir\bin\Debug\net8.0"
    if (Test-Path -Path "$projectDir\bin\Debug\net8.0") {
        Remove-Item $buildDir -Recurse -Force
    }
    Set-Location $projectDir
    Invoke-Expression "dotnet build"
    Copy-Item -Path "$buildDir\*" -Destination $releaseDir -Exclude "datafiles", "teacups", "de", "runtimes" -Recurse
}

Write-Host "--- Release Built"
