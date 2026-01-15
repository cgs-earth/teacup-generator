param (
    [string]$startDate = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd"),
    [string]$endDate = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd"),
    [switch]$useRise,
    [switch]$useTest,
    [switch]$runLocal # use this for testing in dev
)

$runDate = (Get-Date).ToString("yyyyMMdd")
$rootDirPath = "D:\ScheduledTasks\RISE_Teacups"
$outputDataPath = "\\ibr8drofp001\PROD_RISEDATA\rise_resviz\intake"
if ($useTest) {
    $outputDataPath = "\\ibrsacgis001\WebApps\TEMP\RISE_Teacups_output_test"
}

if ($runLocal) {
    $rootDirPath = Get-Location
    $outputDataPath = "$rootDirPath\test-output"
}

##### ensure all required directories exist
$dirs = $rootDirPath, $outputDataPath, "$outputDataPath\data", "$outputDataPath\teacups", "$rootDirPath\logfiles"
foreach ($dir in $dirs) {
    if (!(Test-Path -Path $dir)) {
        New-Item -Path $dir -ItemType "Directory"
    }
}

$userinfo = [PSCustomObject]@{
    'whoami'          = whoami.exe
    'CurrentUser'     = [Security.Principal.WindowsIdentity]::GetCurrent().Name
}
$userinfo | Out-File -FilePath "$rootDirPath\userinfo.txt"

##### FetchHistoricalData
Set-Location -Path "$rootDirPath\_PROGRAM"
if ((Get-Date).DayOfWeek -eq "Sunday") {
    Write-Output "Refreshing historical data"
    $command = "./FetchHistoricalData.exe"
    if ($useRise) {
        $command += " -r"
    }
    if ($useTest) {
        $command += " -t"
    }
    Invoke-Expression $command | Out-File -FilePath "$rootDirPath\fetchHistoricalDataInfo.txt"
}

##### DroughtDataDownloader
Set-Location -Path "$rootDirPath\_PROGRAM"
Write-Output "Running DroughtDataDownloaderV2"
$command = "./DroughtDataDownloaderV2.exe $startDate $endDate"
if ($useRise) {
    $command += " -r"
}
if ($useTest) {
    $command += " -t"
}
Invoke-Expression $command | Out-File -FilePath "$rootDirPath\dataGenInfo.txt"
Add-Content -Path "$rootDirPath\dataGenInfo.txt" -Value "Copying datafiles"
Copy-Item -Path "$rootDirPath\dataGenInfo.txt" -Destination "$rootDirPath\logfiles\dataGenInfo_$runDate.txt"
Copy-Item -Path "$rootDirPath\_PROGRAM\datafiles\*.csv" -Destination "$outputDataPath\data" -Recurse
Start-Sleep -Seconds 10.0

##### TeacupGenerator
Set-Location -Path "$rootDirPath\_PROGRAM"
Write-Output "Generating teacups"
Invoke-Expression "./TeacupV2.exe $startDate $endDate" | Out-File -FilePath "$rootDirPath\teacupGenInfo.txt"
Add-Content -Path "$rootDirPath\teacupGenInfo.txt" -Value "Copying Teacup files"
Copy-Item -Path "$rootDirPath\teacupGeninfo.txt" -Destination "$rootDirPath\logfiles\teacupGenInfo_$runDate.txt"
Copy-Item -Path "$rootDirPath\_PROGRAM\teacups\*.png" -Destination "$outputDataPath\teacups" -Recurse
Copy-Item -Path "$rootDirPath\_PROGRAM\teacups\*.pdf" -Destination $outputDataPath -Recurse
Start-Sleep -Seconds 10.0

##### cleanup files
Write-Output "Cleaning up files"
Remove-Item "$rootDirPath\_PROGRAM\datafiles\*" -Include *.csv
Remove-Item "$rootDirPath\_PROGRAM\teacups\*" -Include *.png
Remove-Item "$rootDirPath\_PROGRAM\teacups\*" -Include *.pdf
