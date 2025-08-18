# Windows resource sampler for a running benchmark API. DATABASE_URL,
# FIXTURE_DATABASE_URL, API_TOKEN, API_URL and METRICS_URL must already be set.
param(
    [Parameter(Mandatory = $true)][int]$ApiProcessId,
    [string]$LedgerctlPath = '.runtime/ledgerctl.exe',
    [ValidateRange(32,100000)][int]$Requests = 1024,
    [string]$OutputDirectory = '.runtime/benchmark'
)
$ErrorActionPreference = 'Stop'
if ($env:OS -ne 'Windows_NT') { throw 'This sampler uses Windows process counters.' }
$cliPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($LedgerctlPath)
$outputPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($OutputDirectory)
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
$resultFile = Join-Path $outputPath 'benchmark-results.json'
$resourceFile = Join-Path $outputPath 'benchmark-resources.json'
$progressFile = [System.IO.Path]::ChangeExtension($resultFile, 'progress.log')
$api = Get-Process -Id $ApiProcessId
$apiBaseline = $api.TotalProcessorTime.TotalSeconds
$pgBaseline = @{}
$pgObserved = @{}
foreach ($p in (Get-Process -Name postgres)) {
    $key = "$($p.Id)/$($p.StartTime.Ticks)"
    $pgBaseline[$key] = $p.TotalProcessorTime.TotalSeconds
    $pgObserved[$key] = $p.TotalProcessorTime.TotalSeconds
}
$samples = [System.Collections.Generic.List[object]]::new()
$startedAt = [DateTimeOffset]::Now.ToString('o')
$clock = [System.Diagnostics.Stopwatch]::StartNew()
$cli = Start-Process -FilePath $cliPath -ArgumentList @('bench','-n',"$Requests") -WindowStyle Hidden -PassThru -RedirectStandardOutput $resultFile -RedirectStandardError $progressFile
# Keep the handle open so ExitCode remains available after background completion.
$null = $cli.Handle
do {
    $api.Refresh()
    $pgWorking = 0L
    $pgPrivate = 0L
    $pgCount = 0
    foreach ($p in (Get-Process -Name postgres)) {
        try {
            $key = "$($p.Id)/$($p.StartTime.Ticks)"
            $cpu = $p.TotalProcessorTime.TotalSeconds
            if (-not $pgObserved.ContainsKey($key) -or $cpu -gt $pgObserved[$key]) { $pgObserved[$key] = $cpu }
            $pgWorking += $p.WorkingSet64
            $pgPrivate += $p.PrivateMemorySize64
            $pgCount++
        } catch { } # A backend may have exited between enumeration and inspection.
    }
    $pgCpu = 0.0
    foreach ($key in $pgObserved.Keys) {
        $baseline = 0.0
        if ($pgBaseline.ContainsKey($key)) { $baseline = $pgBaseline[$key] }
        $pgCpu += [Math]::Max(0.0, ($pgObserved[$key] - $baseline))
    }
    $samples.Add([pscustomobject]@{
        elapsed_seconds = $clock.Elapsed.TotalSeconds
        api_cpu_seconds = $api.TotalProcessorTime.TotalSeconds - $apiBaseline
        api_working_set_bytes = $api.WorkingSet64
        api_private_bytes = $api.PrivateMemorySize64
        postgres_observed_cpu_seconds = $pgCpu
        postgres_aggregate_working_set_bytes = $pgWorking
        postgres_aggregate_private_bytes = $pgPrivate
        postgres_processes = $pgCount
    })
    $cli.Refresh()
    if (-not $cli.HasExited) { Start-Sleep -Milliseconds 100 }
} while (-not $cli.HasExited)
$cli.WaitForExit()
$exitCode = $cli.ExitCode
if ($null -eq $exitCode) { throw 'Could not obtain the benchmark process exit code.' }
$clock.Stop()
$api.Refresh()
$last = $samples[$samples.Count - 1]
$summary = [ordered]@{
    measured_at = $startedAt
    command = "ledgerctl bench -n $Requests"
    method = 'Windows Get-Process; target 100ms sampling. CPU tracks observed per-PID/start-time maxima minus initial baselines. Short-lived PostgreSQL backends and CPU after their final observation can be missed. Aggregate PostgreSQL working sets may double-count shared pages. All processes share the host; totals include fixture setup, scraping and reconciliation.'
    wall_seconds_including_setup = $clock.Elapsed.TotalSeconds
    api_cpu_seconds = $last.api_cpu_seconds
    api_sampled_peak_working_set_bytes = ($samples | Measure-Object -Property api_working_set_bytes -Maximum).Maximum
    api_os_peak_working_set_bytes_since_start = $api.PeakWorkingSet64
    api_sampled_peak_private_bytes = ($samples | Measure-Object -Property api_private_bytes -Maximum).Maximum
    postgres_observed_cpu_seconds = $last.postgres_observed_cpu_seconds
    postgres_sampled_peak_aggregate_working_set_bytes = ($samples | Measure-Object -Property postgres_aggregate_working_set_bytes -Maximum).Maximum
    postgres_sampled_peak_aggregate_private_bytes = ($samples | Measure-Object -Property postgres_aggregate_private_bytes -Maximum).Maximum
    sample_count = $samples.Count
    cli_exit_code = $exitCode
    progress = @(Get-Content -LiteralPath $progressFile)
    samples = $samples
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resourceFile -Encoding utf8
if ($exitCode -ne 0) { throw "Benchmark failed with exit code $exitCode; inspect the captured progress log." }
Write-Output "Benchmark complete: $resultFile; process counters: $resourceFile"
