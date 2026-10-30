param([int]$Requests=256,[int]$Repeats=3)
. (Join-Path $PSScriptRoot 'events-common.ps1')
Push-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
$oldLimit=[Environment]::GetEnvironmentVariable('RATE_LIMIT','Process')
$oldWindow=[Environment]::GetEnvironmentVariable('RATE_WINDOW','Process')
try {
    Initialize-Experiment 'bench'
    try {
        if($Requests -lt 32 -or $Requests -gt 100000 -or $Repeats -lt 2 -or $Repeats -gt 10){throw 'Requests must be 32..100000 and Repeats must be 2..10'}
        if([long]$Requests*$Repeats*3 -gt 100000){throw 'Total raw samples must be at most 100000; reduce Requests or Repeats'}
        $env:RATE_LIMIT='1000000';$env:RATE_WINDOW='1m'
        Read-ExperimentConfig
        [void](Invoke-Docker 'up' @('compose','up','--build','-d','--wait','--wait-timeout','180') 900)
        [void](Invoke-Docker 'benchmark-gateways' @('compose','up','-d','--no-deps','--force-recreate','--wait','--wait-timeout','60','gateway','gateway2') 90)
        [void](Invoke-Docker 'images' @('compose','images','--format','json') 30)
        Save-ContainerStats 'stats-before'
        $cliArguments=@('compose','--profile','tools','run','--rm','--no-deps','-e','GATEWAY_URLS=http://gateway:8080,http://gateway2:8080','-e','RATE_LIMIT=1000000','-e','RATE_WINDOW=1m','-e',('SOURCE_COMMIT_SHA='+$script:Report.source_commit),'ledgerctl','events-bench','-n',[string]$Requests,'-repeats',[string]$Repeats)
        $result=Invoke-Docker 'events-bench' $cliArguments
        $script:Report.benchmark=$result.Stdout | ConvertFrom-Json
        Save-ContainerStats 'stats-after'
        [void](Invoke-EventPhase 'verify')
        $script:Report.status='passed'
    } catch {
        $script:Report.status='failed';$script:Report.error=$_.Exception.Message;throw
    } finally {
        [Environment]::SetEnvironmentVariable('RATE_LIMIT',$oldLimit,'Process')
        [Environment]::SetEnvironmentVariable('RATE_WINDOW',$oldWindow,'Process')
        try {
            $restore=Invoke-Docker 'restore-original-gateways' @('compose','up','-d','--no-deps','--force-recreate','--wait','--wait-timeout','60','gateway','gateway2') 90 -AllowFailure
            if($restore.ExitCode -ne 0){$script:Report.status='failed';$script:Report.restore_error='Original Gateway configuration did not become healthy'}
        } catch {$script:Report.status='failed';$script:Report.restore_error=$_.Exception.Message}
        $script:Report.finished_at=[DateTimeOffset]::UtcNow.ToString('o');Save-Experiment
        Write-Host ('Benchmark report: '+(Join-Path $script:OutputDirectory 'report.json'))
    }
    if($script:Report.status -ne 'passed'){throw 'Benchmark restore failed; inspect report'}
} finally {Pop-Location}
