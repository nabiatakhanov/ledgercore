param([int]$DowntimeSeconds=10)
. (Join-Path $PSScriptRoot 'events-common.ps1')
Push-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
try {
    Initialize-Experiment 'demo'
    $restored=$false
    try {
        if($DowntimeSeconds -lt 1 -or $DowntimeSeconds -gt 60){throw 'DowntimeSeconds must be 1..60'}
        Read-ExperimentConfig
        if($script:RateLimit -ne '60' -or $script:RateWindow -ne '1m'){throw 'Full demo uses RATE_LIMIT=60 and RATE_WINDOW=1m; configure these process values or use individual CLI phases'}
        [void](Invoke-Docker 'up' @('compose','up','--build','-d','--wait','--wait-timeout','180') 900)
        [void](Invoke-Docker 'images' @('compose','images','--format','json') 30)
        # Observe any existing window's expiry, then reuse this one-operation
        # key for the final limiter burst. Shared Redis state is never reset.
        [void](Invoke-EventPhase 'limiter' $script:RunId 1)
        [void](Invoke-EventPhase 'normal')
        $kafkaStopped=[DateTimeOffset]::UtcNow
        [void](Invoke-Docker 'kafka-stop' @('compose','stop','-t','12','kafka') 30)
        $queued=Invoke-EventPhase 'enqueue' ($script:RunId+'-kafka') 20
        if([long]$queued.outbox_backlog -lt 20){throw 'Kafka outage did not retain at least 20 unpublished events'}
        # This is the explicitly measured outage hold, not a readiness condition.
        Start-Sleep -Seconds $DowntimeSeconds
        [void](Invoke-EventPhase 'status')
        $outageStop=Invoke-Docker 'publisher-stop-during-outage' @('compose','stop','-t','12','publisher') 30
        $outagePublisher=(Invoke-Docker 'outage-publisher-container' @('compose','ps','-a','-q','publisher') 30).Stdout.Trim()
        $outageExit=[int](Invoke-Docker 'outage-publisher-exit' @('inspect','--format','{{.State.ExitCode}}',$outagePublisher) 30).Stdout.Trim()
        [void]$script:Report.measurements.Add([ordered]@{phase='sigterm';service='publisher';scenario='kafka_outage';docker_stop_seconds=$outageStop.Seconds;container_exit_code=$outageExit})
        if($outageExit -ne 0){throw ('Publisher did not exit cleanly during Kafka outage, exit '+$outageExit)}
        [void](Invoke-Docker 'kafka-start' @('compose','up','-d','--no-deps','--wait','--wait-timeout','180','kafka') 210)
        $script:Report.kafka_outage=[ordered]@{explicit_hold_seconds=$DowntimeSeconds;stop_start_command_span_seconds=([DateTimeOffset]::UtcNow-$kafkaStopped).TotalSeconds}
        [void](Invoke-Docker 'publisher-start' @('compose','up','-d','--no-deps','--wait','--wait-timeout','60','publisher') 90)
        # Persistent fetch failures deliberately stop Activity without advancing
        # offsets. This is the documented operator recovery after Kafka returns.
        $activityRecovery=Invoke-Docker 'activity-recover-after-kafka' @('compose','up','-d','--no-deps','--wait','--wait-timeout','60','activity') 90
        [void]$script:Report.measurements.Add([ordered]@{phase='recovery';service='activity';reason='kafka_outage';action='explicit_operator_restart';command_seconds=$activityRecovery.Seconds})
        [void](Invoke-EventPhase 'verify')
        $activityStopped=[DateTimeOffset]::UtcNow
        [void](Invoke-Docker 'activity-stop' @('compose','stop','-t','12','activity') 30)
        $activityQueue=Invoke-EventPhase 'enqueue' ($script:RunId+'-activity') 20
        if(([long]$activityQueue.outbox_events-[long]$activityQueue.projected_events) -lt 20){throw 'Stopped Activity did not retain at least 20 unprojected source events'}
        Start-Sleep -Seconds $DowntimeSeconds
        [void](Invoke-EventPhase 'status')
        [void](Invoke-Docker 'activity-start' @('compose','up','-d','--no-deps','--wait','--wait-timeout','60','activity') 90)
        $script:Report.activity_outage=[ordered]@{explicit_hold_seconds=$DowntimeSeconds;stop_start_command_span_seconds=([DateTimeOffset]::UtcNow-$activityStopped).TotalSeconds}
        [void](Invoke-EventPhase 'verify')
        [void](Invoke-EventPhase 'replay')
        [void](Invoke-EventPhase 'limiter' $script:RunId 80)
        [void](Invoke-Docker 'redis-stop' @('compose','stop','-t','12','redis') 30)
        [void](Invoke-EventPhase 'redis-outage')
        [void](Invoke-Docker 'redis-start' @('compose','up','-d','--no-deps','--wait','--wait-timeout','60','redis') 90)
        foreach($service in @('ledger','gateway','gateway2','activity','publisher')) {
            $stopResult=Invoke-Docker ('sigterm-'+$service) @('compose','stop','-t','12',$service) 30
            $id=(Invoke-Docker ('container-'+$service) @('compose','ps','-a','-q',$service) 30).Stdout.Trim()
            $code=[int](Invoke-Docker ('exit-'+$service) @('inspect','--format','{{.State.ExitCode}}',$id) 30).Stdout.Trim()
            [void]$script:Report.measurements.Add([ordered]@{phase='sigterm';service=$service;docker_stop_seconds=$stopResult.Seconds;container_exit_code=$code})
            if($code -ne 0){throw ($service+' did not exit cleanly on SIGTERM, exit '+$code)}
        }
        [void](Invoke-Docker 'restore' @('compose','up','-d','--wait','--wait-timeout','180') 210)
        $restored=$true
        [void](Invoke-EventPhase 'verify')
        $script:Report.status='passed'
    } catch {
        $script:Report.status='failed';$script:Report.error=$_.Exception.Message
        try {[void](Invoke-Docker 'failure-component-logs' @('compose','logs','--no-color','--tail','200','ledger','publisher','activity','gateway','gateway2') 30 -AllowFailure)} catch {}
        throw
    } finally {
        if(-not $restored){
            try {
                $restore=Invoke-Docker 'failure-restore' @('compose','up','-d','--wait','--wait-timeout','180') 210 -AllowFailure
                if($restore.ExitCode -eq 0){$restored=$true}else{$script:Report.restore_error='Services did not become healthy during failure cleanup'}
            } catch {$script:Report.restore_error=$_.Exception.Message}
        }
        $script:Report.services_restored=$restored
        try {[void](Invoke-Docker 'component-logs' @('compose','logs','--no-color','--tail','200','ledger','publisher','activity','gateway','gateway2') 30 -AllowFailure)} catch {}
        $script:Report.finished_at=[DateTimeOffset]::UtcNow.ToString('o');Save-Experiment
        Write-Host ('Demo report: '+(Join-Path $script:OutputDirectory 'report.json'))
    }
} finally {Pop-Location}
