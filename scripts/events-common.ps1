# Shared local experiment I/O. PowerShell 5.1+, Docker Compose, Git; no modules.
$ErrorActionPreference='Stop'
$script:Utf8=New-Object System.Text.UTF8Encoding($false)
function Quote-NativeArgument([string]$Value) {
    if ($Value.Length -gt 0 -and $Value -notmatch '[\s"]') { return $Value }
    return '"'+[regex]::Replace([regex]::Replace($Value,'(\\*)"','$1$1\"'),'(\\+)$','$1$1')+'"'
}
function Initialize-Experiment([string]$Mode) {
    $script:RunId=[guid]::NewGuid().ToString()
    $script:OutputDirectory=Join-Path (Get-Location) ('.runtime/events-'+$Mode)
    $script:RunDirectory=Join-Path $script:OutputDirectory $script:RunId
    New-Item -ItemType Directory -Force -Path $script:RunDirectory | Out-Null
    $script:Sequence=0
    $sha=(& git rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Git HEAD is required to record experiment provenance' }
    $dirty=(@(& git status --porcelain).Count -gt 0)
    $manifest=New-Object System.Collections.Generic.List[string]
    foreach($directory in @('cmd','internal','gen','proto','migrations','scripts')) {
        Get-ChildItem -LiteralPath $directory -Recurse -File | Sort-Object FullName | ForEach-Object {
            $relative=$_.FullName.Substring((Get-Location).Path.Length+1).Replace('\','/')
            $manifest.Add($relative+' '+(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant())
        }
    }
    foreach($file in @('go.mod','go.sum','Dockerfile','compose.yaml')) {
        $manifest.Add($file+' '+(Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant())
    }
    $hasher=[System.Security.Cryptography.SHA256]::Create()
    try {$contentHash=([BitConverter]::ToString($hasher.ComputeHash($script:Utf8.GetBytes(($manifest -join "`n"))))).Replace('-','').ToLowerInvariant()} finally {$hasher.Dispose()}
    $script:Report=[ordered]@{mode=$Mode;run_id=$script:RunId;started_at=[DateTimeOffset]::UtcNow.ToString('o');source_commit=$sha;source_dirty=$dirty;source_content_sha256=$contentHash;status='running';steps=(New-Object System.Collections.ArrayList);measurements=(New-Object System.Collections.ArrayList)}
    Save-Experiment
}
function Save-Experiment {
    $json=$script:Report | ConvertTo-Json -Depth 40
    [IO.File]::WriteAllText((Join-Path $script:RunDirectory 'report.json'),$json,$script:Utf8)
    [IO.File]::WriteAllText((Join-Path $script:OutputDirectory 'report.json'),$json,$script:Utf8)
}
function Invoke-Docker([string]$Label,[string[]]$DockerArgs,[int]$TimeoutSeconds=660,[switch]$SensitiveOutput,[switch]$AllowFailure) {
    $script:Sequence++
    $name=('{0:d2}-{1}' -f $script:Sequence,$Label)
    $stdoutFile=Join-Path $script:RunDirectory ($name+'.stdout.log')
    $stderrFile=Join-Path $script:RunDirectory ($name+'.stderr.log')
    $info=New-Object System.Diagnostics.ProcessStartInfo
    $info.FileName='docker.exe'
    $info.Arguments=(@($DockerArgs | ForEach-Object {Quote-NativeArgument $_}) -join ' ')
    $info.UseShellExecute=$false;$info.CreateNoWindow=$true
    $info.RedirectStandardOutput=$true;$info.RedirectStandardError=$true
    $info.StandardOutputEncoding=$script:Utf8;$info.StandardErrorEncoding=$script:Utf8
    $process=New-Object System.Diagnostics.Process;$process.StartInfo=$info
    $started=[DateTimeOffset]::UtcNow;$watch=[Diagnostics.Stopwatch]::StartNew()
    if (-not $process.Start()) {throw ('Could not start Docker phase '+$Label)}
    $outTask=$process.StandardOutput.ReadToEndAsync();$errTask=$process.StandardError.ReadToEndAsync()
    $timedOut=-not $process.WaitForExit($TimeoutSeconds*1000)
    if($timedOut){
        # Compose runs as a Docker CLI child plugin. Kill only this command's
        # process tree, otherwise an inherited pipe could outlive the CLI.
        try {& taskkill.exe /PID $process.Id /T /F 2>&1 | Out-Null} catch {}
        [void]$process.WaitForExit(5000)
    }
    $stdout='[output unavailable after bounded termination]';$stderr=$stdout
    if($outTask.Wait(5000)){$stdout=$outTask.GetAwaiter().GetResult()}
    if($errTask.Wait(5000)){$stderr=$errTask.GetAwaiter().GetResult()}
    $code=-1;if(-not $timedOut){$code=$process.ExitCode}
    $process.Dispose();$watch.Stop()
    if(-not $SensitiveOutput){[IO.File]::WriteAllText($stdoutFile,$stdout,$script:Utf8);[IO.File]::WriteAllText($stderrFile,$stderr,$script:Utf8)}
    $row=[ordered]@{name=$Label;started_at=$started.ToString('o');finished_at=[DateTimeOffset]::UtcNow.ToString('o');seconds=$watch.Elapsed.TotalSeconds;exit_code=$code;timed_out=$timedOut}
    if(-not $SensitiveOutput){$row.stdout_file=$stdoutFile;$row.stderr_file=$stderrFile}
    [void]$script:Report.steps.Add($row);Save-Experiment
    if($code -ne 0 -and -not $AllowFailure){throw ('Docker phase '+$Label+' failed, exit '+$code+'; inspect its raw stderr log')}
    return [pscustomobject]@{Stdout=$stdout;Stderr=$stderr;ExitCode=$code;Seconds=$watch.Elapsed.TotalSeconds}
}
function Read-ExperimentConfig {
    # Full config contains secrets; parse in memory and never save its raw text.
    $raw=Invoke-Docker 'config-private' @('compose','config','--format','json') 30 -SensitiveOutput
    $config=$raw.Stdout | ConvertFrom-Json
    $script:RateLimit=[string]$config.services.gateway.environment.RATE_LIMIT
    $script:RateWindow=[string]$config.services.gateway.environment.RATE_WINDOW
    $resources=New-Object System.Collections.ArrayList
    foreach($name in @('db','kafka','redis','ledger','publisher','activity','gateway','gateway2')) {
        $service=$config.services.$name
        [void]$resources.Add([ordered]@{service=$name;image=$service.image;cpus=$service.cpus;memory_limit_bytes=$service.mem_limit})
    }
    $script:Report.services=$resources;$script:Report.rate_limit=$script:RateLimit;$script:Report.rate_window=$script:RateWindow
    $hardware=Invoke-Docker 'docker-hardware' @('info','--format','{"server_version":"{{.ServerVersion}}","cpu_count":{{.NCPU}},"memory_bytes":{{.MemTotal}}}') 30
    $script:Report.host=$hardware.Stdout | ConvertFrom-Json
    Save-Experiment
}
function Invoke-EventPhase([string]$Phase,[string]$PhaseRunId=$script:RunId,[int]$Count=20) {
    $cliArguments=@('compose','--profile','tools','run','--rm','--no-deps','-e',('GATEWAY_URLS=http://gateway:8080,http://gateway2:8080'),'-e',('RATE_LIMIT='+$script:RateLimit),'-e',('RATE_WINDOW='+$script:RateWindow),'-e',('SOURCE_COMMIT_SHA='+$script:Report.source_commit),'ledgerctl','events-demo','--phase',$Phase,'--run-id',$PhaseRunId,'--count',[string]$Count)
    $result=Invoke-Docker ('phase-'+$Phase) $cliArguments
    $value=$result.Stdout | ConvertFrom-Json
    [void]$script:Report.measurements.Add([ordered]@{phase=$Phase;command_seconds=$result.Seconds;result=$value})
    Save-Experiment
    return $value
}
function Save-ContainerStats([string]$Label) {
    $ids=(Invoke-Docker ($Label+'-ids') @('compose','ps','-q') 30).Stdout -split '\s+' | Where-Object {$_}
    if($ids.Count -gt 0){[void](Invoke-Docker $Label (@('stats','--no-stream','--format','{{json .}}')+$ids) 30)}
}
