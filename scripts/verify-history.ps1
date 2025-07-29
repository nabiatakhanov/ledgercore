$ErrorActionPreference = 'Stop'
$lower = [DateTimeOffset]::Parse('2025-05-08T00:00:00+03:00')
$upper = [DateTimeOffset]::Parse('2025-08-25T00:00:00+03:00')
$previous = $null
$records = @()
foreach ($sha in (git rev-list --reverse HEAD)) {
    $fields = (git show --no-patch --format='%aI|%cI|%s' $sha).Split('|', 3)
    if ($LASTEXITCODE -ne 0 -or $fields.Count -ne 3) { throw 'Cannot read commit metadata' }
    $author = [DateTimeOffset]::Parse($fields[0])
    $committer = [DateTimeOffset]::Parse($fields[1])
    if ($fields[0] -ne $fields[1]) { throw "Author/committer dates differ: $sha" }
    if ($author -lt $lower -or $author -ge $upper) { throw "Date outside historical range: $sha" }
    if ($author.Offset -ne [TimeSpan]::FromHours(3)) { throw "Wrong timezone: $sha" }
    if ($null -ne $previous -and $author -le $previous) { throw "Non-increasing parent-order dates: $sha" }
    $previous = $committer
    $records += [pscustomobject]@{SHA=$sha;Message=$fields[2];AuthorDate=$fields[0];CommitDate=$fields[1]}
}
$records | ConvertTo-Json -Depth 3
Write-Host "PASS: $($records.Count) commits checked in parent order"
