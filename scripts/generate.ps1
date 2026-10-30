$ErrorActionPreference='Stop'
New-Item -ItemType Directory -Force .tools | Out-Null
$archive='.tools/protoc-36.2-win64.zip'
if (-not (Test-Path -LiteralPath '.tools/protoc-36.2/bin/protoc.exe')) {
    Invoke-WebRequest -Uri 'https://github.com/protocolbuffers/protobuf/releases/download/v36.2/protoc-36.2-win64.zip' -OutFile $archive
    if ((Get-FileHash -Algorithm SHA256 -LiteralPath $archive).Hash -ne 'F0C128DC0D8492ECEECE83BB459A4C0E316764B929FFBF1AA416357FD644EDD3') { throw 'protoc checksum mismatch' }
    Expand-Archive -LiteralPath $archive -DestinationPath '.tools/protoc-36.2' -Force
}
$env:GOBIN=Join-Path (Get-Location) '.tools'
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
if ($LASTEXITCODE -ne 0) { throw 'protoc-gen-go install failed' }
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
if ($LASTEXITCODE -ne 0) { throw 'protoc-gen-go-grpc install failed' }
$env:PATH=$env:GOBIN+';'+$env:PATH
& '.tools/protoc-36.2/bin/protoc.exe' -I proto -I .tools/protoc-36.2/include --go_out=. --go_opt=module=ledgercore --go-grpc_out=. --go-grpc_opt=module=ledgercore proto/ledger/v1/ledger.proto proto/activity/v1/activity.proto
if ($LASTEXITCODE -ne 0) { throw 'protoc generation failed' }
