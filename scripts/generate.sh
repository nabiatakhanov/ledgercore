#!/bin/sh
set -eu
# Real pinned protoc + Go generators. Run from the repository root.
mkdir -p .tools
archive=.tools/protoc-36.2-linux-x86_64.zip
if [ ! -x .tools/protoc-36.2/bin/protoc ]; then
  curl -fL --retry 2 https://github.com/protocolbuffers/protobuf/releases/download/v36.2/protoc-36.2-linux-x86_64.zip -o "$archive"
  echo "121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278  $archive" | sha256sum -c -
  mkdir -p .tools/protoc-36.2
  unzip -oq "$archive" -d .tools/protoc-36.2
fi
export GOBIN="$(pwd)/.tools"
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
export PATH="$GOBIN:$PATH"
.tools/protoc-36.2/bin/protoc -I proto -I .tools/protoc-36.2/include \
  --go_out=. --go_opt=module=ledgercore \
  --go-grpc_out=. --go-grpc_opt=module=ledgercore \
  proto/ledger/v1/ledger.proto proto/activity/v1/activity.proto
