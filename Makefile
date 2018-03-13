agent: build/agent.linux.x86-64

HOST ?= "pgbackup.com"

build/go/agent.vendor:
	@mkdir -p $(@D)
	GOPATH=`pwd`/build/go go get -u \
		github.com/aws/aws-sdk-go/aws/... \
		github.com/aws/aws-sdk-go/service/s3
	touch $@

build/agent.linux.x86-64: build/go/agent.vendor agent/*.go agent/pgwal/*.go agent/pg/*.go
	@mkdir -p $(@D)
	GOPATH=`pwd`/build/go go build -ldflags "-X main.Version=`/bin/date --utc +%Y%m%d.%H%M%S` -X main.Host=$(HOST)" -o $@ agent/*.go

.PHONY: agent
