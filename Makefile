agent: build/agent/pgbackup.linux.x86-64

build/agent/pgbackup.linux.x86-64: build/go/vendor agent/*.go agent/pgwal/*.go agent/pg/*.go
	@mkdir -p $(@D)
	GOPATH=`pwd`/build/go go build -ldflags "-X main.Version=`/bin/date --utc +%Y%m%d.%H%M%S` -X main.Tree=`whoami`" -o $@ agent/*.go

.PHONY: agent
