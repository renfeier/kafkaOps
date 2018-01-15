GO=go

SRC = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

VERSION := 1.0.0
BUILD := `git rev-parse --short HEAD`
TARGETS := main
TEST_TARGETS := kafkaOps-tester
ALL_TARGETS := $(TARGETS) $(TEST_TARGETS)
project=gitlab.sz.sensetime.com/kafkaOps

export CGO_LDFLAGS= -L${PWD}/libs
export LD_LIBRARY_PATH=${PWD}/libs

all: check build

build: $(TARGETS) $(TEST_TARGETS)

$(TARGETS): $(SRC)
	$(GO) build $(project)/cmd/$@

$(TEST_TARGETS): $(SRC)
	$(GO) build $(project)/test/$@


.PHONY: clean all build check image

lint:
	@gometalinter --config=.gometalint --deadline=100s ./...

packages = $(shell go list ./...|grep -v /vendor/)
test: check
	env LD_LIBRARY_PATH=$(PWD)/libs:/opt/OpenBLAS/libs \
		$(GO) test ${packages}

cov: check
	gocov test $(packages) | gocov-html > coverage.html

check:
	@$(GO) tool vet ${SRC}

clean:
	rm -f $(ALL_TARGETS)
