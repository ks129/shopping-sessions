IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= dev
FIXTURES ?= fixtures
ANSWER ?= dev-answer
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev native native-fixtures native-dev test

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare your dev output with the published dev answer (dev-answer.zip, unzipped)
check-dev:
	python3 scripts/compare.py out $(ANSWER)

## Native (no Docker) build, unit tests, fixtures and dev check
native:
	cargo build --release
test:
	cargo test --release
native-fixtures: native
	python3 scripts/run_fixtures.py $(FIXTURES) -- target/release/pipeline
native-dev: native
	rm -rf out && time target/release/pipeline $(DEV) out && python3 scripts/compare.py out $(ANSWER)
