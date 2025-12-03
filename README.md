# Telemetry Ingestor

[![Go Reference](https://pkg.go.dev/badge/github.com/laiambryant/telemetry-ingestor.svg)](https://pkg.go.dev/github.com/laiambryant/telemetry-ingestor)
[![Go Report Card](https://goreportcard.com/badge/github.com/laiambryant/telemetry-ingestor)](https://goreportcard.com/report/github.com/laiambryant/telemetry-ingestor)
[![GitHub license](https://img.shields.io/github/license/laiambryant/telemetry-ingestor.svg)](https://github.com/laiambryant/telemetry-ingestor/blob/main/LICENSE)
[![GitHub issues](https://img.shields.io/github/issues/laiambryant/telemetry-ingestor.svg)](https://github.com/laiambryant/telemetry-ingestor/issues)
[![GitHub stars](https://img.shields.io/github/stars/laiambryant/telemetry-ingestor.svg)](https://github.com/laiambryant/telemetry-ingestor/stargazers)
[![Coverage Status](https://coveralls.io/repos/github/laiambryant/telemetry-ingestor/badge.svg?branch=main)](https://coveralls.io/github/laiambryant/telemetry-ingestor?branch=main)

![Kirby](doc/img/ingestor.png)

A Go-based tool for ingesting OpenTelemetry Protocol (OTLP) formatted telemetry data from JSON Lines files and sending it to an OpenTelemetry Collector.

## Summary

1. [Installation](#installation)
2. [Usage](#usage)
3. [Input Format](#input-format)
4. [Development](#development)
5. [License](#license)
6. [Contributing](#contributing)

## Installation

```bash
go install github.com/laiambryant/observability-utils/ingest_telemetry_go@latest
```

Or build from source:

```bash
git clone https://github.com/laiambryant/telemetry-ingestor
cd telemetry-ingestor
go build -o ingest_telemetry
```

## Usage

### Basic Usage

Send last occurrence of each telemetry type, using the -f flag to specify the file

```bash
./ingest_telemetry -f telemetry.json
```

### Running Send All Mode

Send all telemetry lines with 20 concurrent workers

```bash
./ingest_telemetry -f telemetry.json --sendAll --workers 20
```

### Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-f, --file` | `telemetry.json` | Path to telemetry JSON Lines file |
| `--traces-endpoint` | `http://localhost:4318/v1/traces` | OpenTelemetry traces endpoint |
| `--logs-endpoint` | `http://localhost:4318/v1/logs` | OpenTelemetry logs endpoint |
| `--metrics-endpoint` | `http://localhost:4318/v1/metrics` | OpenTelemetry metrics endpoint |
| `--max-buffer-capacity` | `1048576` (1MB) | Maximum buffer capacity for reading lines |
| `--sendAll` | `false` | Send all telemetry lines instead of last occurrence |
| `--workers` | `10` | Number of concurrent workers (only with `--sendAll`) |

## Input Format

The tool expects JSON Lines format where each line contains OTLP-formatted telemetry data:

```json
{"resourceSpans":[{"resource":{"attributes":[]},"scopeSpans":[]}]}
{"resourceLogs":[{"resource":{"attributes":[]},"scopeLogs":[]}]}
{"resourceMetrics":[{"resource":{"attributes":[]},"scopeMetrics":[]}]}
```

### Processing Modes

#### Last Mode (Default)

1. Scans entire file
2. Keeps only the last occurrence of each telemetry type in memory
3. Sends three payloads maximum (one per type)
4. Memory efficient for large files

#### Send All Mode

1. Reads file line by line
2. Queues each telemetry occurrence as a job
3. Worker pool processes jobs concurrently
4. Higher throughput but more network requests

## Development

### VS Code Configuration

This project includes pre-configured VS Code settings in the docs folder:

- [`.vscode/launch.json`](doc/.vscode/launch.json) - Debug configurations for running and testing the application
- [`.vscode/tasks.json`](doc/.vscode/tasks.json) - Build and test tasks

Should you want to use them copy the .vscode directory in the doc folder into the root of this repository.

After doing so, press `F5` to start debugging or `Ctrl+Shift+B` to build.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please ensure:

- All tests pass (`go test ./...`)
- Code follows Go best practices
- New features include appropriate test
