# cdc-lakehouse-app

Local CDC lakehouse reliability prototype for the data engineering assignment.

## What this repo demonstrates

- a realistic transactional source schema
- CDC capture of inserts, updates, and deletes
- PostgreSQL-backed source, lake, warehouse, and control schemas
- a browser UI with all changes and live dataset views
- schema drift detection with fail-closed behavior
- parity validations between source assumptions and warehouse models
- a lightweight JSON catalog payload exposed through the UI

## Architecture

Read the design first:

- [docs/architecture.md](docs/architecture.md)
- [docs/pr_description.md](docs/pr_description.md)

## Layout

- `cdc_lakehouse/` - implementation
- `tests/` - unittest coverage for CDC, replay, schema drift, restore, and catalog
- `docs/` - design and PR text

## Run it locally

Point the app at a PostgreSQL database with environment variables:

```bash
export CDC_PGHOST=127.0.0.1
export CDC_PGPORT=5432
export CDC_PGUSER=postgres
export CDC_PGDATABASE=cdc_lakehouse
```

```bash
python3 -m cdc_lakehouse.cli init
python3 -m cdc_lakehouse.cli seed
python3 -m cdc_lakehouse.cli sync
python3 -m cdc_lakehouse.cli validate
```

## Run as an inspector service

```bash
python3 -m cdc_lakehouse.cli serve
```

Then open `http://localhost:8080/` or `http://localhost:8080/changes`.

## Run on Minikube

```bash
minikube start --driver=docker --cpus=4 --memory=6g
eval "$(minikube docker-env)"
docker build -t cdc-lakehouse-app:local .
kubectl apply -k k8s
kubectl get pods -n cdc-lakehouse
kubectl port-forward -n cdc-lakehouse svc/cdc-lakehouse-inspector 8080:80
```

Then open:

- `http://localhost:8080/`
- `http://localhost:8080/state`
- `http://localhost:8080/catalog`
- `http://localhost:8080/changes`
- `http://localhost:8080/warehouse/orders/current`

The bootstrap Job initializes the Postgres schemas, seeds demo data, runs CDC sync, validates the warehouse, and writes a `bootstrap.done` marker into the PVC. The inspector Deployment waits for that marker before serving traffic.

## Understand the flow

Open these pages in order:

1. `http://localhost:8080/`
2. `http://localhost:8080/changes`
3. `http://localhost:8080/source/customers`
4. `http://localhost:8080/warehouse/customers/current`
5. `http://localhost:8080/warehouse/customers/history`
6. `http://localhost:8080/catalog`

## Point-in-time snapshot

```bash
python3 -m cdc_lakehouse.cli snapshot --as-of 2026-01-01T00:00:00+00:00
```

## Simulate schema drift

```bash
python3 -m cdc_lakehouse.cli break-schema
python3 -m cdc_lakehouse.cli sync
```

The sync should stop with a schema drift error.

## Run tests

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```

## Notes

- This uses a pure-Python PostgreSQL wire client so the repo stays dependency-light.
- In production, the CDC boundary would usually move into the source database log or a dedicated CDC service.
- The repo is designed to be easy to reason about under replay, restart, and schema-change failure modes.
