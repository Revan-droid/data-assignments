# CDC Lakehouse Reliability Design

## Domain
The model uses a transactional e-commerce checkout domain because it naturally includes:

- strong entities: customers, products, orders, payments
- weak entities: customer_addresses, order_items, payment_attempts
- one-to-many relationships
- monetary fields, timestamps, nullable fields, and enum-like fields

## Source Layer
The source is a PostgreSQL OLTP model in the `source` schema with relational constraints and indexes.

### Tables

| Table | Type | Purpose |
| --- | --- | --- |
| `customers` | strong entity | Account holder and identity anchor |
| `customer_addresses` | weak entity | Customer-owned address records |
| `products` | strong entity | Sellable catalog item |
| `orders` | strong entity | Order header and commercial contract |
| `order_items` | weak entity | Order line items owned by an order |
| `payments` | strong transactional entity | Payment records for an order |
| `payment_attempts` | weak entity | Retry/event trail under a payment |

### Invariants

- customer email and external reference are unique
- foreign keys must resolve
- amounts cannot be negative
- quantities must be positive
- order status transitions are validated in application code
- final order totals must equal line totals plus tax and shipping

### Strong vs Weak Entities

- strong entities have their own identity and survive independently: `customers`, `products`, `orders`, `payments`
- weak entities depend on a parent and have no standalone meaning without it: `customer_addresses`, `order_items`, `payment_attempts`

## CDC Contract
The source application writes every insert, update, and delete to `cdc_log` in the same transaction as the OLTP change.

Captured fields:

- `change_id`
- `table_name`
- `op`
- entity key JSON
- before image JSON
- after image JSON
- source commit timestamp
- source transaction id
- source schema hash

The ingestion layer then copies each new source change into the `lake` schema in PostgreSQL, which is the durable append-only history exposed to downstream consumers.

### Replay and Restart

- the pipeline stores a checkpoint in the `control.pipeline_state` table
- the warehouse also stores an `applied_changes` ledger
- if a batch partially succeeds, replay is safe because already applied `change_id` values are skipped

### Duplicate Safety

- the lake table uses `change_id` as a unique key
- the warehouse uses `applied_changes.change_id` as a unique key
- reprocessing the same source event is idempotent

## Schema Drift Policy
This solution uses a conservative fail-closed policy.

If the current source schema hash differs from the recorded baseline, ingestion stops and a drift alert is recorded. This is intentionally stricter than a permissive compatibility policy because the assignment states the source does not guarantee backward compatibility.

In practice this catches:

- renamed columns
- dropped columns
- type changes
- nullability changes
- key and index shape changes

## Lake Layer
The lake is an append-only change store backed by the `lake.cdc_events` PostgreSQL table.

Why this counts as a lake:

- every source change is retained
- the lake is physically separate from the source database
- no update/delete semantics are allowed on change records
- each record is queryable for replay and audit
- the log is durable and can be replayed into any downstream model

The lake is intentionally minimal and operationally boring. That is a feature.

## Warehouse Layer
The warehouse keeps curated current-state views backed by SCD2-like history tables in the `warehouse` schema.

### Behavior

- current views return the latest snapshot
- history tables preserve every version
- delete events close the current version instead of erasing history
- point-in-time snapshots can be materialized from history

### Restore Strategy
To restore to a prior time:

1. choose an `as_of` timestamp
2. materialize rows from history where `valid_from <= as_of < valid_to`
3. publish the resulting snapshot as a restore table or backfill target

This gives operational recovery without losing audit history.

## Validation Parity
The warehouse re-checks the important source validations:

- uniqueness and referential expectations
- non-null requirements for operational columns
- enum/domain checks
- amount and quantity constraints
- order total reconciliation

Failures are surfaced via `ValidationError` and recorded in `validation_runs`.

## Catalog
The catalog is a live JSON metadata payload published by the UI. It includes:

- dataset name
- layer
- owner
- purpose
- intended users
- refresh cadence
- primary keys
- schema contract hash

It exposes both:

- lake dataset: `lake.cdc_events`
- warehouse datasets: `warehouse.<table>_current`, `warehouse.<table>_history`

## Operational Notes

- source, lake, warehouse, and control stores are separated
- ingest stops on schema drift
- warehouse validation can be run independently
- restore is derived from immutable history, not from source replay

## Limitations

- source CDC is simulated in application code instead of reading a PostgreSQL logical decoding stream
- this keeps the implementation dependency-light and easy to inspect
- a production implementation would move the capture boundary into the source database log or a dedicated CDC service
