# Pull Request Description

## 1. Source Schema Design

I modeled a transactional e-commerce system with seven tables in the PostgreSQL `source` schema:

- `customers` - strong entity
- `customer_addresses` - weak entity owned by a customer
- `products` - strong entity
- `orders` - strong entity
- `order_items` - weak entity owned by an order
- `payments` - strong transactional entity
- `payment_attempts` - weak event/entity under a payment

### Keys and Relationships

- primary keys are explicit on every table
- foreign keys connect orders to customers, line items to orders/products, payments to orders, and attempts to payments
- composite key is used for `order_items (order_id, line_number)`

### Indexes

- customer email and status
- product SKU and status
- orders by customer/status/time
- order items by product
- payments by order/status
- payment attempts by payment/time

### Validation Rules

- unique customer email and external reference
- positive quantities and money fields
- valid enum-like status values
- order status transition guard in source code
- final order total must reconcile to line totals + tax + shipping

## 2. CDC Strategy

The source application writes every insert, update, and delete to `cdc_log` within the same transaction as the source change.
The pipeline then appends each new event into a separate lake database before applying it to the warehouse.

### Replay / Restart

- the pipeline checkpoints `last_change_id`
- the warehouse deduplicates on `change_id`
- re-running after a partial failure is safe because already applied events are skipped

### Duplicate Handling

- `cdc_log.change_id` is the source ordering key
- the lake stores `change_id` uniquely
- the warehouse stores `applied_changes.change_id` uniquely

### Deletes

- deletes are represented as CDC events with a before image and no after image
- the warehouse closes the current version rather than erasing historical rows

## 3. Lake and Warehouse Modeling

### Lake

- physically separate append-only change log in the PostgreSQL `lake` schema
- every source change is retained
- before and after images are preserved

### Warehouse

- current-state views are backed by history tables
- the warehouse reflects latest state via `*_current` views
- history tables preserve all versions for restore and audit

### Time Travel / Restore

- `create_point_in_time_snapshot(as_of_ts)` materializes an as-of snapshot from history
- restore is derived from warehouse history, not from source replay

## 4. Schema Change Safety

The pipeline computes a source schema fingerprint from:

- tables
- columns
- types
- nullability
- keys
- foreign keys
- indexes

If the fingerprint changes, ingestion stops and a drift alert is recorded. The policy is deliberately fail-closed because the assignment states the source may not be backward compatible.

## 5. Validation Parity

Warehouse validations re-check:

- not-null and enum expectations
- referential integrity
- non-negative amounts
- positive quantities
- order total reconciliation

Failures raise `ValidationError` and are recorded in `validation_runs`.

## 6. Catalog Exposure

The UI publishes live JSON metadata for:

- `lake.cdc_events`
- `warehouse.<table>_current`
- `warehouse.<table>_history`

Each entry includes:

- layer
- owner
- description
- intended users
- refresh cadence
- primary keys
- schema hash

## 7. Responsible AI Usage

I used AI to help scaffold the repository structure, documentation, and test ideas. I personally reviewed and corrected:

- the schema and relational modeling
- the CDC checkpoint and dedupe logic
- the warehouse history/current design
- schema drift behavior
- the validation rules and tests
