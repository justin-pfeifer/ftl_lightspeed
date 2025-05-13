# FTL: Fetch → Transform → Load Framework

FTL is a lightweight and extensible framework for defining modular data pipelines based on the classic Fetch → Transform → Load pattern. It supports memory-efficient, chunked streaming — particularly optimized for PostgreSQL ingestion via the `COPY` command — and is structured to support multiple source formats.

## ✨ Features

- True streaming from CSV files or PostgreSQL queries
- Chunked writes to PostgreSQL using `COPY FROM STDIN`
- Optional buffering to control write chunk size
- Logging with configurable verbosity
- Extensible `Producer` class system — define or override your own
- Future support for JSON, Excel, Parquet, DuckDB, etc.

---

## 🧱 Core Components

### CopyProducer

Streams data from a PostgreSQL `SELECT` query using `COPY (SELECT ...) TO STDOUT WITH CSV`.

**Typical Use:**

```python
class MyETL(CopyProducer, FTL):
    source_conn = psycopg.connect(...)
    query = "SELECT ..."
    chunk_size = 512  # in MB

    def fetch(self):
        self.consumer = CopyConsumer(
            conn=target_conn,
            table="public.my_table",
            columns="id, name, email"
        )

    def load(self):
        self.consumer.consume(self, chunk_mb=64)  # self is the producer
```

### CSVProducer

Streams from a local CSV file.

**Typical Use:**

```python
class MyCSVETL(CSVProducer, FTL):
    file_path = "data.csv"

    def fetch(self):
        self.headers = self.get_headers(self.file_path)
        self.consumer = CopyConsumer(
            conn=target_conn,
            table="public.contacts",
            columns=self.headers
        )

    def load(self):
        self.consumer.consume(self.file_path, chunk_mb=64)
```

---

## 🔌 Consumer Interface

All consumers write data to PostgreSQL. You can use:

- `CopyConsumer` — stable and verified
- `InsertConsumer` — in development
- `UpsertConsumer` — in development

> ⚠️ Note: Only `CopyConsumer` is currently verified and production-ready. The insert-based consumers are in-progress and may change.

Each consumer supports the `.consume()` method or callable interface.

---

## 🛠️ Adding Your Own Producer

To define a new Producer:

1. Inherit from `FTL` and either an existing `Producer` (`CopyProducer`, `CSVProducer`) or create your own.
2. Implement the `fetch()` and `load()` methods.
3. Use `transform()` as needed for inline transformation logic.
4. Run the job using `.warp()`:

```python
if __name__ == "__main__":
    MyETL().warp()
```

You can also define your own producer to handle JSON, Excel, Parquet, DuckDB, etc. — as either file-based or queryable sources.

---

## 📦 Environment Variables

You can use a `.env` file for quick local development or scripting. For more secure and team-oriented setups, we plan to support secret management integrations like **1Password CLI** in the future. This will allow secrets to be pulled at runtime without requiring a static `.env` file.

> ℹ️ **Note:** For the time being, define these in a `.env` file for quick local use. This is a temporary approach — in future versions, secret management tools like 1Password will be supported for more secure credential handling.

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=your_db
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_pass
```

For COPY-based sources:

```env
SOURCE_HOST=...
SOURCE_PORT=...
SOURCE_DB=...
SOURCE_USER=...
SOURCE_PASSWORD=...
```

---

## 📁 Examples Directory

The `/examples` folder contains runnable tests and reference pipelines:

- `ftl_copy_out.py` — Demonstrates PostgreSQL-to-PostgreSQL streaming using `CopyProducer` and `CopyConsumer`.
- `ftl_csv_out.py` — Original CSV-to-PostgreSQL example, formerly `ftl_test.py`.
- `generate.py` — Script to generate synthetic test data for the `contacts` table in CSV format.
- `test_table.sql` — SQL statement to create the `public.contacts` table used in tests:

```sql
CREATE TABLE public.contacts (
	id int8 NOT NULL GENERATED ALWAYS AS IDENTITY,
	first_name varchar(64) NULL,
	last_name varchar(64) NULL,
	phone varchar(15) NULL,
	extension varchar(4) NULL,
	email varchar(254) NULL,
	subaddress varchar(64) NULL,
	CONSTRAINT contacts_pkey PRIMARY KEY (id)
);
```

---

## 📖 Logging

Logging is set up via the `logging` module with default level `INFO`. Adjust using:

```python
logger.setLevel(logging.DEBUG)
```

Or use an env var:

```env
FTL_LOG_LEVEL=DEBUG
```

---

## 🧼 Tip: Enforce Explicit Columns

Avoid `SELECT *` in `CopyProducer` queries — always use:

```sql
SELECT column1, column2 FROM ...
```

So that column alignment is predictable for COPY.

---

## ✅ Ready to Build

Pick your source, define your ETL class, plug in a consumer, and start streaming!
