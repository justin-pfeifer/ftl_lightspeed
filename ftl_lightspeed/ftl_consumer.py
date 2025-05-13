import os
import logging

logger = logging.getLogger("ftl")
logger.setLevel(logging.INFO)  # Can override via env or main script

console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(console)

class BaseConsumer:
    mode: str = "base"

    def write(self, data):
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class CopyConsumer:
    def __init__(self, conn, table: str, columns: str, has_header: bool = True):
        self.conn = conn
        self.table = table
        self.columns = columns
        self.has_header = has_header
        self.cur = None
        self._context = None
        self.copy_in = None

    def __enter__(self):
        logger.info(f"[START] COPY -> {self.table}")
        self.cur = self.conn.cursor()
        header_clause = " HEADER" if self.has_header else ""
        copy_sql = f"COPY {self.table} ({self.columns}) FROM STDIN WITH CSV{header_clause}"
        self._context = self.cur.copy(copy_sql)
        self.copy_in = self._context.__enter__()
        return self

    def write(self, data: bytes):
        self.copy_in.write(data)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._context:
            self._context.__exit__(exc_type, exc_val, exc_tb)
        if self.cur:
            self.cur.close()

        if exc_type is None:
            try:
                self.conn.commit()
                logger.info("[COMMIT] Transaction committed successfully.")
            except Exception as e:
                logger.error(f"[ERROR] Commit failed: {e}")
                raise
        else:
            logger.warning("[ROLLBACK] Error occurred, rolling back.")
            self.conn.rollback()

    def consume(self, source, chunk_mb: int = 64):
        chunk_size = chunk_mb * 1024 * 1024

        if isinstance(source, str):
            file_size = os.path.getsize(source)
            with self as copy, open(source, "rb") as f:
                if file_size <= chunk_size:
                    data = f.read()
                    copy.write(data)
                    logger.info(f"[BATCH 1] Wrote {len(data)/(1024*1024):.2f}MB to {self.table} in one stream")
                else:
                    batch_num = 1
                    while chunk := f.read(chunk_size):
                        copy.write(chunk)
                        logger.info(f"[BATCH {batch_num}] Wrote {len(chunk)/(1024*1024):.2f}MB to {self.table}")
                        batch_num += 1

            logger.info(f"[DONE] Loaded {source} into {self.table}")

        elif hasattr(source, "stream"):
            with self as copy:
                batch_num = 1
                for chunk in source.stream():
                    copy.write(chunk)
                    logger.info(f"[BATCH {batch_num}] Wrote {len(chunk)/(1024*1024):.2f}MB to {self.table}")
                    batch_num += 1

            logger.info(f"[DONE] Loaded stream into {self.table}")

        else:
            raise ValueError("Invalid source passed to consume: must be file path or producer with stream()")

class BatchConsumer(BaseConsumer):
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.mode = "batch"

    def __call__(self, rows):
        raise NotImplementedError


class InsertConsumer(BatchConsumer):
    def __init__(self, conn, table: str, columns: list[str], batch_size=1000):
        super().__init__(batch_size)
        self.conn = conn
        self.table = table
        self.columns = columns

    def __call__(self, rows):
        placeholders = ",".join(["%s"] * len(self.columns))
        values_str = ",".join(f"({placeholders})" for _ in rows)
        flat_values = [item for row in rows for item in row]
        sql = f"INSERT INTO {self.table} ({', '.join(self.columns)}) VALUES {values_str}"
        with self.conn.cursor() as cur:
            cur.execute(sql, flat_values)


class UpsertConsumer(BatchConsumer):
    def __init__(self, conn, table: str, columns: list[str], conflict_columns: list[str], batch_size=1000):
        super().__init__(batch_size)
        self.conn = conn
        self.table = table
        self.columns = columns
        self.conflict_columns = conflict_columns

    def __call__(self, rows):
        placeholders = ",".join(["%s"] * len(self.columns))
        values_str = ",".join(f"({placeholders})" for _ in rows)
        flat_values = [item for row in rows for item in row]
        update_str = ", ".join(f"{col}=EXCLUDED.{col}" for col in self.columns if col not in self.conflict_columns)
        conflict_str = ", ".join(self.conflict_columns)
        sql = f"""
            INSERT INTO {self.table} ({', '.join(self.columns)}) 
            VALUES {values_str}
            ON CONFLICT ({conflict_str}) DO UPDATE 
            SET {update_str}
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, flat_values)
