from psycopg import Connection, sql
from ftl_lightspeed import __version__
from ftl_lightspeed.db.utils import set_application_name


def toHeaders(headers: list[str]) -> str:
    """Convert a list of headers to a comma-separated string."""
    return ','.join([f'"{x}"' for x in headers])


class CSVProducer:
    file_path: str  # Set on subclass or dynamically before warp()

    def get_headers(self, path=None) -> list[str]:
        """Return the first line of the CSV as a list of column names."""
        target = path or self.file_path
        with open(target, newline='', encoding='utf-8') as f:
            import csv
            return toHeaders(next(csv.reader(f)))



class CopyProducer:
    source_conn: Connection
    query: str
    chunk_size: int = 64  # in bytes

    def stream(self):
        with self.source_conn.cursor() as cur:
            set_application_name(cur, "COPY Producer")
            copy_sql = sql.SQL("COPY ({}) TO STDOUT WITH CSV").format(sql.SQL(self.query))
            with cur.copy(copy_sql) as copy_out:
                buffer = bytearray()
                for chunk in copy_out:  # Each chunk is already bytes
                    buffer.extend(chunk)
                    if len(buffer) >= self.chunk_size*1024*1024:
                        yield bytes(buffer)
                        buffer.clear()

                # Yield any remaining buffer
                if buffer:
                    yield bytes(buffer)