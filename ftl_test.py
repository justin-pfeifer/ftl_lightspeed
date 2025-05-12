import os
import psycopg
from ftl import FTL
from pg_consumer import CopyConsumer
from dotenv import load_dotenv

load_dotenv()
conn = psycopg.connect(**{
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
})

class CSVProducer:
    file_path: str  # Set on subclass or dynamically before warp()

    def get_headers(self, path=None) -> list[str]:
        """Return the first line of the CSV as a list of column names."""
        target = path or self.file_path
        with open(target, newline='', encoding='utf-8') as f:
            import csv
            return ','.join([f'"{x}"' for x in next(csv.reader(f))])

class LightspeedFTL(CSVProducer, FTL):
    file_path = "contacts3.csv"

    def fetch(self):
        self.headers = self.get_headers(self.file_path)
        self.consumer = CopyConsumer(conn, "public.contacts", self.headers)

    def transform(self):
        print(f"Transforming file: {self.file_path}")

    def load(self):
        self.consumer.consume(self.file_path, chunk_mb=512)

if __name__ == "__main__":
    LightspeedFTL().warp()