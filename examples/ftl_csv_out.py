import os
import psycopg
from ftl_lightspeed.ftl import FTL
from ftl_lightspeed.ftl_consumer import CopyConsumer
from ftl_lightspeed.ftl_producer import CSVProducer
from dotenv import load_dotenv

load_dotenv()
conn = psycopg.connect(**{
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
})

class LightspeedFTL(CSVProducer, FTL):
    file_path = "contacts.csv"

    def fetch(self):
        self.headers = self.get_headers(self.file_path)
        self.consumer = CopyConsumer(conn, "public.contacts", self.headers)

    def transform(self):
        pass

    def load(self):
        self.consumer.consume(self.file_path, chunk_mb=512)

if __name__ == "__main__":
    LightspeedFTL().warp()