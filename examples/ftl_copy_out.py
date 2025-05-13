import os
import psycopg
from ftl import FTL
from ftl_consumer import CopyConsumer
from ftl_producer import CopyProducer, toHeaders
from dotenv import load_dotenv

load_dotenv()
conn = psycopg.connect(**{
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
})

class LightspeedFTL(CopyProducer, FTL):
    source_conn = psycopg.connect(**{
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    })
    query = """
        SELECT
            first_name,
            last_name,
            phone,
            extension,
            email,
            subaddress
        FROM contacts
    """
    chunk_size = 512  # in bytes


    def fetch(self):
        headers = toHeaders([
            "first_name",
            "last_name",
            "phone",
            "extension",
            "email",
            "subaddress"
        ])
        self.consumer = CopyConsumer(conn, "public.contacts_v2", headers)

    def transform(self):
        pass

    def load(self):
        self.consumer.consume(self ,chunk_mb=512)

if __name__ == "__main__":
    LightspeedFTL().warp()