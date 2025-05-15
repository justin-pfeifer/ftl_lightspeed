from psycopg import sql
from ftl_lightspeed.version import __version__
import re

def split_class_name(name: str) -> str:
    """
    Split PascalCase into readable words while preserving acronyms like FTL or ETL.

    For example:
    - "FTL" → "FTL"
    - "FTLProducer" → "FTL Producer"
    - "UserETLJob" → "User ETL Job"
    """
    # First, split PascalCase into words
    parts = re.findall(r'[A-Z]+(?=[A-Z][a-z]|[0-9]|$)|[A-Z][a-z]*|[a-z]+|\d+', name)

    # Known acronyms to preserve (uppercased)
    acronyms = {"FTL", "ETL", "API", "SQL"}

    # Join with space, uppercasing known acronyms
    result = ' '.join(part if part.upper() not in acronyms else part.upper() for part in parts)

    return result

def set_application_name(cur, mode: str = "Dev", job: str = "Generic FTL Job"):
    """
    Set a descriptive application_name in Postgres for tracking.

    :param cur: psycopg cursor
    :param mode: "Producer", "Consumer", "Dev", etc.
    :param job: short description or script name
    """
    label = f"FTL Lightspeed v{__version__} - [{mode}] {split_class_name(job)}"
    cur.execute(sql.SQL("SET application_name = {}").format(sql.Literal(label)))