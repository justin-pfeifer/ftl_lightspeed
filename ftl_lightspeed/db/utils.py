from psycopg import sql
from ftl_lightspeed.version import __version__
import re

def split_class_name(name: str) -> str:
    """
    Split PascalCase into a space-separated string, preserving known acronyms.
    For example:
    - "FTLProducer" → "FTL Producer"
    - "ETLRunner"   → "ETL Runner"
    - "ContactLoad" → "Contact Load"
    """
    # Known acronyms that should not be split
    acronyms = {"FTL", "ETL"}

    # Matches acronym followed by a capitalized word
    def preserve_acronyms(match):
        prefix, word = match.groups()
        return f"{prefix} {word}"

    # First pass: preserve acronyms followed by another capitalized word
    pattern = re.compile(rf"({'|'.join(acronyms)})([A-Z][a-z]+)")
    name = pattern.sub(preserve_acronyms, name)

    # Second pass: split remaining PascalCase parts
    name = re.sub(r'(?<!^)(?=[A-Z])', ' ', name)

    return name

def set_application_name(cur, mode: str = "Dev", job: str = "Generic FTL Job"):
    """
    Set a descriptive application_name in Postgres for tracking.

    :param cur: psycopg cursor
    :param mode: "Producer", "Consumer", "Dev", etc.
    :param job: short description or script name
    """
    label = f"FTL Lightspeed v{__version__} - [{mode}] {job}"
    cur.execute(sql.SQL("SET application_name = {}").format(sql.Literal(label)))