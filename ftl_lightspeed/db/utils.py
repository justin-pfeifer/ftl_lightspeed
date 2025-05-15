from psycopg import sql
from ftl_lightspeed.version import __version__
import re

def split_class_name(name: str) -> str:
    """
    Split a class name into a more readable format.
    For example, "MyClassName" becomes "My Class Name".
    """
    return re.sub(r'(?<!^)(?=[A-Z])', ' ', name)

def set_application_name(cur, mode: str = "Dev", job: str = "Generic FTL Job"):
    """
    Set a descriptive application_name in Postgres for tracking.

    :param cur: psycopg cursor
    :param mode: "Producer", "Consumer", "Dev", etc.
    :param job: short description or script name
    """
    label = f"FTL Lightspeed v{__version__} - [{mode}] {job}"
    cur.execute(sql.SQL("SET application_name = {}").format(sql.Literal(label)))