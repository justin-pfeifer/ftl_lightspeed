def set_application_name(cur, suffix: str = "COPY Producer"):
    from ftl_lightspeed.version import __version__
    from psycopg import sql

    app_name = f"FTL Lightspeed v{__version__} - {suffix}"
    cur.execute(sql.SQL("SET application_name = {}").format(sql.Literal(app_name)))