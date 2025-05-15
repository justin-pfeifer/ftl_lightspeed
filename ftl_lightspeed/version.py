from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("ftl_lightspeed")
except PackageNotFoundError:
    __version__ = "0.0.0"  # fallback during local dev without install