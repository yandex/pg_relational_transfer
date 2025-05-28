import importlib
from os import environ

from dotenv import load_dotenv

from .default import Settings


load_dotenv()

env = environ.get("ENV", "local")
module = importlib.import_module(name=f"src.config.{env}")
settings: Settings = module.Settings()


__all__ = ["settings"]
