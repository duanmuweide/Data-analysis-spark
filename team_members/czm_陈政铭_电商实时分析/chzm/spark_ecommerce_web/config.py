from pathlib import Path
import os

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


class Config:
    WEB_HOST = os.getenv("WEB_HOST", "127.0.0.1")
    WEB_PORT = env_int("WEB_PORT", 5000)
    FLASK_DEBUG = env_bool("FLASK_DEBUG", True)

    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
    DB_PORT = env_int("DB_PORT", 3306)
    DB_USER = os.getenv("DB_USER", "spark")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "spark123456")
    DB_NAME = os.getenv("DB_NAME", "spark_ecommerce")
    DB_CHARSET = os.getenv("DB_CHARSET", "utf8mb4")

    REALTIME_REFRESH_SECONDS = max(env_int("REALTIME_REFRESH_SECONDS", 5), 3)
