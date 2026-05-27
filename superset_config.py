import os

# ─── Основные настройки Superset ───────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change_me")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db"
)

# Включаем поддержку кэша через Redis
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_URL": os.environ.get("REDIS_URL", "redis://superset-redis:6379/0"),
}

DATA_CACHE_CONFIG = CACHE_CONFIG

# Разрешаем iframe (нужно для встраивания дашбордов)
WTF_CSRF_ENABLED = True
WTF_CSRF_EXEMPT_LIST = []
TALISMAN_ENABLED = False

# Включаем публичный роль для просмотра без логина (опционально)
# PUBLIC_ROLE_LIKE = "Gamma"

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}
