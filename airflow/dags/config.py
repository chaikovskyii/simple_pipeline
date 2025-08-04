import os


class DatabaseConfig:
    DEFAULT_BATCH_SIZE = int(os.getenv('DB_BATCH_SIZE', '1000'))
    MAX_CONNECTIONS = int(os.getenv('DB_MAX_CONNECTIONS', '10'))
    CONNECTION_TIMEOUT = int(os.getenv('DB_CONNECTION_TIMEOUT', '30'))


class APIConfig:
    REQUEST_TIMEOUT = int(os.getenv('API_REQUEST_TIMEOUT', '10'))
    MAX_RETRIES = int(os.getenv('API_MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('API_RETRY_DELAY', '2'))
    CACHE_DURATION = int(os.getenv('CURRENCY_CACHE_DURATION', '3600'))


class ETLConfig:
    MAX_RUNTIME_MINUTES = int(os.getenv('ETL_MAX_RUNTIME_MINUTES', '30'))
    MIN_CURRENCY_RATE = float(os.getenv('MIN_CURRENCY_RATE', '0.001'))
    ORDERS_BATCH_SIZE = int(os.getenv('ORDERS_BATCH_SIZE', '5000'))