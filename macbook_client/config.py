# Macbook Client Configuration

# Default server settings
DEFAULT_SERVER_HOST = "localhost"
DEFAULT_SERVER_PORT = 8080
DEFAULT_API_KEY = "your-secure-api-key-here"

# Request settings
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Cache settings
ENABLE_CLIENT_CACHE = True
CACHE_DIR = ".bloomberg_cache"
CACHE_TTL_MINUTES = 5

# Logging
LOG_FILE = "bloomberg_client.log"
LOG_LEVEL = "INFO"

# Export settings
DEFAULT_DATE_FORMAT = "%Y-%m-%d"
DEFAULT_CSV_ENCODING = "utf-8"

# Connection pool settings
CONNECTION_POOL_SIZE = 10
CONNECTION_KEEP_ALIVE = True