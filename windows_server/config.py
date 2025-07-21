# Windows Server Configuration

# API設定
API_HOST = "0.0.0.0"  # 全てのネットワークインターフェースで待ち受け
API_PORT = 8080
API_KEY = "your-secure-api-key-here"  # 本番環境では強力なキーに変更

# Bloomberg設定
BLOOMBERG_HOST = "localhost"
BLOOMBERG_PORT = 8194

# キャッシュ設定
CACHE_TTL_SECONDS = 300  # 5分間キャッシュ
MAX_CACHE_SIZE = 1000

# ログ設定
LOG_FILE = "bloomberg_api_server.log"
LOG_LEVEL = "INFO"

# レート制限
RATE_LIMIT_PER_MINUTE = 60

# モックモード設定
MOCK_DATA_VARIANCE = 0.02  # モックデータの変動幅（2%）

# CORS設定（本番環境では特定のオリジンのみ許可）
ALLOWED_ORIGINS = ["*"]  # 開発環境用。本番では ["http://macbook-ip:port"] など

# リクエスト制限
MAX_SECURITIES_PER_REQUEST = 100
MAX_FIELDS_PER_REQUEST = 50
MAX_DATE_RANGE_DAYS = 3650  # 10年

# タイムアウト設定
REQUEST_TIMEOUT_SECONDS = 30
BLOOMBERG_TIMEOUT_MS = 30000