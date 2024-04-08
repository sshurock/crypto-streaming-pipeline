### URL
BITCOIN_HISTORY_URL: str =f"api.coincap.io/v2/assets/bitcoin/history?interval=d1"
ASSETS_URL: str = "api.coincap.io/v2/assets"
RATES_URL: str ="api.coincap.io/v2/rates"
EXCHANGES_URL: str ="api.coincap.io/v2/exchanges"
MARKETS_URL: str ="api.coincap.io/v2/markets"
CANDLES_URL: str ="api.coincap.io/v2/candles?exchange=poloniex&interval=h8&baseId=ethereum&quoteId=bitcoin" ##Specific to a market

### Topics
ASSETS_TOPIC: str = "assets-topic"
EXCHANGE_TOPIC: str = "exchanges-topic"

### Kafka Settings
BOOTSTRAP_SERVERS = ['localhost:9092']


##GCS
CREDENTIALS_FILE = None
GCS_BUCKET = None
PROJECT_ID = None

