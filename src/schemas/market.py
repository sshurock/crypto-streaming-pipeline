class Market:
    "Market data"
    def __init__(self, arr: List[str]):
        self.exchange_id = arr["id"]
        self.rank = arr["rank"]
        self.base_symbol = arr["baseSymbol"]
        self.base_id = arr["baseId"]
        self.quote_symbol = arr["quoteSymbol"]
        self.quote_id = arr["quoteId"]
        self.price_quote = arr["priceQuote"]
        self.price_usd = arr["priceUsd"]
        self.volume_usd_24hr = arr["volumeUsd24Hr"]
        self.percent_exchange_volume = arr["percentExchangeVolume"]
        self.trades_count_24hr = arr["tradesCount24Hr"]
        self.updated = arr["updated"]


# Example:
# "exchangeId": "bitstamp",
# "rank": "1",
# "baseSymbol": "BTC",
# "baseId": "bitcoin",
# "quoteSymbol": "USD",
# "quoteId": "united-states-dollar",
# "priceQuote": "6927.3300000000000000",
# "priceUsd": "6927.3300000000000000",
# "volumeUsd24Hr": "43341291.9576547008000000",
# "percentExchangeVolume": "67.2199253376108585",
# "tradesCount24Hr": "420721",
# "updated": 1533581033590