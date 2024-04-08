

from typing import List, Dict
class Assets:
    def __init__(self, arr: List[str], timestamp=None):
        self.id = arr["id"]
        self.rank = arr["rank"]
        self.symbol = arr["symbol"]
        self.name = arr["name"]
        self.supply = arr["supply"]
        self.maxSupply = arr["maxSupply"]
        self.marketCapUsd = arr["marketCapUsd"]
        self.volumeUsd24Hr = arr["volumeUsd24Hr"]
        self.priceUsd = arr["priceUsd"]
        self.changePercent24Hr = arr["changePercent24Hr"]
        self.vwap24Hr = arr["vwap24Hr"]
        self.explorer = arr["explorer"]
        if timestamp:
            self.timestamp = timestamp
        else:
            pass

        ##TODO: Resolve timestamp
# Example:
# "id": "bitcoin",
# "rank": "1",
# "symbol": "BTC",
# "name": "Bitcoin",
# "supply": "17193925.0000000000000000",
# "maxSupply": "21000000.0000000000000000",
# "marketCapUsd": "119150835874.4699281625807300",
# "volumeUsd24Hr": "2927959461.1750323310959460",
# "priceUsd": "6929.8217756835584756",
# "changePercent24Hr": "-0.8101417214350335",
# "vwap24Hr": "7175.0663247679233209"