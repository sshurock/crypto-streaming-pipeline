from typing import List, Dict
class Exchange:
    "Exchange data"
    def __init__(self, arr: List[str], timestamp=None):
        self.exchangeId = arr["exchangeId"]
        self.name = arr["name"]
        self.rank = arr["rank"]
        self.percentTotalVolume = arr["percentTotalVolume"]
        self.volumeUsd = arr["volumeUsd"]
        self.tradingPairs = arr["tradingPairs"]
        self.socket = arr["socket"]
        self.exchangeUrl = arr["exchangeUrl"]
        self.updated = arr["updated"]
        if timestamp:
            self.timestamp = timestamp
        else:
            pass


# {
    #     "data": [
    #         {
    #             "id": "okex",
    #             "name": "Okex",
    #             "rank": "1",
    #             "percentTotalVolume": "21.379485735166293542000000000000000000",
    #             "volumeUsd": "616465445.1646260280799955",
    #             "tradingPairs": "22",
    #             "socket": false,
    #             "exchangeUrl": "https://www.okex.com/",
    #             "updated": 1536343139514
    #         },
