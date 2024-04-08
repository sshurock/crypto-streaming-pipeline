import datetime


@dataclass
class History:
    def __init__(self, arr: List[str]):
        self.priceUsd = arr["priceUsd"]
        self.time = arr["time"]
        self.date = arr["date"]