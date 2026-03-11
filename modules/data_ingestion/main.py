import pydantic
import datetime
import yfinance


class Asset(pydantic.BaseModel):
    symbol: str
    date: list[datetime.date] = []
    open: list[float] = []
    high: list[float] = []
    low: list[float] = []
    close: list[float] = []
    volume: list[int] = []

    def __str__(self):
        return f"Asset(symbol={self.symbol}, date={self.date}, open={self.open}, high={self.high}, low={self.low}, close={self.close}, volume={self.volume})"

    def add_data(
        self,
        date: datetime.date,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int,
    ):
        # Forward Fill
        if self.date:
            last_date = self.date[-1]
            gap = (date - last_date).days
            if gap > 1:
                last_close = self.close[-1]
                for i in range(1, gap):
                    fill_date = last_date + datetime.timedelta(days=i)
                    self.date.append(fill_date)
                    self.open.append(last_close)
                    self.high.append(last_close)
                    self.low.append(last_close)
                    self.close.append(last_close)
                    self.volume.append(0)
        self.date.append(date)
        self.open.append(open)
        self.high.append(high)
        self.low.append(low)
        self.close.append(close)
        self.volume.append(volume)


USEquities: list[Asset] = []

aapl = Asset(symbol="AAPL")
dataframe = yfinance.Ticker("AAPL")
history = dataframe.history(period="1mo")
for date, row in history.iterrows():
    aapl.add_data(
        date=date.date(),
        open=row["Open"],
        high=row["High"],
        low=row["Low"],
        close=row["Close"],
        volume=int(row["Volume"]),
    )

for i in range(len(aapl.date)):
    print(aapl.date[i], " ", aapl.open[i])

USEquities.append(aapl)
