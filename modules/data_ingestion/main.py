import pydantic
import datetime
import yfinance

from enum import Enum


# Structs
class AssetEnum(Enum):
    Equity = 1
    Crypto = 2
    CommodityFutrues = 3
    Bond = 4
    FX = 5


class CurrencyEnum(Enum):
    USD = 1
    GBP = 2


class Portfolio(pydantic.BaseModel):
    symbol: list[str] = []
    assetType: list[AssetEnum] = []
    currencyType: list[CurrencyEnum] = []
    amount: list[float] = []

    def __str__(self):
        lines = [f"Portfolio(size={self.size()})"]
        for i in range(self.size()):
            lines.append(
                f"  {self.symbol[i]} | {self.assetType[i].name} | {self.currencyType[i].name} | {self.amount[i]}"
            )
        return "\n".join(lines)

    def size(self):
        return len(self.symbol)

    def push_back(
        self,
        symbol: str,
        assetType: AssetEnum,
        currencyType: CurrencyEnum,
        amount: float,
    ):
        self.symbol.append(symbol)
        self.assetType.append(assetType)
        self.currencyType.append(currencyType)
        self.amount.append(amount)


class AssetData(pydantic.BaseModel):
    symbol: str = ""
    date: list[datetime.date] = []
    open: list[float] = []
    high: list[float] = []
    low: list[float] = []
    close: list[float] = []
    volume: list[int] = []

    def __str__(self):
        lines = [f"AssetData(symbol={self.symbol}, rows={self.size()})"]
        for i in range(self.size()):
            lines.append(
                f"  {self.date[i]} | O:{self.open[i]:.2f} H:{self.high[i]:.2f}"
                f" L:{self.low[i]:.2f} C:{self.close[i]:.2f} V:{self.volume[i]}"
            )
        return "\n".join(lines)

    def size(self):
        return len(self.date)

    def push_back(
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
                # Use the close price of most recent day
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


# Main
def main():

    # User input
    portfolio = Portfolio()
    portfolio.push_back("AAPL", AssetEnum.Equity, CurrencyEnum.USD, 1.0)
    portfolio.push_back("GOOGL", AssetEnum.Equity, CurrencyEnum.USD, 1.0)
    portfolio.push_back("HSBA", AssetEnum.Equity, CurrencyEnum.GBP, 1.0)
    portfolio.push_back("GBP", AssetEnum.FX, CurrencyEnum.USD, 100.0)

    PERIOD = "5d"
    print(portfolio)
    print("")

    assetDataTable: list[AssetData] = []

    for i in range(portfolio.size()):
        if portfolio.assetType[i] == AssetEnum.Equity:
            if portfolio.currencyType[i] == CurrencyEnum.USD:
                try:
                    dataframe = yfinance.Ticker(portfolio.symbol[i])
                    history = dataframe.history(period=PERIOD)
                except Exception as e:
                    print(
                        f"[ERROR]: yfinance failed to fetch data for {portfolio.symbol[i]}: {e}"
                    )
                    continue

                assetData = AssetData(symbol=portfolio.symbol[i])
                for date, row in history.iterrows():
                    assetData.push_back(
                        date=date.date(),
                        open=row["Open"],
                        high=row["High"],
                        low=row["Low"],
                        close=row["Close"],
                        volume=int(row["Volume"]),
                    )
                assetDataTable.append(assetData)

            elif portfolio.currencyType[i] == CurrencyEnum.GBP:
                try:
                    ticker = yfinance.Ticker("GBPUSD=X")  # GBP -> USD
                    FX: float = ticker.info.get("regularMarketPrice")
                    multiplier: float = 0.01  # Pennies -> Pounds

                    tickerName = portfolio.symbol[i] + ".L"
                    dataframe = yfinance.Ticker(tickerName)
                    history = dataframe.history(period=PERIOD)
                except Exception as e:
                    print(
                        f"[ERROR]: yfinance failed to fetch data for {portfolio.symbol[i]}: {e}"
                    )
                    continue

                assetData = AssetData(symbol=portfolio.symbol[i])
                for date, row in history.iterrows():
                    assetData.push_back(
                        date=date.date(),
                        open=row["Open"] * multiplier * FX,
                        high=row["High"] * multiplier * FX,
                        low=row["Low"] * multiplier * FX,
                        close=row["Close"] * multiplier * FX,
                        volume=int(row["Volume"]),
                    )
                assetDataTable.append(assetData)
            else:
                print(
                    f"[ERROR]: Currency not supported for {portfolio.assetType[i]}: {portfolio.currencyType[i]}"
                )
                continue

        elif portfolio.assetType[i] == AssetEnum.FX:
            if portfolio.currencyType[i] == CurrencyEnum.USD:
                try:
                    tickerName = portfolio.symbol[i] + "USD=X"
                    dataframe = yfinance.Ticker(tickerName)
                    history = dataframe.history(period=PERIOD)
                except Exception as e:
                    print(
                        f"[ERROR]: yfinance failed to fetch data for {portfolio.symbol[i]}: {e}"
                    )
                    continue

                assetData = AssetData(symbol=portfolio.symbol[i])
                for date, row in history.iterrows():
                    assetData.push_back(
                        date=date.date(),
                        open=row["Open"],
                        high=row["High"],
                        low=row["Low"],
                        close=row["Close"],
                        volume=int(row["Volume"]),
                    )
                assetDataTable.append(assetData)

        else:
            print(
                f"[ERROR]: Asset not supported for {portfolio.assetType[i]}: Currency {portfolio.currencyType[i]}"
            )
            continue

    for i in range(len(assetDataTable)):
        print(assetDataTable[i])


if __name__ == "__main__":
    main()
