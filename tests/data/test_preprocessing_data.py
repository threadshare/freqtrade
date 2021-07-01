from freqtrade.data.preprocessing import DataPreprocessing


def test_preprocessing_data_to_file(testdatadir) -> None:
    custom_config = {
        "exchange": {
            "name": "binance"
        },
        "timeframe": "1d",
        "timerange": "20210101-20210130",
        "pairs": ["1INCH/USDT", "AAVE/USDT"],
        "dataformat_ohlcv": "json",
        "datadir": testdatadir,
        "dry_run": True,
        "stake_currency": "USDT"
    }
    obj = DataPreprocessing(custom_config)
    obj.execute()
