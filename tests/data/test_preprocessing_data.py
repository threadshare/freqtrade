from freqtrade.data.preprocessing import DataPreprocessing
from tests.conftest import log_has, log_has_re


def test_preprocessing_data_to_file(testdatadir, caplog) -> None:
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
    assert log_has('data pre-processing finished', caplog)
