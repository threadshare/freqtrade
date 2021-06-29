from freqtrade.configuration import TimeRange
from freqtrade.data.preprocessing import preprocessing_data_to_file


def test_preprocessing_data_to_file( testdatadir) -> None:
    timerange = TimeRange.parse_timerange("20180110-20180130")
    a = preprocessing_data_to_file(datadir=testdatadir, timeframe='5m', pairs=['UNITTEST/BTC', 'ETC/BTC'], timerange=timerange)
    pass
