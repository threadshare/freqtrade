"""
Functions to pre-processing data
"""
import logging
from typing import List, Optional, Dict, Any

from freqtrade.configuration import TimeRange
from freqtrade.data.history import load_data, refresh_backtest_ohlcv_data
from freqtrade.exceptions import ParamsException, OperationalException
from freqtrade.exchange import Exchange
from freqtrade.plugins.pairlist.pairlist_helpers import expand_pairlist
from freqtrade.resolvers import ExchangeResolver

logger = logging.getLogger(__name__)


class DataPreprocessing:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.exchange_name = config['exchange']['name']
        self.timerange: Optional[TimeRange] = self.get_timerange()
        self.timeframe = config['timeframe']
        self.pairs: Optional[List[str]] = config['pairs']
        self.dataformat_ohlcv = config['dataformat_ohlcv']
        self.datadir = config['datadir']
        self.exchange: Optional[Exchange] = ExchangeResolver.load_exchange(exchange_name=self.exchange_name,
                                                                           config=self.config,
                                                                           validate=False)
        self.expanded_pairs: Optional[List[str]] = expand_pairlist(self.pairs, list(self.exchange.markets))

    def get_timerange(self):
        timerange = TimeRange()
        if 'timerange' in self.config:
            timerange = timerange.parse_timerange(self.config['timerange'])
        return timerange

    def check_params(self):
        if 'timeframe' not in self.config:
            raise ParamsException("Data pre-processing requires timefram. "
                                  "Please check the documention on how to configure this.")

        if 'pairs' not in self.config:
            raise OperationalException(
                "Data pre-processing requires a list of pairs. "
                "Please check the documentation on how to configure this.")
        # TODO now just support XXX/USDT pair To facilitate analysis and research
        for pair in self.pairs:
            pair_list = pair.split("/")
            if len(pair_list) != 2 or pair_list[1] != "USDT":
                raise ParamsException("pair invalid pair: {}".format(pair))
        # validate config pairs
        self.exchange.validate_pairs(self.pairs)
        # check timeframe
        self.exchange.validate_timeframes(self.timeframe)

    def check_and_refresh_exchange_data(self):
        # download data which pair is miss
        pairs_not_available = refresh_backtest_ohlcv_data(
            self.exchange, pairs=self.expanded_pairs, timeframes=[self.timeframe],
            datadir=self.datadir, timerange=self.timerange,
            new_pairs_days=self.timerange.timerange2days(),
            erase=False, data_format=self.dataformat_ohlcv)
        if pairs_not_available:
            logger.warning(f"Pairs [{','.join(pairs_not_available)}] not available "
                           f"on exchange {self.exchange.name}.")

    def processing_data_to_csv_file(self) -> None:
        data = load_data(self.datadir, self.timeframe, timerange=self.timerange)

        df = {pair: item.loc[:, ["date", "close"]] for pair, item in data.items()}
        columns = [str(item).split("/")[0] for item in df.keys()]
        pair_list = [pair for pair in data.keys()]
        left = df[pair_list[0]].set_index("date")
        left.columns = [pair_list[0]]
        # left.set_index("date")
        right = df[pair_list[1]].set_index("date")
        right.columns = [pair_list[1]]
        # right.set_index("date")
        new_df = left.join(right, on="date")
        # import pandas as pd

        # a = pd.DataFrame()
        # a.join()
        # a.set_index()
        # a.columns

    def execute(self) -> None:
        # check config
        self.check_params()
        # init exchange and refresh data
        self.check_and_refresh_exchange_data()
        # format data and save to csv file
        self.processing_data_to_csv_file()
