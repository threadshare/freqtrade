"""
Functions to pre-processing data
"""
import json
import logging
import os.path
from typing import List, Optional, Dict, Any

import arrow
import pandas as pd
from pandas import DataFrame

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
        self.expanded_pairs: List[str] = expand_pairlist(self.pairs, list(self.exchange.markets))

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
        not_supported_pairs = []
        for pair in self.pairs:
            pair_list = pair.split("/")
            if len(pair_list) != 2 or pair_list[1] != "USDT":
                not_supported_pairs.append(pair)
                raise ParamsException("pair invalid pair: {}".format(pair))
        if not not_supported_pairs:
            logger.warning("Data pre-processing only support */USDT pair. this pair: {} will not format".format(
                json.dumps(not_supported_pairs)))
            self.pairs = [item for item in self.pairs if item not in not_supported_pairs]
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

    @staticmethod
    def _valid_pair(pair: str) -> bool:
        if len(pair.split('/')) != 2:
            return False
        return True

    def processing_data_to_csv_file(self):
        data: Dict[str, DataFrame] = load_data(self.datadir, self.timeframe, self.expanded_pairs,
                                               timerange=self.timerange)

        df: Dict[str, DataFrame] = {pair: item.loc[:, ["date", "close"]] for pair, item in data.items()}
        file_path = self.save_file_path()
        if len(df) == 1:
            for pair, item in df.items():
                if not self._valid_pair(pair):
                    logger.warning("Invalid Pair: {}".format(pair))
                    continue
                base_name = pair.split("/")[0]
                item.rename(columns={"close": base_name}, inplace=True)
                item.set_index("date", inplace=True)
                item.to_csv(file_path)
                return

        if len(df) > 1:
            columns = [str(item).split("/")[0] or None for item in df.keys()]
            df_items = [item for item in df.values()]
            i = 1
            res = df_items[0]
            res.rename(columns={"close": columns[0]}, inplace=True)

            while len(df_items) > i:
                df_items[i].rename(columns={"close": columns[i]}, inplace=True)
                res = pd.merge(res, df_items[i], on="date", how="left")
                i += 1
            res.set_index("date", inplace=True)
            res.to_csv(file_path)
        logger.info("save csv file path: {}".format(file_path))

    def save_file_path(self):
        return os.path.join(self.datadir,
                            "{}_data_preprocessing.csv".format(arrow.utcnow().int_timestamp))

    def execute(self) -> None:
        # check config
        self.check_params()
        # init exchange and refresh data
        self.check_and_refresh_exchange_data()
        # format data and save to csv file
        self.processing_data_to_csv_file()
        logger.info("data pre-processing finished")
