# -*- coding: utf-8 -*-
"""
Class for analysing token correlation
"""
from pathlib import Path
from typing import Dict, Any, List

import arrow
import pandas as pd
import numpy as np
from copy import deepcopy
from pandas import DataFrame
import matplotlib.pyplot as plt
from tabulate import tabulate
import seaborn as sns

from freqtrade.data.preprocessing import DataPreprocessing
from freqtrade.exceptions import FileException


def text_table_analyze_results(results: List[Dict[str, Any]]) -> str:
    """
    Generates and returns a text table
    """

    headers = ["token", "btc_price"]
    floatfmt = ['s', '0.8f']
    output = [[
        t["token"], t["btc_price"]
    ] for t in results]
    return tabulate(output, headers=headers,
                    floatfmt=floatfmt, tablefmt="orgtbl", stralign="right")


class Analysis:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_preprocessing_obj = DataPreprocessing(self.config)
        self.file_path = self.get_datapreprocessing_file_path()

    def get_datapreprocessing_file_path(self):
        self.data_preprocessing_obj.execute()
        return self.data_preprocessing_obj.save_file_path

    def get_plot_file_path(self):
        path = Path(self.file_path)
        dir_path = path.parent
        file_name = "{}_plot.png".format(arrow.utcnow().int_timestamp)
        return dir_path.joinpath(file_name)

    def get_heat_map_file_path(self):
        path = Path(self.file_path)
        dir_path = path.parent
        file_name = "{}_heat_map.png".format(arrow.utcnow().int_timestamp)
        return dir_path.joinpath(file_name)

    def execute(self):
        # 1. check params
        self.check_params()

        # 2. format data
        # get btc dataframe
        df: DataFrame = pd.read_csv(self.file_path, index_col=0)
        price_usdt: DataFrame = deepcopy(df)
        price_btc = price_usdt.div(price_usdt["BTC"], axis="rows")
        price_btc_norm = price_btc / price_btc.fillna(method='bfill').iloc[0,]

        plt.figure()
        price_btc_norm.plot(figsize=(32, 12), grid=True, legend=True)
        plt.savefig(fname=self.get_plot_file_path())

        filtered_token = price_btc_norm.iloc[-1,].sort_values()[-8:]
        print(filtered_token)

        plt.subplots(figsize=(24, 24))
        sns.heatmap(price_btc.corr(), annot=True, vmax=1, square=True, cmap="Blues")
        plt.savefig(fname=self.get_heat_map_file_path())

    def check_params(self):
        # check file exist
        path = Path(self.file_path)
        if not path.exists() or not path.is_file():
            raise FileException("Data pre-processing file err. pls check file. path: {}".format(self.file_path))
