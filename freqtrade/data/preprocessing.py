"""
Functions to pre-processing data
"""
import logging
from pathlib import Path
from typing import List, Optional

from freqtrade.configuration import TimeRange
from freqtrade.data.history import load_data

logger = logging.getLogger(__name__)


def preprocessing_data_to_file(datadir: Path,
                               timeframe: str,
                               pairs: List[str],
                               info_pair: str="",
                               timerange: Optional[TimeRange] = None) -> bool:
    if not timerange:
        logger.warning("preprocessing_data_to_file: timerange params is required")
        return False
    if not pairs:
        return False
    # TODO convert
    data = load_data(datadir, timeframe, pairs, timerange=timerange)
    df = {pair: item.loc[:, ["date", "close"]] for pair, item in data.items()}
    columns = [str(item).split("/")[0] for item in df.keys()]
    pair_list = [pair for pair in data.keys()]
    left = df[pair_list[0]].set_index("date")
    left.columns = [pair_list[0]]
    # left.set_index("date")
    right = df[pair_list[1]].set_index("date")
    right.columns=[pair_list[1]]
    # right.set_index("date")
    new_df = left.join(right, on="date")
    pass
    import pandas as pd

    # a = pd.DataFrame()
    # a.join()
    # a.set_index()
    # a.columns
