#!/usr/bin/env python  
# -*- coding: utf-8 -*-
import logging
from typing import Dict, Any

from freqtrade.configuration import setup_utils_configuration
from freqtrade.data.analysis import Analysis
from freqtrade.enums import RunMode
from freqtrade.exceptions import OperationalException

logger = logging.getLogger(__name__)


def start_analyse(args: Dict[str, Any]) -> None:
    config = setup_utils_configuration(args, RunMode.UTIL_EXCHANGE)
    logger.info("Data analyze starting.")
    obj = Analysis(config)
    obj.execute()
    logger.info("Data analyze finished.")
