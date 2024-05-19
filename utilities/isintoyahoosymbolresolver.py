import requests
import yfinance as yf
import logging
import json

from constants import constants
from utilities import data_parser_utils


class IsinToYahooSymbol:

    def __init__(self) -> None:
        self._cache_hit_count = 0
        self._cache_miss_count = 0
        self._retrieved_from_user_data_count = 0
        self._cache = {}
        self._failed = []
        self._logger = logging.getLogger(self.__class__.__name__)
        # Load cache from file
        try:
            with open(constants.LOCAL_ISIN_TO_YAHOO_SYMBOL_CACHE_FILE, 'r') as cache_file:
                self._cache = json.load(cache_file)
        except FileNotFoundError:
            self._logger.warning(f'Local cache file: {constants.LOCAL_ISIN_TO_YAHOO_SYMBOL_CACHE_FILE} not found, '
                                 f'initialised empty in memory cache.')

    def __resolve_isin_to_yahoo_symbol_from_ticker__(self, isin: str):
        try:
            if isin not in self._failed:
                if isin not in self._cache:
                    ticker = yf.Ticker(isin)
                    yahoo_symbol = ticker.info['symbol']
                    self._cache[isin] = yahoo_symbol
                    self._cache_miss_count += 1
                else:
                    self._cache_hit_count += 1
                return 0
        except requests.exceptions.HTTPError:
            self._failed.append(isin)
            return -1

    def resolve_list_of_isin_to_yahoo_symbols(self, isin_list: list, user_data_schemes: list):
        self._logger.info("Resolving list of isin to yahoo symbols")
        success = 0
        failed = 0
        for isin in isin_list:
            if self.__resolve_isin_to_yahoo_symbol_from_ticker__(isin) == 0:
                success += 1
            else:
                failed += 1
            self._logger.info(f'Total: {len(isin_list)}, Success: {success}, Failed: {failed}')
        if self._failed:
            self._logger.info(f'Resolving list of failed isin: {self._failed} to yahoo symbols from user data')
        for isin in self._failed:
            matching_scheme = data_parser_utils.get_matching_scheme_from_isin(isin, user_data_schemes)
            if not matching_scheme[constants.KEY_YAHOO_SYMBOL]:
                self._logger.error(f'Cannot resolve isin {isin} to Yahoo symbol.')
            else:
                self._logger.info(f'Resolved isin: {isin} to {matching_scheme[constants.KEY_YAHOO_SYMBOL]}')
                self._cache[isin] = matching_scheme[constants.KEY_YAHOO_SYMBOL]
                self._failed.remove(isin)
                self._retrieved_from_user_data_count += 1
        # Store cache in a file to reduce network calls to yahoo finance ticker
        with open(constants.LOCAL_ISIN_TO_YAHOO_SYMBOL_CACHE_FILE, 'w') as cache_file:
            json.dump(self._cache, cache_file, indent=4)
        self._logger.info(f'Total: {len(isin_list)}, Success: {success}, Failed: {failed}')
        self._logger.info(f'Finished resolving list of isin to yahoo symbols, cache hit: {self._cache_hit_count}, '
                          f'cache miss: {self._cache_miss_count}, retrieved from user data count: {self._retrieved_from_user_data_count}')

    def populate_yahoo_symbols_in_cas_data(self, cas_schemes: list):
        for cas_scheme in cas_schemes:
            if constants.KEY_YAHOO_SYMBOL not in cas_scheme and cas_scheme[constants.KEY_ISIN] in self._cache:
                cas_scheme[constants.KEY_YAHOO_SYMBOL] = self._cache[cas_scheme[constants.KEY_ISIN]]