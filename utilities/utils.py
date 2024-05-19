import json
import logging
import csv
import pandas as pd

from constants import constants
from utilities.isintoyahoosymbolresolver import IsinToYahooSymbol

logger = logging.getLogger(__name__)


def format_folio_num_in_cas(folio_num_in_cas: str):
    """ Folio Numbers can contain spaces and trailing ' / 0', this function is to remove them.

    Args:
        folio_num_in_cas (String): Folio number in CAS

    Returns:
        String: Formatted folio number
    """
    if folio_num_in_cas.endswith('/ 0'):
        # Extract the part before '/ 0'
        extracted_data = folio_num_in_cas.split(' / ')[0]
        return extracted_data
    else:
        # Remove spaces and format the data
        return folio_num_in_cas.replace(' ', '')


def restructure_cas_json(cas_jsons: list):
    """ This function will construct the dictionary 'cas_json_by_pan' on the basis of data in 'cas_jsons'
    in usable format (cas_json_by_pan[pan][folio]['schemes'].extend(modified_schemes))

    Args:
        cas_jsons (List): List of CAS JSONs
    """
    logger.info("Restructuring CAS JSONs")
    cas_json_by_pan = {}
    for casJson in cas_jsons:
        for folioData in casJson.get('folios', []):
            pan = folioData.get('PAN')
            folio = format_folio_num_in_cas(folioData.get('folio'))
            schemes = folioData.get('schemes', [])
            modified_schemes = [
                {"scheme": scheme.get("scheme"), "valuation": scheme.get("valuation"), "isin": scheme.get("isin")} for
                scheme in schemes]
            for scheme in modified_schemes:
                scheme_valuation = scheme[constants.KEY_VALUATION]
                scheme_valuation[constants.KEY_COST] = float(scheme_valuation[constants.KEY_COST])
                scheme_valuation[constants.KEY_VALUE] = float(scheme_valuation[constants.KEY_VALUE])
            if pan not in cas_json_by_pan:
                cas_json_by_pan[pan] = {}
            if folio not in cas_json_by_pan[pan]:
                cas_json_by_pan[pan][folio] = {'schemes': modified_schemes}
            else:
                cas_json_by_pan[pan][folio]['schemes'].extend(modified_schemes)
    logger.info("Restructured CAS JSONs")
    return cas_json_by_pan


def load_current_data_from_csv(csv_path: str):
    """ Method to convert users data into a python dictionary which can be processed later.

    Args:
        csv_path (String): File path of the CSV

    Returns:
        Dictionary: Parsed dictionary
    """
    logger.info(f'Loading data from CSV at {csv_path}')
    current_data_list = []
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            current_data_list.append(row)
    logger.info(f'Loaded data from CSV at {csv_path}')
    return current_data_list


def restructure_current_data_by_pan(current_data_list: list):
    logger.info("Restructuring Current Data")
    # Remove last row, which is the resultant row in spreadsheet
    current_data_list.pop()
    current_data_by_pan = {}
    for row in current_data_list:
        transaction_type = row.get('Transaction Type')
        if transaction_type != 'Buy':
            continue
        owner = row.get('Owner')
        if owner == 'Charu Mittal':
            pan = 'ADYPM3720C'
        elif owner == 'Akshay Mittal':
            pan = 'DDMPM6612L'
        elif owner == 'Ramesh Chand':
            pan = 'ABUPC0136G'
        elif owner == 'Sanjay Kumar Mittal':
            pan = 'AAXPK9504E'
        else:
            continue
        if pan not in current_data_by_pan:
            current_data_by_pan[pan] = {}
        current_data_by_pan_pan = current_data_by_pan[pan]
        row_folio_num = row.get('Folio No.')
        if row_folio_num not in current_data_by_pan_pan:
            current_data_by_pan_pan[row_folio_num] = {'schemes': []}

        current_scheme_valuation = {
            constants.KEY_COST: get_amount_from_string_with_currency(row.get('Invested amount')),
            constants.KEY_VALUE: get_amount_from_string_with_currency(
                row.get('Present/Redeemed market value'))}
        current_scheme = {constants.KEY_YAHOO_SYMBOL: row.get('Symbol'), constants.KEY_ISIN: row.get('ISIN'),
                          constants.KEY_VALUATION: current_scheme_valuation}

        existing_schemes = current_data_by_pan_pan[row_folio_num].get(constants.KEY_SCHEMES)
        matching_existing_scheme = None
        for existingScheme in existing_schemes:
            existing_scheme_yahoo_symbol = existingScheme[constants.KEY_YAHOO_SYMBOL]
            current_scheme_yahoo_symbol = current_scheme[constants.KEY_YAHOO_SYMBOL]
            if existing_scheme_yahoo_symbol == current_scheme_yahoo_symbol:
                matching_existing_scheme = existingScheme
        if matching_existing_scheme is not None:
            matching_existing_scheme_valuation = matching_existing_scheme[constants.KEY_VALUATION]
            matching_existing_scheme_valuation[constants.KEY_COST] += current_scheme_valuation[constants.KEY_COST]
            matching_existing_scheme_valuation[constants.KEY_VALUE] += current_scheme_valuation[constants.KEY_VALUE]
        else:
            current_data_by_pan_pan[row_folio_num].get(constants.KEY_SCHEMES).append(current_scheme)
    logger.info("Restructured Current Data")
    return current_data_by_pan


def add_yahoo_symbol_to_cas_json_by_pan(cas_json_by_pan: dict, current_data_by_pan: dict):
    """ Users' spreadsheet is expected to contain a mapping of Yahoo symbol with ISIN. This function will populate
    this mapping into the parsed CAS data dictionary so that same records from both data sources can be identified
    for later verification.

    Args:
        cas_json_by_pan (Dictionary): Parsed CAS data
        current_data_by_pan (Dictionary): Parsed user data
    """
    logger.info("Adding Yahoo Symbols to CAS JSON")
    list_of_isin = []
    cas_schemes = get_all_schemes_from_data(cas_json_by_pan)
    for cas_scheme in cas_schemes:
        isin = cas_scheme[constants.KEY_ISIN]
        if isin is None:
            logger.error(f'No ISIN for {cas_scheme}')
            continue
        list_of_isin.append(cas_scheme[constants.KEY_ISIN])
    current_data_schemes = get_all_schemes_from_data(current_data_by_pan)
    isin_to_yahoo_resolver = IsinToYahooSymbol()
    isin_to_yahoo_resolver.resolve_list_of_isin_to_yahoo_symbols(isin_list=list_of_isin,
                                                                 user_data_schemes=current_data_schemes)
    isin_to_yahoo_resolver.populate_yahoo_symbols_in_cas_data(cas_schemes=cas_schemes)


def get_all_schemes_from_data(data: dict) -> list:
    cas_schemes = []
    # Add all schemes in cas data into a list
    for pan in data:
        cas_json_by_pan_pan = data[pan]
        for folio in cas_json_by_pan_pan:
            cas_schemes.extend(cas_json_by_pan_pan[folio]['schemes'])
    return cas_schemes


def fulfill_cas_data_for_missing_pans_from_current_data(cas_data_by_pan, current_data_by_pan):
    """ It has been observed that in some cases, CAS data does not contain users' PAN, in such cases,
    we assume that the PAN present in user's data against the corresponding folio is correct and will populate
    the same in parsed CAS data.

    Args:
        cas_data_by_pan (Dictionary): Parsed CAS data
        current_data_by_pan (Dictionary): Parsed user's data
    """
    cas_missing_pan_data = cas_data_by_pan['']
    resolved_cas_data_for_missing_pans = {}
    for casFolioNum in cas_missing_pan_data:
        for pan in current_data_by_pan:
            current_pan_data = current_data_by_pan[pan]
            for currentFolioNum in current_pan_data:
                if currentFolioNum == casFolioNum:
                    if pan not in resolved_cas_data_for_missing_pans:
                        resolved_cas_data_for_missing_pans[pan] = {}
                    resolved_cas_data_for_missing_pans[pan][casFolioNum] = cas_missing_pan_data[casFolioNum]
    for pan in resolved_cas_data_for_missing_pans:
        missing_pan_folio_data = resolved_cas_data_for_missing_pans[pan]
        for folioNum in missing_pan_folio_data:
            cas_data_by_pan[pan][folioNum] = missing_pan_folio_data[folioNum]
            cas_data_by_pan[''].pop(folioNum)
    if not bool(cas_data_by_pan['']):
        cas_data_by_pan.pop('')


def get_amount_from_string_with_currency(string_with_currency):
    """ Users' data might have formatted monetary values. (Might start with '₹' and have commas).
    This function will remove these characters and return the value.

    Args:
        string_with_currency (String): Formatted monetary value in users' data.

    Returns:
        Float: Value after removing unnecessary characters.
    """
    string_without_currency = string_with_currency.replace('₹', '').replace(',', '')
    amount = float(string_without_currency)
    return amount


def add_value_to_dictionary(dictionary, keys, value):
    last_nested_dictionary = dictionary
    for key in keys[:-1]:
        if key not in last_nested_dictionary:
            last_nested_dictionary[key] = {}
        last_nested_dictionary = last_nested_dictionary[key]
    last_nested_dictionary[keys[-1]] = value


def dict_to_tuple(d):
    """
        Recursively converts a dictionary to a tuple of key-value pairs,
        replacing nested dictionaries with tuples recursively.
    """
    return tuple((k, dict_to_tuple(v) if isinstance(v, dict) else v) for k, v in d.items())


def tuple_to_dict(t):
    """
        Recursively converts a tuple of key-value pairs to a dictionary,
        replacing nested tuples with dictionaries recursively.
    """
    return {k: tuple_to_dict(v) if isinstance(v, tuple) else v for k, v in t}
