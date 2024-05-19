from constants import constants
from utilities import utils


def _generate_scheme_to_scheme_mapped_dictionary_by_folio(cas_data_by_pan: dict, current_data_by_pan: dict):
    scheme_to_scheme_mapped_dictionary_by_folio = {}
    for pan in cas_data_by_pan:
        for folio in cas_data_by_pan[pan]:
            scheme_to_scheme_mapped_dictionary_by_folio[folio] = {}
            cas_schemes = cas_data_by_pan[pan][folio][constants.KEY_SCHEMES]
            current_data_schemes = current_data_by_pan[pan][folio][constants.KEY_SCHEMES]
            for cas_scheme in cas_schemes:
                if cas_scheme[constants.KEY_ISIN] is None:
                    continue
                for current_scheme in current_data_schemes:
                    if current_scheme[constants.KEY_YAHOO_SYMBOL] == cas_scheme[constants.KEY_YAHOO_SYMBOL]:
                        cas_scheme_tuple = utils.dict_to_tuple(cas_scheme)
                        scheme_to_scheme_mapped_dictionary_by_folio[folio][cas_scheme_tuple] = current_scheme
    return scheme_to_scheme_mapped_dictionary_by_folio


class SchemesInvestmentValidator:
    def __init__(self, cas_data_by_pan: dict, current_data_by_pan: dict):
        self.totalValidatedCasValue = 0.0
        self.totalValidatedCasCost = 0.0
        self.scheme_to_scheme_mapped_dictionary_by_folio = _generate_scheme_to_scheme_mapped_dictionary_by_folio(
            cas_data_by_pan,
            current_data_by_pan)
        self._errored_scheme_to_scheme_mapped_dictionary_by_folio = {}

    def validate(self):
        for folio in self.scheme_to_scheme_mapped_dictionary_by_folio:
            for cas_scheme_tuple, current_scheme in self.scheme_to_scheme_mapped_dictionary_by_folio[folio].items():
                cas_scheme = utils.tuple_to_dict(cas_scheme_tuple)
                if not self._validate_schemes(cas_scheme, current_scheme):
                    if folio not in self._errored_scheme_to_scheme_mapped_dictionary_by_folio:
                        self._errored_scheme_to_scheme_mapped_dictionary_by_folio[folio] = {}
                    cas_scheme_tuple = utils.dict_to_tuple(cas_scheme)
                    self._errored_scheme_to_scheme_mapped_dictionary_by_folio[folio][cas_scheme_tuple] = current_scheme

    def fulfill_result(self, result):
        if not self._errored_scheme_to_scheme_mapped_dictionary_by_folio:
            return
        for folio in self._errored_scheme_to_scheme_mapped_dictionary_by_folio:
            for cas_scheme_tuple, current_scheme in self._errored_scheme_to_scheme_mapped_dictionary_by_folio[folio].items():
                cas_scheme = utils.tuple_to_dict(cas_scheme_tuple)
                cas_scheme_cost = cas_scheme[constants.KEY_VALUATION][constants.KEY_COST]
                current_scheme_cost = current_scheme[constants.KEY_VALUATION][constants.KEY_COST]
                cas_scheme_value = cas_scheme[constants.KEY_VALUATION][constants.KEY_VALUE]
                current_scheme_value = current_scheme[constants.KEY_VALUATION][constants.KEY_VALUE]
                utils.add_value_to_dictionary(result,
                                              [constants.VALIDATION_KEY_FOLIOS, folio, cas_scheme[constants.KEY_ISIN],
                                               constants.VALIDATION_KEY_SCHEME_COST,
                                               constants.VALIDATION_KEY_STATUS_CODE],
                                              constants.VALIDATION_STATUS_CODE_FAILURE)
                utils.add_value_to_dictionary(result,
                                              [constants.VALIDATION_KEY_FOLIOS, folio, cas_scheme[constants.KEY_ISIN],
                                               constants.VALIDATION_KEY_SCHEME_COST, constants.VALIDATION_KEY_COMMENTS],
                                              f'Current data scheme cost: {current_scheme_cost}, CAS data scheme cost: {cas_scheme_cost}, Deflection allowed: {constants.VALIDATION_COST_DEFLECTION_ALLOWED_FACTOR}, Current data scheme value: {current_scheme_value}, CAS data scheme cost: {cas_scheme_value}, Deflection allowed: {constants.VALIDATION_VALUE_DEFLECTION_ALLOWED_FACTOR}')

    def _validate_schemes(self, cas_scheme, current_scheme):
        cas_scheme_cost = cas_scheme[constants.KEY_VALUATION][constants.KEY_COST]
        current_scheme_cost = current_scheme[constants.KEY_VALUATION][constants.KEY_COST]
        cas_scheme_value = cas_scheme[constants.KEY_VALUATION][constants.KEY_VALUE]
        current_scheme_value = current_scheme[constants.KEY_VALUATION][constants.KEY_VALUE]
        return self.check_validation_criteria(cas_scheme_cost, current_scheme_cost, cas_scheme_value,
                                              current_scheme_value)

    def check_validation_criteria(self, cas_scheme_cost, current_scheme_cost, cas_scheme_value, current_scheme_value):
        return self.validate_cost(cas_scheme_cost, current_scheme_cost) and self.validate_value(cas_scheme_value,
                                                                                                current_scheme_value)

    def validate_cost(self, cas_scheme_cost, current_scheme_cost):
        if (cas_scheme_cost - (cas_scheme_cost * constants.VALIDATION_COST_DEFLECTION_ALLOWED_FACTOR)
                <= current_scheme_cost <=
                cas_scheme_cost + (cas_scheme_cost * constants.VALIDATION_COST_DEFLECTION_ALLOWED_FACTOR)):
            self.totalValidatedCasCost += cas_scheme_cost
            return True
        return False

    def validate_value(self, cas_scheme_value, current_scheme_value):
        if cas_scheme_value - (
                cas_scheme_value * constants.VALIDATION_VALUE_DEFLECTION_ALLOWED_FACTOR) <= current_scheme_value <= current_scheme_value + (
                cas_scheme_value * constants.VALIDATION_VALUE_DEFLECTION_ALLOWED_FACTOR):
            self.totalValidatedCasValue += cas_scheme_value
            return True
        return False
