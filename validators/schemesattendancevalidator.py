from constants import constants


def find_matching_scheme(scheme, folio_data):
    for folio_data_scheme in folio_data[constants.KEY_SCHEMES]:
        if scheme[constants.KEY_ISIN] == folio_data_scheme[constants.KEY_ISIN]:
            return folio_data_scheme
    return None


class SchemesAttendanceValidator:
    def __init__(self, cas_data_by_pan: dict, current_data_by_pan: dict):
        self._cas_data_by_folios = {folio_num: folio_data for pan_data in cas_data_by_pan.values()
                                    for folio_num, folio_data in pan_data.items()}
        self._current_data_by_folios = {folio_num: folio_data for pan_data in current_data_by_pan.values()
                                        for folio_num, folio_data in pan_data.items()}
        self._cas_data_schemes_not_in_current_data_by_folios = {}
        self._current_data_schemes_not_in_cas_data_by_folios = {}

    def validate(self):
        self._fulfill_current_data_schemes_not_in_cas_data_by_folios()
        self._fulfill_cas_data_schemes_not_in_current_data_by_folios()

    def _fulfill_cas_data_schemes_not_in_current_data_by_folios(self):
        for cas_folio in self._cas_data_by_folios:
            for cas_folio_scheme in self._cas_data_by_folios[cas_folio][constants.KEY_SCHEMES]:
                matching_scheme = find_matching_scheme(cas_folio_scheme, self._current_data_by_folios[cas_folio])
                if not matching_scheme:
                    if cas_folio not in self._cas_data_schemes_not_in_current_data_by_folios:
                        self._cas_data_schemes_not_in_current_data_by_folios[cas_folio] = []
                    self._cas_data_schemes_not_in_current_data_by_folios[cas_folio].append(
                        cas_folio_scheme[constants.KEY_ISIN])

    def _fulfill_current_data_schemes_not_in_cas_data_by_folios(self):
        for current_data_folio in self._current_data_by_folios:
            for current_data_folio_scheme in self._current_data_by_folios[current_data_folio][constants.KEY_SCHEMES]:
                matching_scheme = find_matching_scheme(current_data_folio_scheme,
                                                       self._cas_data_by_folios[current_data_folio])
                if not matching_scheme:
                    if current_data_folio not in self._current_data_schemes_not_in_cas_data_by_folios:
                        self._current_data_schemes_not_in_cas_data_by_folios[current_data_folio] = []
                    self._current_data_schemes_not_in_cas_data_by_folios[current_data_folio].append(
                        current_data_folio_scheme[constants.KEY_ISIN])

    def fulfill_result(self, result):
        if not self._current_data_schemes_not_in_cas_data_by_folios and not self._fulfill_cas_data_schemes_not_in_current_data_by_folios:
            return
        if constants.VALIDATION_KEY_FOLIOS not in result:
            result[constants.VALIDATION_KEY_FOLIOS] = {}
        result_folios = result[constants.VALIDATION_KEY_FOLIOS]
        for folio in self._current_data_schemes_not_in_cas_data_by_folios:
            if folio not in result_folios:
                result_folios[folio] = {}
            for isin in self._current_data_schemes_not_in_cas_data_by_folios[folio]:
                result_folios[folio][isin] = {}
                result_folios[folio][isin][constants.VALIDATION_KEY_SCHEME_ATTENDANCE] = {}
                result_folios[folio][isin][constants.VALIDATION_KEY_SCHEME_ATTENDANCE][
                    constants.VALIDATION_KEY_STATUS_CODE] = constants.VALIDATION_STATUS_CODE_FAILURE
                result_folios[folio][isin][constants.VALIDATION_KEY_SCHEME_ATTENDANCE][
                    constants.VALIDATION_KEY_COMMENTS] = "Absent from CAS."

        for folio in self._cas_data_schemes_not_in_current_data_by_folios:
            if folio not in result_folios:
                result_folios[folio] = {}
            for isin in self._cas_data_schemes_not_in_current_data_by_folios[folio]:
                result_folios[folio][isin] = {}
                result_folios[folio][isin][constants.VALIDATION_KEY_SCHEME_ATTENDANCE] = {}
                result_folios[folio][isin][constants.VALIDATION_KEY_SCHEME_ATTENDANCE][
                    constants.VALIDATION_KEY_STATUS_CODE] = constants.VALIDATION_STATUS_CODE_FAILURE
                result_folios[folio][isin][constants.VALIDATION_KEY_SCHEME_ATTENDANCE][
                    constants.VALIDATION_KEY_COMMENTS] = "Absent from current data."

    def _find_errored_list_of_isin_for_folio(self, folio):
        list_of_isin = []
        if folio in self._cas_data_schemes_not_in_current_data_by_folios:
            list_of_isin.extend(self._cas_data_schemes_not_in_current_data_by_folios[folio])
        if folio in self._current_data_schemes_not_in_cas_data_by_folios:
            list_of_isin.extend(self._current_data_schemes_not_in_cas_data_by_folios[folio])
        return list_of_isin

    def get_valid_data(self, data):
        for pan in data:
            for folio in data[pan]:
                errored_list_of_isin = self._find_errored_list_of_isin_for_folio(folio)
                for scheme in data[pan][folio][constants.KEY_SCHEMES]:
                    if scheme in errored_list_of_isin:
                        data[pan][folio][constants.KEY_SCHEMES].remove(scheme)
        return data
