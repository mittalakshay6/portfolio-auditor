from constants import constants


class FolioValidator:
    def __init__(self, current_data_by_pan, cas_data_by_pan):
        self._foliosInCas = []
        self._foliosInCurrentPortfolio = []
        self._cas_folios_not_in_current_data = []
        self._current_data_folios_not_in_cas = []

        for pan in cas_data_by_pan:
            cas_pan_data = cas_data_by_pan[pan]
            self._foliosInCas.extend(cas_pan_data.keys())
        for pan in current_data_by_pan:
            current_pan_data = current_data_by_pan[pan]
            self._foliosInCurrentPortfolio.extend(current_pan_data.keys())

    def validate(self):
        self._fulfill_cas_folios_not_in_current_data()
        self._fulfill_current_data_folios_not_in_cas_data()

    def _fulfill_cas_folios_not_in_current_data(self):
        for pan in self._foliosInCas:
            if pan not in self._foliosInCurrentPortfolio:
                self._cas_folios_not_in_current_data.append(pan)

    def _fulfill_current_data_folios_not_in_cas_data(self):
        for pan in self._foliosInCurrentPortfolio:
            if pan not in self._foliosInCas:
                self._current_data_folios_not_in_cas.append(pan)

    def fulfill_result(self, result):
        if not self._current_data_folios_not_in_cas and not self._cas_folios_not_in_current_data:
            return
        result[constants.VALIDATION_KEY_FOLIOS] = {}
        result_folios = result[constants.VALIDATION_KEY_FOLIOS]
        for casFolio in self._cas_folios_not_in_current_data:
            result_folios[casFolio] = {}
            result_folios[casFolio][constants.VALIDATION_KEY_FOLIO_ATTENDANCE] = {}
            result_folios[casFolio][constants.VALIDATION_KEY_FOLIO_ATTENDANCE][
                constants.VALIDATION_KEY_STATUS_CODE] = constants.VALIDATION_STATUS_CODE_FAILURE
            result_folios[casFolio][constants.VALIDATION_KEY_FOLIO_ATTENDANCE][
                constants.VALIDATION_KEY_COMMENTS] = "Absent from current data"
        for currentFolio in self._current_data_folios_not_in_cas:
            result_folios[currentFolio] = {}
            result_folios[currentFolio][constants.VALIDATION_KEY_FOLIO_ATTENDANCE] = {}
            result_folios[currentFolio][constants.VALIDATION_KEY_FOLIO_ATTENDANCE][
                constants.VALIDATION_KEY_STATUS_CODE] = constants.VALIDATION_STATUS_CODE_FAILURE
            result_folios[currentFolio][constants.VALIDATION_KEY_FOLIO_ATTENDANCE][
                constants.VALIDATION_KEY_COMMENTS] = "Absent from CAS data"

    def get_invalid_folios(self):
        return set(self._current_data_folios_not_in_cas + self._cas_folios_not_in_current_data)

    def get_valid_data(self, data: dict):
        invalid_folios = self.get_invalid_folios()
        return {pan: {folio_num: folio_data for folio_num, folio_data in pan_data.items() if
                      folio_num not in invalid_folios}
                for pan, pan_data in data.items()}
