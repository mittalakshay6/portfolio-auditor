from constants import constants


class PanValidator:
    def __init__(self, current_data_by_pan, cas_data_by_pan):
        self._current_data_by_pan = current_data_by_pan
        self._cas_data_by_pan = cas_data_by_pan
        self._current_data_pan_numbers = current_data_by_pan.keys()
        self._cas_pan_numbers = cas_data_by_pan.keys()
        self._cas_pan_numbers_not_in_current_data = []
        self._current_pan_numbers_not_in_cas_data = []

    def validate(self):
        self._fulfill_cas_pan_not_in_current_data()
        self._fulfill_current_data_pan_not_in_cas_data()

    def _fulfill_cas_pan_not_in_current_data(self):
        for pan in self._cas_pan_numbers:
            if pan not in self._current_data_pan_numbers:
                self._cas_pan_numbers_not_in_current_data.append(pan)

    def _fulfill_current_data_pan_not_in_cas_data(self):
        for pan in self._current_data_pan_numbers:
            if pan not in self._cas_pan_numbers:
                self._current_pan_numbers_not_in_cas_data.append(pan)

    def fulfill_result(self, result):
        if not self._current_pan_numbers_not_in_cas_data and not self._cas_pan_numbers_not_in_current_data:
            return
        result[constants.VALIDATION_KEY_PAN] = {}
        result_pan = result[constants.VALIDATION_KEY_PAN]
        if self._cas_pan_numbers_not_in_current_data:
            for pan in self._cas_pan_numbers_not_in_current_data:
                if pan == '':
                    continue
                result_pan[pan] = {}
                result_pan[pan][constants.VALIDATION_KEY_PAN_ATTENDANCE] = {}
                result_pan[pan][constants.VALIDATION_KEY_PAN_ATTENDANCE][
                    constants.VALIDATION_KEY_STATUS_CODE] = constants.VALIDATION_STATUS_CODE_FAILURE
                result_pan[pan][constants.VALIDATION_KEY_PAN_ATTENDANCE][
                    constants.VALIDATION_KEY_COMMENTS] = "Absent from current data"

        if self._current_pan_numbers_not_in_cas_data:
            for pan in self._current_pan_numbers_not_in_cas_data:
                if pan not in result_pan:
                    result_pan[pan] = {}
                    result_pan[pan][constants.VALIDATION_KEY_PAN_ATTENDANCE] = {}
                    result_pan[pan][constants.VALIDATION_KEY_PAN_ATTENDANCE][
                        constants.VALIDATION_KEY_STATUS_CODE] = constants.VALIDATION_STATUS_CODE_FAILURE
                    result_pan[pan][constants.VALIDATION_KEY_PAN_ATTENDANCE][
                        constants.VALIDATION_KEY_COMMENTS] = "Absent from CAS data"

    def _get_invalid_pans(self):
        return set(self._cas_pan_numbers_not_in_current_data + self._current_pan_numbers_not_in_cas_data)

    def get_valid_data(self, data: dict):
        invalid_pans = self._get_invalid_pans()
        return {pan: pan_data for pan, pan_data in data.items() if pan not in invalid_pans}
