# Library imports
import logging_config
import logging
import json

# User imports
from constants import dataSources
from constants import constants
from utilities import utils
from dataLoader import casDataLoader
from validators.foliovalidator import FolioValidator
from validators.panvalidator import PanValidator
from validators.schemesattendancevalidator import SchemesAttendanceValidator
from validators.schemesinvestmentvalidator import SchemesInvestmentValidator


def main():
    logging_config.setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Data Preparation")
    current_data = utils.load_current_data_from_csv(constants.MF_PORTFOLIO_SHEET_PATH)
    cas_jsons = casDataLoader.batch_load_data(data_sources=dataSources.CAS_SOURCES)
    current_data_by_pan = utils.restructure_current_data_by_pan(current_data)
    cas_data_by_pan = utils.restructure_cas_json(cas_jsons)
    utils.add_yahoo_symbol_to_cas_json_by_pan(cas_data_by_pan, current_data_by_pan)
    utils.fulfill_cas_data_for_missing_pans_from_current_data(cas_data_by_pan, current_data_by_pan)
    print('Data Preparation - Done')

    # Validations
    result_with_only_errors = {}

    pan_validator = PanValidator(current_data_by_pan, cas_data_by_pan)
    pan_validator.validate()
    pan_validator.fulfill_result(result_with_only_errors)
    valid_cas_data_by_pan = pan_validator.get_valid_data(cas_data_by_pan)
    valid_current_data_by_pan = pan_validator.get_valid_data(current_data_by_pan)

    folio_validator = FolioValidator(valid_current_data_by_pan, valid_cas_data_by_pan)
    folio_validator.validate()
    folio_validator.fulfill_result(result_with_only_errors)
    valid_cas_data_by_pan = folio_validator.get_valid_data(valid_cas_data_by_pan)
    valid_current_data_by_pan = folio_validator.get_valid_data(valid_current_data_by_pan)

    schemes_attendance_validator = SchemesAttendanceValidator(valid_cas_data_by_pan, valid_current_data_by_pan)
    schemes_attendance_validator.validate()
    schemes_attendance_validator.fulfill_result(result_with_only_errors)
    valid_cas_data_by_pan = schemes_attendance_validator.get_valid_data(valid_cas_data_by_pan)
    valid_current_data_by_pan = schemes_attendance_validator.get_valid_data(valid_current_data_by_pan)

    schemes_investment_validator = SchemesInvestmentValidator(valid_cas_data_by_pan, valid_current_data_by_pan)
    schemes_investment_validator.validate()
    schemes_investment_validator.fulfill_result(result_with_only_errors)

    logger.info(f'Result with only errors: {json.dumps(result_with_only_errors)}')
    logger.info(f'Writing to file({constants.RESULT_OUTPUT_PATH})...')
    with open (constants.RESULT_OUTPUT_PATH, 'w') as result_file:
        json.dump(result_with_only_errors, result_file)
    logger.info(f'Total CAS cost validated = {schemes_investment_validator.totalValidatedCasCost}, Total CAS value '
                f'validated = {schemes_investment_validator.totalValidatedCasValue}')


if __name__ == "__main__":
    main()
