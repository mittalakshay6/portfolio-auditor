from copy import deepcopy
import requests
import casparser
import json
import csv
import yfinance as yf
import pandas as pd

CAS_SOURCES = [
    {
    'email': 'mittalcharu469@gmail.com',
    'path': '/Users/akshay.m/Downloads/mittalcharu469@gmail.com.pdf',
    'password': "Akshay@1997"
},
{
    'email': 'mittalakshay6@gmail.com',
    'path': '/Users/akshay.m/Downloads/mittalakshay6@gmail.com.pdf',
    'password': 'Akshay@1997'
},
{
    'email': 'sanjaykumar6838@gmail.com',
    'path': '/Users/akshay.m/Downloads/sanjaykumar6838@gmail.com.pdf',
    'password': 'Akshay@1997'
}
]
MF_PORTFOLIO_SHEET_PATH = '/Users/akshay.m/Downloads/Portfolio/Mutual-funds-Mutual-fund-investments.csv'
RESULT_OUTPUT_PATH = '/Users/akshay.m/Downloads/result.csv'
VALIDATION_STATUS_CODE_SUCCESS = 0
VALIDATION_STATUS_CODE_FAILURE = 1
VALIDATION_STATUS_CODE_MISSED = 2
KEY_YAHOO_SYMBOL = 'yahooSymbol'
KEY_ISIN = 'isin'
KEY_FOLIO = 'folio'
KEY_VALUATION = 'valuation'
KEY_COST = 'cost'
KEY_VALUE = 'value'
KEY_SCHEMES = 'schemes'
VALIDATION_KEY_PAN = 'pan'
VALIDATION_KEY_FOLIOS = 'folios'
VALIDATION_KEY_PAN_ATTENDANCE = 'panAttendance'
VALIDATION_KEY_FOLIO_ATTENDANCE = 'folioAttendance'
VALIDATION_KEY_FOLIO_PAN_LINK = 'folioPanLinkage'
VALIDATION_KEY_CAS_FOLIO_IN_CURRENT_DATA_PRESENCE = 'casFolioInCurrentDataPresence'
VALIDATION_KEY_SCHEME_YAHOO_SYMBOL_PRESENCE = 'casYahooSymbolPresence'
VALIDATION_KEY_MATCHING_YAHOO_SYMBOL_PRESENCE = 'matchingYahooSymbolPresence'
VALIDATION_KEY_MATCHING_ISIN_PRESENCE = 'matchingIsinPresence'
VALIDATION_KEY_SCHEME_COST = 'schemeCostValidation'
VALIDATION_KEY_SCHEME_VALUE = 'schemeValueValidation'
VALIDATION_KEY_MISC = 'misc'
VALIDATION_KEY_CURRENT_DATA_MISSING_VALIDATIONS = 'currentDataMissingValidation'
VALIDATION_KEY_STATUS_CODE = 'statusCode'
VALIDATION_KEY_COMMENTS = 'comments'
VALIDATION_COST_DEFLECTION_ALLOWED_PRECENTAGE = 0.01
VALIDATION_VALUE_DEFLECTION_ALLOWED_PERCENTAGE = 0.01

def formatFolioNumInCas(folioNumInCas):
    if folioNumInCas.endswith('/ 0'):
        # Extract the part before '/ 0'
        extracted_data = folioNumInCas.split(' / ')[0]
        return extracted_data
    else:
        # Remove spaces and format the data
        return folioNumInCas.replace(' ', '')

def restuctureCASJson(casJsonByPan, casJson):
    for folioData in casJson.get('folios', []):
        pan = folioData.get('PAN')
        folio = formatFolioNumInCas(folioData.get('folio'))
        schemes = folioData.get('schemes', [])
        modifiedSchemes = [{"scheme": scheme.get("scheme"), "valuation": scheme.get("valuation"), "isin": scheme.get("isin")} for scheme in schemes]
        for scheme in modifiedSchemes:
            schemeValuation = scheme[KEY_VALUATION]
            schemeValuation[KEY_COST] = float(schemeValuation[KEY_COST])
            schemeValuation[KEY_VALUE] = float(schemeValuation[KEY_VALUE])
        if pan not in casJsonByPan:
            casJsonByPan[pan] = {}
        if folio not in casJsonByPan[pan]:
            casJsonByPan[pan][folio] = {'schemes': modifiedSchemes}
        else:
            casJsonByPan[pan][folio]['schemes'].extend(modifiedSchemes)

def getCurrentDataByPanFromSpreadsheet(spreadsheetPath):
    currentDataList = []
    with open(spreadsheetPath, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            currentDataList.append(dict(row))

    # Remove last row, which is the resultant row in spreadsheet
    currentDataList.pop()
    currentDataByPan = {}
    for row in currentDataList:
        transactionType = row.get('Transaction Type')
        if (transactionType != 'Buy'):
            continue
        owner = row.get('Owner')
        if (owner == 'Charu Mittal'):
            pan = 'ADYPM3720C'
        elif (owner == 'Akshay Mittal'):
            pan = 'DDMPM6612L'
        elif (owner == 'Ramesh Chand'):
            pan = 'ABUPC0136G'
        elif (owner == 'Sanjay Kumar Mittal'):
            pan = 'AAXPK9504E'
        else:
            continue
        if pan not in currentDataByPan:
            currentDataByPan[pan] = {}
        currentDataByPanPan = currentDataByPan[pan]
        rowFolioNum = row.get('Folio No.')
        if rowFolioNum not in currentDataByPanPan:
            currentDataByPanPan[rowFolioNum] = {'schemes': []}
        currentScheme = {}
        currentScheme[KEY_YAHOO_SYMBOL] = row.get('Symbol')
        currentScheme[KEY_ISIN] = row.get('ISIN')
        currentSchemeValuation = {}
        currentScheme[KEY_VALUATION] = currentSchemeValuation
        currentSchemeValuation[KEY_COST] = getAmountFromStringWithCurrency(row.get('Invested amount'))
        currentSchemeValuation[KEY_VALUE] = getAmountFromStringWithCurrency(row.get('Present/Redeemed market value'))

        existingSchemes = currentDataByPanPan[rowFolioNum].get(KEY_SCHEMES)
        matchingExistingScheme = None
        for existingScheme in existingSchemes:
            existingSchemeYahooSymbol = existingScheme[KEY_YAHOO_SYMBOL]
            currentSchemeYahooSymbol = currentScheme[KEY_YAHOO_SYMBOL]
            if existingSchemeYahooSymbol == currentSchemeYahooSymbol:
                matchingExistingScheme = existingScheme
        if matchingExistingScheme is not None:
            matchingExistingSchemeValuation = matchingExistingScheme[KEY_VALUATION]
            matchingExistingSchemeValuation[KEY_COST] += currentSchemeValuation[KEY_COST]
            matchingExistingSchemeValuation[KEY_VALUE] += currentSchemeValuation[KEY_VALUE]
        else:
            currentDataByPanPan[rowFolioNum].get(KEY_SCHEMES).append(currentScheme)
    return currentDataByPan

def addYahooSymbolToCasJsonByPan(casJsonByPan, currentDataByPan):
    schemes = []
    isin2ys = {}
    for pan in casJsonByPan:
        casJsonByPanPan = casJsonByPan[pan]
        for folio in casJsonByPanPan:
            schemes.extend(casJsonByPanPan[folio]['schemes'])
    total = len(schemes)
    success = 0
    failure = 0
    for scheme in schemes:
        isin = scheme.get('isin')
        yahooSymbol = isin2ys.get(isin)
        if yahooSymbol is None:
            try:
                ticker = yf.Ticker(isin)
                yahooSymbol = ticker.info['symbol']
                isin2ys[isin] = yahooSymbol
                success += 1
            except requests.exceptions.HTTPError:
                isin2ys[isin] = None
                failure += 1
        else:
            success += 1
        scheme['yahooSymbol'] = yahooSymbol
        print(f'Success: {success}, Failure: {failure}, Total processed: {success + failure}, Total: {total}')
    print("Trying to resolve failures, considering current data as source of truth")
    for pan in casDataByPan:
        casPanData = casDataByPan[pan]
        for folioNum in casPanData:
            casFolioData = casPanData[folioNum]
            for casScheme in casFolioData[KEY_SCHEMES]:
                if casScheme[KEY_YAHOO_SYMBOL] is None:
                    print(f'Searching Yahoo symbol for ISIN: {casScheme[KEY_ISIN]} in current data')
                    for currentScheme in currentDataByPan[pan][folioNum][KEY_SCHEMES]:
                        if casScheme[KEY_ISIN] == currentScheme[KEY_ISIN]:
                            print(f'Yahoo symbol: {currentScheme[KEY_YAHOO_SYMBOL]} found for ISIN: {casScheme[KEY_ISIN]}')
                            failure -= 1
                            success += 1
                            print(f'Success: {success}, Failure: {failure}, Total processed: {success + failure}, Total: {total}')
                            casScheme[KEY_YAHOO_SYMBOL] = currentScheme[KEY_YAHOO_SYMBOL]

def fulFillCasDataForMissingPansFromCurrentData(casDataByPan, currentDataByPan):
    casMissingPanData = casDataByPan['']
    resolvedCasDataForMissingPans = {}
    for casFolioNum in casMissingPanData:
        for pan in currentDataByPan:
            currentPanData = currentDataByPan[pan]
            for currentFolioNum in currentPanData:
                if currentFolioNum == casFolioNum:
                    if pan not in resolvedCasDataForMissingPans:
                        resolvedCasDataForMissingPans[pan] = {}
                    resolvedCasDataForMissingPans[pan][casFolioNum] = casMissingPanData[casFolioNum]
    for pan in resolvedCasDataForMissingPans:
        missingPanFolioData = resolvedCasDataForMissingPans[pan]
        for folioNum in missingPanFolioData:
            casDataByPan[pan][folioNum] = missingPanFolioData[folioNum]
            casDataByPan[''].pop(folioNum)
    if not bool(casDataByPan['']):
        casDataByPan.pop('')

def getAmountFromStringWithCurrency(stringWithCurrency):
    stringWithoutCurrency = stringWithCurrency.replace('₹', '').replace(',', '')
    amount = float(stringWithoutCurrency)
    return amount

def outputResultToFile(result, filename):
    df = pd.DataFrame(result)
    df.to_csv(filename, index=False)

# %% - Data preparation
casJsons = []
for casSource in CAS_SOURCES:
    print(f"Loading CAS for {casSource['email']}...")
    casJsons.append(json.loads(casparser.read_cas_pdf(casSource['path'], casSource['password'], output="json")))
print('Restructuring CAS JSON...')
casDataByPan = {}
for casJson in casJsons:
    restuctureCASJson(casDataByPan, casJson)
print('Parsing Portfolio spreadsheet...')
currentDataByPan = getCurrentDataByPanFromSpreadsheet(MF_PORTFOLIO_SHEET_PATH)
print('Adding Yahoo symbols to CAS data...')
addYahooSymbolToCasJsonByPan(casDataByPan, currentDataByPan)
fulFillCasDataForMissingPansFromCurrentData(casDataByPan, currentDataByPan)
print('Data preparation - Done')
# Validations
result = {}
resultWithOnlyErrors = {}
# %% PAN attendance
panInCas = casDataByPan.keys()
panInCurrentData = currentDataByPan.keys()
result[VALIDATION_KEY_PAN] = {}
resultPan = result[VALIDATION_KEY_PAN]
for pan in panInCas:
    if pan == '':
        continue
    resultPan[pan] = {}
    resultPan[pan][VALIDATION_KEY_PAN_ATTENDANCE] = {}
    resultPan[pan][VALIDATION_KEY_PAN_ATTENDANCE][VALIDATION_KEY_STATUS_CODE] = ""
    resultPan[pan][VALIDATION_KEY_PAN_ATTENDANCE][VALIDATION_KEY_COMMENTS] = ""
    if pan in panInCurrentData:
        resultPan[pan][VALIDATION_KEY_PAN_ATTENDANCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_SUCCESS
    else:
        resultPan[pan][VALIDATION_KEY_PAN_ATTENDANCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
        resultPan[pan][VALIDATION_KEY_PAN_ATTENDANCE][VALIDATION_KEY_COMMENTS] = "Absent from current data"
        if pan not in resultWithOnlyErrors:
            resultWithOnlyErrors[VALIDATION_KEY_PAN] = {}
        resultWithOnlyErrors[VALIDATION_KEY_PAN][pan] = resultPan[pan]

for pan in panInCurrentData:
    if pan not in resultPan:
        resultPan[pan] = {}
        resultPan[pan][VALIDATION_KEY_PAN_ATTENDANCE] = {}
        resultPan[pan][VALIDATION_KEY_PAN_ATTENDANCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
        resultPan[pan][VALIDATION_KEY_PAN_ATTENDANCE][VALIDATION_KEY_COMMENTS] = "Absent from CAS data"
        if VALIDATION_KEY_PAN not in resultWithOnlyErrors:
            resultWithOnlyErrors[VALIDATION_KEY_PAN] = {}
        resultWithOnlyErrors[VALIDATION_KEY_PAN][pan] = resultPan[pan]

# %% - Folio attendance
foliosInCas = []
foliosInCurrentPortfolio = []
result[VALIDATION_KEY_FOLIOS] = {}
resultFolios = result[VALIDATION_KEY_FOLIOS]
for pan in casDataByPan:
    casPanData = casDataByPan[pan]
    foliosInCas.extend(casPanData.keys())
for pan in currentDataByPan:
    currentPanData = currentDataByPan[pan]
    foliosInCurrentPortfolio.extend(currentPanData.keys())
for casFolio in foliosInCas:
    resultFolios[casFolio] = {}
    resultFolios[casFolio][VALIDATION_KEY_FOLIO_ATTENDANCE] = {}
    resultFolios[casFolio][VALIDATION_KEY_FOLIO_ATTENDANCE][VALIDATION_KEY_STATUS_CODE] = ""
    resultFolios[casFolio][VALIDATION_KEY_FOLIO_ATTENDANCE][VALIDATION_KEY_COMMENTS] = ""
    if casFolio in foliosInCurrentPortfolio:
        resultFolios[casFolio][VALIDATION_KEY_FOLIO_ATTENDANCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_SUCCESS
    else:
        resultFolios[casFolio][VALIDATION_KEY_FOLIO_ATTENDANCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
        resultFolios[casFolio][VALIDATION_KEY_FOLIO_ATTENDANCE][VALIDATION_KEY_COMMENTS] = "Absent from current data"
        if VALIDATION_KEY_FOLIOS not in resultWithOnlyErrors:
            resultWithOnlyErrors[VALIDATION_KEY_FOLIOS] = {}
        resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][casFolio] = resultFolios[casFolio]
for currentFolio in foliosInCurrentPortfolio:
    if currentFolio not in resultFolios:
        resultFolios[casFolio] = {}
        resultFolios[casFolio][VALIDATION_KEY_FOLIO_ATTENDANCE] = {}
        resultFolios[casFolio][VALIDATION_KEY_FOLIO_ATTENDANCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
        resultFolios[casFolio][VALIDATION_KEY_FOLIO_ATTENDANCE][VALIDATION_KEY_COMMENTS] = "Absent from CAS data"
        if VALIDATION_KEY_FOLIOS not in resultWithOnlyErrors:
            resultWithOnlyErrors[VALIDATION_KEY_FOLIOS] = {}
        resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][casFolio] = resultFolios[casFolio]
# %% Data Validations
totalValidatedCasValue = 0.0
totalValidatedCasCost = 0.0
for pan in casDataByPan:
    casPanData = casDataByPan[pan]
    if pan == '':
        for folioNum in casPanData:
            result[VALIDATION_KEY_FOLIOS][folioNum][VALIDATION_KEY_FOLIO_PAN_LINK] = {}
            result[VALIDATION_KEY_FOLIOS][folioNum][VALIDATION_KEY_FOLIO_PAN_LINK][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
            result[VALIDATION_KEY_FOLIOS][folioNum][VALIDATION_KEY_FOLIO_PAN_LINK][VALIDATION_KEY_COMMENTS] = "Folio has no PAN"
            if VALIDATION_KEY_FOLIOS not in resultWithOnlyErrors:
                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS] = {}
            resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum] = result[VALIDATION_KEY_FOLIOS][folioNum]
        continue
    currentPanData = currentDataByPan[pan]
    for folioNum in casPanData:
        if folioNum not in currentPanData:
            result[VALIDATION_KEY_FOLIOS][folioNum][VALIDATION_KEY_CAS_FOLIO_IN_CURRENT_DATA_PRESENCE] = {}
            result[VALIDATION_KEY_FOLIOS][folioNum][VALIDATION_KEY_CAS_FOLIO_IN_CURRENT_DATA_PRESENCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
            result[VALIDATION_KEY_FOLIOS][folioNum][VALIDATION_KEY_CAS_FOLIO_IN_CURRENT_DATA_PRESENCE][VALIDATION_KEY_COMMENTS] = " CAS folio is not in current data"
            if VALIDATION_KEY_FOLIOS not in resultWithOnlyErrors:
                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS] = {}
            resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum] = result[VALIDATION_KEY_FOLIOS][folioNum]
            continue
        casFolioData = casPanData[folioNum]
        currentFolioData = currentPanData[folioNum]
        result[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES] = []
        for casScheme in casFolioData[KEY_SCHEMES]:
            schemeValidationResult = deepcopy(casScheme)
            result[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES].append(schemeValidationResult)
            if casScheme[KEY_YAHOO_SYMBOL] == '':
                schemeValidationResult[VALIDATION_KEY_SCHEME_YAHOO_SYMBOL_PRESENCE] = {}
                schemeValidationResult[VALIDATION_KEY_SCHEME_YAHOO_SYMBOL_PRESENCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
                schemeValidationResult[VALIDATION_KEY_SCHEME_YAHOO_SYMBOL_PRESENCE][VALIDATION_KEY_COMMENTS] = "Yahoo symbol not present in CAS"
                if VALIDATION_KEY_FOLIOS not in resultWithOnlyErrors:
                    resultWithOnlyErrors[VALIDATION_KEY_FOLIOS] = {}
                if folioNum not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS]:
                    resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum] = {}
                if KEY_SCHEMES not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum]:
                    resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES] = []
                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES].append(schemeValidationResult)
                continue
            matchingSchemeFound = False
            for currentScheme in currentFolioData[KEY_SCHEMES]:
                if casScheme[KEY_YAHOO_SYMBOL] == currentScheme[KEY_YAHOO_SYMBOL]:
                    matchingSchemeFound = True
                    if casScheme[KEY_ISIN] == currentScheme[KEY_ISIN]:
                        casSchemeCost = casScheme[KEY_VALUATION][KEY_COST]
                        currentSchemeCost = currentScheme[KEY_VALUATION][KEY_COST]
                        casSchemeValue = casScheme[KEY_VALUATION][KEY_VALUE]
                        currentSchemeValue = currentScheme[KEY_VALUATION][KEY_VALUE]
                        schemeValidationResult[VALIDATION_KEY_SCHEME_COST] = {}
                        schemeValidationResult[VALIDATION_KEY_SCHEME_VALUE] = {}
                        if casSchemeCost - (casSchemeCost*VALIDATION_COST_DEFLECTION_ALLOWED_PRECENTAGE) <= currentSchemeCost <= casSchemeCost + (casSchemeCost*VALIDATION_COST_DEFLECTION_ALLOWED_PRECENTAGE):
                            schemeValidationResult[VALIDATION_KEY_SCHEME_COST][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_SUCCESS
                            schemeValidationResult[VALIDATION_KEY_SCHEME_COST][VALIDATION_KEY_COMMENTS] = f'Current data scheme cost: {currentSchemeCost}, CAS data scheme cost: {casSchemeCost}'
                        else:
                            schemeValidationResult[VALIDATION_KEY_SCHEME_COST][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
                            schemeValidationResult[VALIDATION_KEY_SCHEME_COST][VALIDATION_KEY_COMMENTS] = f'Current data scheme cost: {currentSchemeCost}, CAS data scheme cost: {casSchemeCost}, Deflection allowed: {VALIDATION_COST_DEFLECTION_ALLOWED_PRECENTAGE}'
                            if VALIDATION_KEY_FOLIOS not in resultWithOnlyErrors:
                                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS] = {}
                            if folioNum not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS]:
                                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum] = {}
                            if KEY_SCHEMES not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum]:
                                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES] = []
                            resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES].append(schemeValidationResult)
                        if casSchemeValue - (casSchemeValue*VALIDATION_VALUE_DEFLECTION_ALLOWED_PERCENTAGE) <= currentSchemeValue <= casSchemeValue + (casSchemeValue*VALIDATION_VALUE_DEFLECTION_ALLOWED_PERCENTAGE):
                            schemeValidationResult[VALIDATION_KEY_SCHEME_VALUE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_SUCCESS
                            schemeValidationResult[VALIDATION_KEY_SCHEME_VALUE][VALIDATION_KEY_COMMENTS] = f'Current data scheme value: {currentSchemeValue}, CAS data scheme value: {casSchemeValue}'
                            totalValidatedCasValue += casSchemeValue
                            totalValidatedCasCost += casSchemeCost
                        else:
                            schemeValidationResult[VALIDATION_KEY_SCHEME_VALUE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
                            schemeValidationResult[VALIDATION_KEY_SCHEME_VALUE][VALIDATION_KEY_COMMENTS] = f'Current data scheme value: {currentSchemeValue}, CAS data scheme cost: {casSchemeValue}, Deflection allowed: {VALIDATION_VALUE_DEFLECTION_ALLOWED_PERCENTAGE}'
                            if VALIDATION_KEY_FOLIOS not in resultWithOnlyErrors:
                                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS] = {}
                            if folioNum not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS]:
                                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum] = {}
                            if KEY_SCHEMES not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum]:
                                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES] = []
                            resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES].append(schemeValidationResult)
                    else:
                        schemeValidationResult[VALIDATION_KEY_MATCHING_ISIN_PRESENCE] = {}
                        schemeValidationResult[VALIDATION_KEY_MATCHING_ISIN_PRESENCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
                        schemeValidationResult[VALIDATION_KEY_MATCHING_ISIN_PRESENCE][VALIDATION_KEY_COMMENTS] = "Yahoo symbols don't match with ISIN in CAS and current data"
                        if VALIDATION_KEY_FOLIOS not in resultWithOnlyErrors:
                            resultWithOnlyErrors[VALIDATION_KEY_FOLIOS] = {}
                        if folioNum not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS]:
                            resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum] = {}
                        if KEY_SCHEMES not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum]:
                            resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES] = []
                        resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES].append(schemeValidationResult)
            if not matchingSchemeFound:
                schemeValidationResult[VALIDATION_KEY_MATCHING_YAHOO_SYMBOL_PRESENCE] = {}
                schemeValidationResult[VALIDATION_KEY_MATCHING_YAHOO_SYMBOL_PRESENCE][VALIDATION_KEY_STATUS_CODE] = VALIDATION_STATUS_CODE_FAILURE
                schemeValidationResult[VALIDATION_KEY_MATCHING_YAHOO_SYMBOL_PRESENCE][VALIDATION_KEY_COMMENTS] = "No matching Yahoo symbol found in current portfolio"
                if VALIDATION_KEY_FOLIOS not in resultWithOnlyErrors:
                    resultWithOnlyErrors[VALIDATION_KEY_FOLIOS] = {}
                if folioNum not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS]:
                    resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum] = {}
                if KEY_SCHEMES not in resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum]:
                    resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES] = []
                resultWithOnlyErrors[VALIDATION_KEY_FOLIOS][folioNum][KEY_SCHEMES].append(schemeValidationResult)
                continue
currentDataPendingValidation = {}
for pan in casDataByPan:
    casPanData = casDataByPan[pan]
    for folio in casPanData:
        casFolioData = casPanData[folio]
        currentDataSchemes = currentDataByPan[pan][folio][KEY_SCHEMES]
        for casScheme in casFolioData[KEY_SCHEMES]:
            matchFound = False
            for currentScheme in currentDataSchemes:
                if casScheme[KEY_YAHOO_SYMBOL] == currentScheme[KEY_YAHOO_SYMBOL]:
                    matchFound = True
                    break
            if not matchFound:
                if pan not in currentDataPendingValidation:
                    currentDataPendingValidation[pan] = {}
                if folio not in currentDataPendingValidation[pan]:
                    currentDataPendingValidation[pan][folio] = {KEY_SCHEMES: []}
                currentDataPendingValidation[pan][folio][KEY_SCHEMES].append(currentScheme)
result[VALIDATION_KEY_MISC] = {}
result[VALIDATION_KEY_MISC][VALIDATION_KEY_CURRENT_DATA_MISSING_VALIDATIONS] = currentDataPendingValidation
if bool(currentDataPendingValidation):
    resultWithOnlyErrors[VALIDATION_KEY_MISC][VALIDATION_KEY_CURRENT_DATA_MISSING_VALIDATIONS] = result[VALIDATION_KEY_MISC][VALIDATION_KEY_CURRENT_DATA_MISSING_VALIDATIONS]
# %%
print(f'Complete result: {json.dumps(result)}')
print(f'Result with only errors: {json.dumps(resultWithOnlyErrors)}')
# print(f'Writing to file({RESULT_OUTPUT_PATH})...')
# outputResultToFile(resultWithOnlyErrors, RESULT_OUTPUT_PATH)
print(f'Total CAS cost validated = {totalValidatedCasCost}, Total CAS value validated = {totalValidatedCasValue}')

