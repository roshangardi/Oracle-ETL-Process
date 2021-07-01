import datetime
import re

import pandas as pd
from pandas_schema import Column, Schema
from pandas_schema.validation import CustomElementValidation

'''
Python script to validate csv file data against schema type
Valid data will be written to original_file_name_valid.csv
Invalid data will be written to original_file_name_invalid.csv

@Author - nivashegde
'''

#Load schema headers into this method along with required dtype headers
def get_headers():

    file_headers = ["LOAD_ID", "GPL_TRANSACTION_ID", "GPL_TRANSACTION_TYPE", "GPL_SOURCE_CODE", "GPL_TRANSACTION_GROUP",
                    "TRANSACTION_TYPE", "PAYMENT_TYPE", "PLEDGE_PAYMENT_IND", "PLEDGE_PAYMENT_PLEDGE_NUMBER",
                    "CC_CARD_TYPE", "CHECK_NUMBER", "CHECK_AMT", "PLEDGE_TYPE", "PLEDGE_STATUS",
                    "PLEDGE_INITIAL_PAY_DATE", "PLEDGE_PAYMENT_FREQ", "PLEDGE_PAYMENT_AMOUNT", "TRANSACTION_DATE",
                    "TRANSACTION_AMT", "SPECIAL_HANDLING", "SOURCE_OF_INFORMATION", "ANONYMOUS", "APPEAL_CODE",
                    "ACK_CODE", "XCOMMENT", "TRANSMIT_TO_ACCTG_CODE", "REPORTING_CODE", "LEDGER_MONTH_CODE",
                    "PRIMARY_ID_NUMBER", "PRIMARY_ALLOCATION", "PRIMARY_CAMPAIGN", "PRIMARY_CLASS_CREDIT",
                    "PRIMARY_SCHOOL_CREDIT", "PRIMARY_REUNION_CAMPAIGN", "JOINT_ID_NUMBER", "JOINT_ALLOCATION",
                    "JOINT_CAMPAIGN", "JOINT_CLASS_CREDIT", "JOINT_SCHOOL_CREDIT", "JOINT_REUNION_CAMPAIGN",
                    "OTHER_ASSOC_CODE", "OTHER_ID_NUMBER", "OTHER_ALLOCATION", "OTHER_CAMPAIGN", "OTHER_CLASS_CREDIT",
                    "OTHER_SCHOOL_CREDIT", "OTHER_REUNION_CAMPAIGN", "BATCH_NUMBER", "RECEIPT_NUMBER", "PLEDGE_NUMBER",
                    "RUN_ID", "IMPORT_DATE", "IMPORT_MSG", "AUTH_TRANS_NUMB", "CARD_AUTH_CODE", "AUTH_DATE_TIME",
                    "CARD_AUTH_SOURCE", "DEPOSIT_DATE", "CREATE_TAX_RECEIPT_IND", "YR_END_RCPT_REQUESTED_IND",
                    "CHARITY_CODE", "SPONSORSHIP_AMT", "PREMIUM_DECLINED_IND", "PREMIUM_CODE", "PREMIUM_AMT",
                    "PREMIUM_COMMENT", "MISC_DETAIL1", "MISC_DETAIL2", "MISC_DATE1", "MISC_FLOAT1", "DATE_ADDED",
                    "DATE_MODIFIED", "OPERATOR_NAME", "USER_GROUP", "LOCATION_ID"]

    int_headers = ["LOAD_ID", "CHECK_AMT", "PLEDGE_PAYMENT_AMOUNT", "TRANSACTION_AMT", "RUN_ID", "SPONSORSHIP_AMT",
                   "PREMIUM_AMT", "MISC_FLOAT1", "LOCATION_ID"]

    date_headers = ["PLEDGE_INITIAL_PAY_DATE", "TRANSACTION_DATE", "IMPORT_DATE", "AUTH_DATE_TIME", "DEPOSIT_DATE",
                    "MISC_DATE1", "DATE_ADDED", "DATE_MODIFIED"]

    not_null_headers = ["LOAD_ID", "GPL_TRANSACTION_ID", "GPL_TRANSACTION_TYPE", "GPL_SOURCE_CODE",
                        "OPERATOR_NAME", "USER_GROUP"]

    char_headers = ["GPL_TRANSACTION_TYPE", "PAYMENT_TYPE", "PLEDGE_PAYMENT_IND", "PLEDGE_STATUS",
                    "PLEDGE_PAYMENT_FREQ", "ANONYMOUS", "OTHER_ASSOC_CODE", "CREATE_TAX_RECEIPT_IND",
                    "YR_END_RCPT_REQUESTED_IND", "PREMIUM_DECLINED_IND"]

    return file_headers, int_headers, date_headers, not_null_headers, char_headers


#Method to check if valud is integer or decimal or empty
def check_int(value):
    num_regex = "^[0-9]+([,.][0-9]+)?$"
    if re.match(num_regex, str(value)) or str(value) == "nan":
        return True
    else:
        return False


#Method to check of value is 1 BYTE CHAR or empty
def check_char(value):
    char_regex = "^[\D]{1}$"
    if re.match(char_regex, str(value)) or str(value) == "nan":
        return True
    else:
        return False


#Method to check if value is in date format mm/dd/yyyy
def check_date(value):

    if str(value) == "nan":
        return True

    try:
        datetime.datetime.strptime(str(value), '%m/%d/%Y')
        return True
    except ValueError:
        return False


#Method to chcek for null
def check_null(value):
    if str(value) == "nan":
        return False
    else:
        return True


#Method to generate CustomElementValidation functions for each condition
def return_validation_functions():

    int_validation = [CustomElementValidation(lambda x: check_int(x), 'is not number')]
    char_validation = [CustomElementValidation(lambda x: check_char(x), 'is not a character')]
    date_validation = [CustomElementValidation(lambda x: check_date(x), 'is not in date format mm/dd/yyyy')]
    null_validation = [CustomElementValidation(lambda x: check_null(x), 'is null')]

    return int_validation, char_validation, date_validation, null_validation


#Validating schema
def validate_schema(file_name):

    #Fetch headers
    file_headers, int_headers, date_headers, not_null_headers, char_headers = get_headers()

    #Read data from file
    dataset = pd.read_csv(file_name, names=file_headers, index_col=False)

    #Fetch validation functions
    int_validation, char_validation, date_validation, null_validation = return_validation_functions()

    #Define schema. Each column indicates a header along with validation function
    #Multiple validation functions can be merged, Eg - Column(int_headers[0], int_validation + null_validation)
    #TODO - iterate headers and depending on number of headers & type add the 'Column' dynamically
    schema = Schema([
            Column(int_headers[0], int_validation),
            Column(int_headers[1], int_validation),
            Column(int_headers[2], int_validation),
            Column(int_headers[3], int_validation),
            Column(int_headers[4], int_validation),
            Column(int_headers[5], int_validation),
            Column(int_headers[6], int_validation),
            Column(int_headers[7], int_validation),
            Column(int_headers[8], int_validation),
            Column(char_headers[0], char_validation),
            Column(char_headers[1], char_validation),
            Column(char_headers[2], char_validation),
            Column(char_headers[3], char_validation),
            Column(char_headers[4], char_validation),
            Column(char_headers[5], char_validation),
            Column(char_headers[6], char_validation),
            Column(char_headers[7], char_validation),
            Column(char_headers[8], char_validation),
            Column(char_headers[9], char_validation),
            Column(date_headers[0], date_validation),
            Column(date_headers[1], date_validation),
            Column(date_headers[2], date_validation),
            Column(date_headers[3], date_validation),
            Column(date_headers[4], date_validation),
            Column(date_headers[5], date_validation),
            Column(date_headers[6], date_validation),
            Column(date_headers[7], date_validation),
            Column(not_null_headers[0], null_validation),
            Column(not_null_headers[1], null_validation),
            Column(not_null_headers[2], null_validation),
            Column(not_null_headers[3], null_validation),
            Column(not_null_headers[4], null_validation),
            Column(not_null_headers[5], null_validation)
        ])

    #Combine all the headers checked into a list without duplicates
    all_check_headers = list(dict.fromkeys(int_headers + char_headers + date_headers + not_null_headers))

    #Validate against dataset
    errors = schema.validate(dataset, all_check_headers)

    #Print errors
    #TODO - Print error messages in invalid file
    for error in errors:
        print(error)

    #Fetch row numbers in which validation has failed
    errors_index_rows = [e.row for e in errors]

    #Fetch rows in which validation has failed and put it in a separate dataframe
    invalid_data = dataset.take(errors_index_rows)

    #Drop rows in which validation has failed
    valid_data = dataset.drop(index=errors_index_rows)

    return valid_data, invalid_data

#Main Driver function
def main():
    #Main

    file_name = "csulbfy21_2021-01-20_custom_daily_pledge.csv"
    file_name_prefix = file_name.split(".")[0]
    valid_data, invalid_data = validate_schema(file_name)

    #Split valid data to original_file_name_valid.csv and invalid data to original_file_name_invalid.csv
    valid_data.to_csv(path_or_buf=file_name_prefix + "_valid.csv", index=False)
    invalid_data.to_csv(path_or_buf=file_name_prefix + "_invalid.csv", index=False)


if __name__ == "__main__":
    main()