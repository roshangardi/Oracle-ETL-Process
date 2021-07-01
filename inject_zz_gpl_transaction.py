import pandas as pd
from sqlalchemy import create_engine

'''
Python code to insert data from file to schema ZZ_GPL_TRANSACTION.
Reads file as dataframe & converts data to required format as per schema & injects to table.
'''

#Connection Details
filename = ""
username = ""
password = ""
host = ""
service = ""
schema = ""

def injectData(filename, username, password, host, service, schema):

    dataset = pd.read_csv(filename, converters={'PRIMARY_ID_NUMBER': '{:0>9}'.format, 'JOINT_ID_NUMBER': '{:0>9}'.format})
    # df = pd.read_excel('API_Account_Creation.xlsx', converters={'HR_EMPLID': '{:0>9}'.format})
    date_headers = ["PLEDGE_INITIAL_PAY_DATE", "TRANSACTION_DATE", "IMPORT_DATE", "AUTH_DATE_TIME", "DEPOSIT_DATE",
                    "MISC_DATE1", "DATE_ADDED", "DATE_MODIFIED"]
    dataset[date_headers] = dataset[date_headers].apply(pd.to_datetime)

    conn_link = "oracle+cx_oracle://" + username + ":" + password + "@" + host + "/?service_name=" + service

    try:
        conn = create_engine(conn_link)
    except Exception as ex:
        print("Connection cannot be established - ", ex)
        exit(1)

    try:
        #NOTE: if_exists='append' is compulsary
        #If value of if_exists is 'replace', table will be removed before inserting!
        dataset.to_sql(name=schema.lower(), con=conn, if_exists='append', index=False)
    except Exception as ex:
        print("Data cannot be processed - ", ex)
        exit(1)

    print("Data processed!")
    exit(0)


#Main
injectData(filename, username, password, host, service, schema)