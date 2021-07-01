import re
from datetime import date
from datetime import timedelta

import pysftp

'''
Python script to fetch file from remote server to localhost using SFTP
'''

#Server details
host = ""
username = ""
password = ""

#Remote file details
remote_file_path = '/Custom Daily Files'

#Local file details
local_file_path = ''

# #regex for pledge & refusal file
# regex_refusals = "csulbfy21_[\d]{4}-[\d]{2}-[\d]{2}_custom_daily_refusals.csv$"
# regex_pledges = "csulbfy21_[\d]{4}-[\d]{2}-[\d]{2}_custom_daily_pledge.csv$"


today = date.today()
yesterday = today - timedelta(days=1)

#regex for pledge & refusal file
regex_refusals = "csulbfy21_" + str(yesterday) + "_custom_daily_refusals.csv$"
regex_pledges = "csulbfy21_" + str(yesterday) + "_custom_daily_pledge.csv$"
print(regex_pledges)

#SFTP Connection
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
processed_files = []

try:
    with pysftp.Connection(host=host, username=username, password=password, cnopts=cnopts) as sftp:
        file_flag = False

        file_list = sftp.listdir(remote_file_path)
        for filename in file_list:
            if re.match(regex_pledges, filename):
                remoteFilePath = remote_file_path + '/' + filename
                localFilePath = local_file_path + filename
                sftp.get(remoteFilePath, localFilePath)
                file_flag = True
                processed_files.append(filename)


except pysftp.paramiko.ssh_exception.BadAuthenticationType as ex:
    # Mostly locked account. Contact RNL to unlock account.
    print("Bad Authentication :", ex)
    exit(1)

except Exception as ex:
    print("Exception occurred - ", ex)
    exit(1)

if not file_flag:
    print("No file processed")
    exit(0)
else:
    print("Files processed -", processed_files)
    exit(0)

