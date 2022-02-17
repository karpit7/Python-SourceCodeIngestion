import uuid
import datetime
import pytz
import requests
import zipfile
import pyodbc

server = r'ARPITLTOP'  # Database server instance
database = r'eventmanagement'  # Database name
username = r'sa'  # Database user name
password = r'sa123@123'

processID = str(uuid.uuid4())
processStartDateTime = datetime.datetime.now(tz=pytz.timezone("Asia/Calcutta")).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

# *************** DATABASE CONNECTION *************** #
cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

print('Connection successful')


def downloadAndUnzip(urlAddress, downloadLocation):
    # Downloading the file by sending the request to the URL

    print('Downloading started')
    req = requests.get(urlAddress)

    # Split URL to get the file name
    filename = urlAddress.split('/')[-1]
    print(filename)
    newLocation = downloadLocation + "/" + filename
    print(newLocation)
    # Writing the file to the local file system
    with open(newLocation, 'wb') as output_file:
        output_file.write(req.content)
    print('Downloading Completed')

    # unzip file
    unzipLoc = "D:\penataho\input"  # unzip location
    with zipfile.ZipFile(newLocation, 'r') as zip_ref:
        zip_ref.extractall(unzipLoc + "/" + filename[:-4])
    print('file unzipped')


if __name__ == "__main__":

    urlAddress = ['https://golang.org/dl/go1.17.3.windows-amd64.zip', 'https://go.dev/dl/go1.17.6.windows-arm64.zip',
                  'https://www.youtube.com/']
    downloadLocation = "D:\penataho\input"  # File download location

    try:
        for url in urlAddress:
            downloadAndUnzip(url, downloadLocation)
            # *************** Audit *************** #
            cursor = cnxn.cursor()
            sql = " downloadCount ?,?,?,?"
            values = processID, processStartDateTime, datetime.datetime.now(tz=pytz.timezone("Asia/Calcutta")).strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3], "Execution Successful"
            cursor.execute(sql, values)
            cnxn.commit()

    except:
        # *************** Audit *************** #
        cursor = cnxn.cursor()
        sql = " downloadCount ?,?,?,?"
        values = processID, processStartDateTime, datetime.datetime.now(tz=pytz.timezone("Asia/Calcutta")).strftime(
            "%Y-%m-%d %H:%M:%S.%f")[:-3], "Execution Unsuccessful"
        cursor.execute(sql, values)
        cnxn.commit()
        print('Execution Unsuccessful')
