import datetime
import os
import shutil
import sys
import uuid
import zipfile
import pytz
import requests
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable  # Environment variable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName("Download and Unzip file").getOrCreate()  # Creating Spark Session


def download(urlAddress, downloadLocation):
    req = requests.get(urlAddress)                  # create HTTP response object
    filename = urlAddress.split('/')[-1]            # Split URL to get the file name
    newLocation = downloadLocation + "/" + filename
    with open(newLocation, 'wb') as output_file:    # Writing the file to the local file system
        output_file.write(req.content)              # write the contents of the respons
    print('Downloading Completed')


def unZipFiles(downloadLocation, unzipLocation):    # unzip the files
    files = os.listdir(downloadLocation)            # returns a list containing the names of the entries in the directory
    for file in files:
        if file.endswith('.zip'):
            filePath = downloadLocation + '/' + file
            zip_file = zipfile.ZipFile(filePath)
            for names in zip_file.namelist():
                zip_file.extract(names, unzipLocation + '/' + file[:-4])
            zip_file.close()
            print('Unzip Completed')


if __name__ == "__main__":
    urlAddress = ['https://golang.org/dl/go1.17.3.windows-amd64.zip', 'https://go.dev/dl/go1.17.6.windows-arm64.zip']
    downloadLocation = r"D:\ELT\download"  # File download location
    unzipLocation = r'D:\ELT\unzip'

    for url in urlAddress:
        try:
            processID = str(uuid.uuid4())
            processStartDateTime = datetime.datetime.now(tz=pytz.timezone("Asia/Calcutta")).strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3]
            download(url, downloadLocation)           # function call
            processEndDateTime = datetime.datetime.now(tz=pytz.timezone("Asia/Calcutta")).strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3]
            # *************** Audit *************** #
            try:
                newRow = spark.createDataFrame([(processID, processStartDateTime, processEndDateTime, url)])
                df_pandas = newRow.toPandas()
                df_pandas.to_csv(r"D:\ELT\Files\audit_success.csv", index=False, mode='a', header=False)
            except Exception as e:
                print("Exception in Logging")
        except:
            processEndDateTime = datetime.datetime.now(tz=pytz.timezone("Asia/Calcutta")).strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3]
            # ****** Rollback ********#
            for files in os.listdir(downloadLocation):
                path = os.path.join(downloadLocation, files)
                try:
                    shutil.rmtree(path)
                except OSError:
                    os.remove(path)
            print('Rollback successful')
            # *************** Audit *************** #
            try:
                newRow = spark.createDataFrame([(processID, processStartDateTime, processEndDateTime, url)])
                df_pandas = newRow.toPandas()
                df_pandas.to_csv(r"D:\ELT\Files\audit_failure.csv", index=False, mode='a', header=False)
            except Exception as e:
                print("Exception in Logging")
    unZipFiles(downloadLocation, unzipLocation)         # function call
