from scripts.DatetimeTimestamp import *
import pandas as pd
import zipfile
import sqlite3
import boto3
import json
import os


def only_step(x):
    return json.loads(x)["step"]


def to_datetime(ts):
    dt = datetime.fromtimestamp(ts / 1000)
    return dt


# download s3files and get Dataframe from it
class S3fileLoader:
    # init with group informations + directory for files

    def __init__(self, fileDirPath):
        self.fileDirPath = fileDirPath
        if not os.path.isdir(fileDirPath):
            os.makedirs(fileDirPath)

        # read access key
        # define s3 client
        with open('../configs/S3.json', 'r') as f:
            config = json.load(f)
        self.s3 = boto3.client('s3',
                               aws_access_key_id=config['accessKey'],
                               aws_secret_access_key=config['secretKey']
                               )
        self.uidFid = {}
        self.uidDf = {}
        self.logTypes = ['accelerometer', 'location', 'log', 'status', 'step']
        self.db_df = {}
        # self.logTypes = ['log', 'status', 'step']

    # download & Extract zipfile to db file
    def download_extract_file(self, fid, file_path: str):
        try:
            self.s3.download_file("file.beactive.kr", fid, file_path + fid + '.zip')
            with zipfile.ZipFile(file_path + fid + '.zip', 'r') as zip_ref:
                zip_ref.extractall(file_path)
                os.rename(file_path + 'send_data.db', file_path + fid + '.db')
                os.remove(file_path + fid + '.zip')
        except:
            pass

    # download users batch files
    def download_user_batch_files(self, uid, fids):
        uid = str(uid)
        if not os.path.isdir(self.fileDirPath + '/' + uid):
            os.makedirs(self.fileDirPath + '/' + uid)
        for fid in fids:
            self.download_extract_file(fid, self.fileDirPath + '/' + uid + '/')

    # read db files and build one pd df
    def get_data_from_files(self, uid, fids):
        uid = str(uid)
        path = self.fileDirPath + '/' + uid
        db_data = {}
        for fid in fids:
            con = sqlite3.connect(path + '/' + fid + '.db')
            cur = con.cursor()
            try:
                cur.execute('select * from Activity_info')
                for r in cur:
                    logType = r[1]
                    if logType in self.logTypes:
                        if logType in db_data:
                            db_data[logType].append(r)
                        else:
                            db_data[logType] = [r]
            except:
                pass

        print(db_data)

        db_df = {}
        for logType in self.logTypes:
            if logType in db_data:
                df = db_data[logType]
                db_df[logType] = pd.DataFrame(df)
        self.db_df[int(uid)] = db_df
        self.refine_step_data(uid)

    def refine_step_data(self, uid):
        uid = int(uid)
        df = self.db_df[uid]
        if 'step' in df:
            step_df = df['step']
            step_df['step'] = step_df[3].apply(only_step)
            step_df['timeStamps'] = step_df[2].apply(timestamp_to_datetime)
            self.db_df[uid]['step'] = step_df
