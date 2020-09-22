from scripts.ReportGenerator import ReportGenerator
from scripts.GroupDbLoader import GroupDbLoader
from scripts.S3fileloader import S3fileLoader


def main():
    gdl = GroupDbLoader("차세정_E")
    gdl.connect()
    print(gdl.tables.keys())
    print(gdl.uids)

    rg = ReportGenerator(gdl)
    rg.run()
    print(rg.tables.keys())

    gdl.connect(key=['batchFiles'])
    print(gdl.tables.keys())

    s3 = S3fileLoader('s3files')
    for uid in gdl.uids:
        fids = [r['fid'] for i, r in gdl.tables['batchFiles'].query(f"uid=={uid}").iterrows()]
        s3.download_user_batch_files(uid=uid, fids=fids)
        s3.get_data_from_files(uid=uid, fids=fids)
        for type in s3.db_df.keys():
            s3.db_df[type].to_csv(path_or_buf=f'/Users/kevin/PycharmProjects/beactive_datapipeline/s3files/csvs/{uid}_{type}.csv')


if __name__ == '__main__':
    main()
