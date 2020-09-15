from scripts.DbConnector import DbConnector
from scripts.DbConnector import ConnectionCfg
import pandas as pd

'''
Group DB Loader 
- Load data in RDB 
- Save data as pd.DataFrame
- convert datetime, timestamp to unixtimestamp
- Get group data
'''


class GroupDbLoader(DbConnector):
    def __init__(self, group_name):
        self.groupName = group_name
        self.tables = {}
        self.cfg = ConnectionCfg()
        self.gid = None
        self.start = None
        self.end = None
        self.group_members = None
        self.uids = None

    def get_table(self, db, table_name: str, unix_time=True, where=None):
        # use BeActive db
        cur = db.cursor()
        cur.execute('USE ' + self.cfg.DB)

        # get Table Scheme
        scheme = []
        scheme_sql = 'desc ' + table_name
        cur.execute(scheme_sql)
        for r in cur:
            scheme.append(r)

        # col name "condition" make problem while query
        cols = []
        sql_cols = []
        for row in scheme:
            col_name, col_type = row[0], row[1]
            if unix_time:
                if col_type.startswith('timestamp') or col_type.startswith('datetime'):
                    col_name = "UNIX_TIMESTAMP(`" + col_name + "`)"
                    col_name_ = col_name
                else:
                    col_name_ = table_name + '.' + col_name
            else:
                col_name_ = table_name + '.' + col_name
            sql_cols.append(col_name_)
            cols.append(col_name)

        table_sql = 'select ' + ', '.join(sql_cols) + ' from ' + table_name

        if where:
            table_sql = table_sql + ' where ' + where
        # print(tableSql)

        print(table_sql)

        table = []
        cur.execute(table_sql)
        for r in cur:
            table.append(r)

        df = pd.DataFrame(table)
        if table:
            df.columns = cols
        return df

    # get by groupName
    def get_group_info(self, db, group_name: str):
        group_where = ('groupName="' + group_name + '"')
        df = self.get_table(db, 'groupInfo', where=group_where)
        gid, oid, _, setting, start, end, _ = df.iloc[0].values.tolist()

        # 실험이 끝나지 않은 경우
        if end is None:
            end = 1599115229 * 2
        return gid, oid, setting, start, end

    # get by gid
    def get_group_members(self, db):
        gid_where = ('gid="' + str(self.gid) + '"')
        return self.get_table(db, 'groupMembers', where=gid_where)

    def get_missions_view_alarms(self, db):
        gid_where = ('gid="' + str(self.gid) + '"')
        return self.get_table(db, 'missionsViewAlarms', where=gid_where)

    def get_missions_view(self, db):
        gid_where = ('gid="' + str(self.gid) + '"')
        return self.get_table(db, 'missionsView', where=gid_where)

    def get_reward_view(self, db):
        gid_where = ('gid="' + str(self.gid) + '"')
        return self.get_table(db, 'rewardView', where=gid_where)

    def get_missions(self, db):
        gid_where = ('gid="' + str(self.gid) + '"')
        return self.get_table(db, 'missions', where=gid_where)

    def get_step_count(self, db):
        uids_where = ('start_time between ' + str(self.start * 1000) + ' and ' + str(self.end * 1000) + ' and uid in (' + str(self.uids)[1:-1] + ')')
        return self.get_table(db, 'ActivityLogStepCount', where=uids_where)

    # get by uids
    def get_status_activity(self, db):
        uid_where = ('uid in (' + str(self.uids)[1:-1] + ')' + ' and UNIX_TIMESTAMP(`timestamp`) between ' + str(self.start) + ' and ' + str(self.end))
        return self.get_table(db, 'StatusActivity', where=uid_where)

    def get_batch_file_list(self, db):
        try:
            gid_where = ('uid in (' + str(self.uids)[1:-1] + ') and rangeBegin between ' + str((self.start - 60 * 60 * 24) * 1000) + ' and ' + str(self.end * 1000))
            return self.get_table(db, 'batchFiles', where=gid_where).sort_values(by='uploaded')
        except Exception as e:
            print("Fail to get BatchFileList!", e)

    # override from DbConnector
    # use key to select tables to query (list or dict)
    def run(self, db, key=None):
        gid, oid, setting, start, end = self.get_group_info(db, self.groupName)
        print('groupInfo-----------------------')
        self.gid = gid
        self.start = start
        self.end = end

        self.group_members = self.get_group_members(db)
        print('groupMembers-----------------------')
        self.uids = self.group_members['uid'].values.tolist()

        if not key:
            self.tables['statusActivity'] = self.get_status_activity(db)
            print('StatusActivity-----------------------')
            self.tables['missionsViewAlarms'] = self.get_missions_view_alarms(db)
            print('missionsViewAlarms-----------------------')
            self.tables['missionsView'] = self.get_missions_view(db)
            print('missionsView-----------------------')
            self.tables['rewardView'] = self.get_reward_view(db)
            print('rewardView-----------------------')
            self.tables['missions'] = self.get_missions(db)
            print('missions-----------------------')
            self.tables['stepCount'] = self.get_step_count(db)
        else:
            if 'statusActivity' in key:
                self.tables['statusActivity'] = self.get_status_activity(db)
                print('StatusActivity-----------------------')
            if 'statusActivity' in key:
                self.tables['missionsViewAlarms'] = self.get_missions_view_alarms(db)
                print('missionsViewAlarms-----------------------')
            if 'missionsView' in key:
                self.tables['missionsView'] = self.get_missions_view(db)
                print('missionsView-----------------------')
            if 'rewardView' in key:
                self.tables['rewardView'] = self.get_reward_view(db)
                print('rewardView-----------------------')
            if 'missions' in key:
                self.tables['missions'] = self.get_missions(db)
                print('missions-----------------------')
            if 'stepCount' in key:
                self.tables['stepCount'] = self.get_step_count(db)
                print('stepCount-----------------------')
            if 'batchFiles' in key:
                self.tables['batchFiles'] = self.get_batch_file_list(db)
                print('batchFiles-----------------------')
            # if 'userBatch' in key:
            #     uids = key['userBatch']
