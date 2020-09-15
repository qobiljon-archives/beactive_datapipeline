from scripts.DatetimeTimestamp import get_date_str_from_timestamp
from scripts.DatetimeTimestamp import timestamp_to_datetime
from scripts.DatetimeTimestamp import date_str_to_timestamp
import pandas as pd


class ReportGenerator:
    # GroupDbLoader or groupName ???
    def __init__(self, loader):
        # get group information
        self.groupName = loader.groupName
        self.gid = loader.gid
        self.start = loader.start
        self.groupMembers = loader.group_members
        self.uids = loader.uids
        self.tables = loader.tables

        # 실험이 끝나지 않은 경우, status데이터로 부터 날짜 범위를 받아옴
        if loader.end == 1599115229 * 2:
            status_data = []
            for uid in self.uids:
                if not self.tables['statusActivity'].empty:
                    status_data.append(self.tables['statusActivity']['UNIX_TIMESTAMP(`timestamp`)'])
            self.end = max(pd.concat(status_data).tolist())
        else:
            self.end = loader.end
        self.days = self.get_date_range()

    # Utils
    def filter_table(self, df: pd.DataFrame, column, value):
        filtered_df = df[df[column].values == value]
        return filtered_df

    def get_date_range(self):
        start_day = timestamp_to_datetime(self.start)
        end_day = timestamp_to_datetime(self.end)
        days = pd.date_range(start=start_day, end=end_day)
        return [str(i.strftime("%Y%m%d")) for i in days]

    def agg_by_date_user(self, df: pd.DataFrame, date_col, val_col, task: str):
        if df.empty:
            return df

        if task == 'sum':
            df[val_col] = df[val_col].apply(int)

        dic = {}
        for uid in self.uids:
            userData = {}
            if task == 'count':
                data = df[df['uid'] == uid].groupby(date_col).count()
            elif task == 'sum':
                data = df[df['uid'] == uid].groupby(date_col).sum()
            else:
                data = None
                print("check task!")

            if data.empty:
                dic[uid] = {}
                continue

            keys = data[val_col].keys().tolist()
            values = data[val_col].values.tolist()

            for k, v in zip(keys, values):
                userData[k] = v

            dic[uid] = userData

        # put data into array
        dfcand = []
        for uid in self.uids:
            userDataList = []
            userData = dic[uid]
            for day in self.days:
                if day in userData:
                    userDataList.append(userData[day])
                else:
                    userDataList.append(0)
            dfcand.append(userDataList)
        standup_alarm = pd.DataFrame(dfcand, columns=self.days, index=self.uids)
        return standup_alarm

    def get_ratio(self, result: pd.DataFrame, total: pd.DataFrame, filler):
        return (result / total * 100).round(2).fillna(value=filler)

    def get_daily_active_time(self, df, time_col):

        if df.empty:
            return df

        # add date col and sort
        df['date'] = df[time_col].apply(get_date_str_from_timestamp)
        df = df.sort_values(by=time_col).reset_index()

        dateTime = {}
        flag = False
        date = self.days[0]
        d = date
        for i in df.index:
            now, state, d = df[time_col].iloc[i], df['status'].iloc[i], df['date'].iloc[i]

            # start active
            if not flag and state == 1:
                flag = True
                start = now
                date = d

            # end active
            if d == date and flag and state == 0:
                time = (now - start)

                if date in dateTime:
                    dateTime[date] += round(time / 60, 2)
                else:
                    dateTime[date] = round(time / 60, 2)
                flag = False

            if d != date:
                # if date change -> save active time and start again
                if flag:
                    ydayTs = date_str_to_timestamp(d, -1)
                    tdayTs = date_str_to_timestamp(d, 0)
                    time = (ydayTs - start)
                    if date in dateTime:
                        dateTime[date] += round(time / 60, 2)
                    else:
                        dateTime[date] = round(time / 60, 2)
                    start = tdayTs
                date = d
        return dateTime

    # Transform data
    # save standup alarm count df
    def get_standup_alarm(self):
        alarmDf = self.tables['missionsViewAlarms']
        alarmDf_failed = self.filter_table(alarmDf, 'missionResult', 'Failed')
        alarmDf_success = self.filter_table(alarmDf, 'missionResult', 'Success')
        alarmDf = pd.concat([alarmDf_failed, alarmDf_success])
        self.tables['StandupAlarm'] = self.agg_by_date_user(alarmDf, 'missionDate', 'missionResult', task="count")
        return

    def get_standup_data(self):
        alarmDf = self.tables['missionsViewAlarms']
        alarmDf = self.filter_table(alarmDf, 'missionResult', 'Success')
        self.tables['StandupData'] = self.agg_by_date_user(alarmDf, 'missionDate', 'missionResult', task="count")
        return

    def get_standup_goal_success(self):
        self.tables['StandupGoalSuccess'] = self.get_ratio(self.tables['StandupData'], self.tables['StandupAlarm'], filler='-')
        return

    # only Success Case
    def get_add_step_data(self):
        addStepDf = self.tables['missionsView']
        addStepDf = self.filter_table(addStepDf, 'missionType', 'SUB1')
        addStepDf = self.filter_table(addStepDf, 'missionResult', 'Success')
        self.tables['AddStepData'] = self.agg_by_date_user(addStepDf, 'missionDate', 'missionResult', 'count')
        return

    # All Additional Case
    def get_add_step_data_total(self):
        addStepDf = self.tables['missionsView']
        addStepDf = self.filter_table(addStepDf, 'missionType', 'SUB1')
        self.tables['AddStepDataTotal'] = self.agg_by_date_user(addStepDf, 'missionDate', 'missionResult', 'count')
        return

    def get_add_step_goal_success(self):
        self.tables['AddStepGaolSuccess'] = self.get_ratio(self.tables['AddStepData'], self.tables['AddStepDataTotal'], filler='-')
        return

    def get_activity_data(self):
        userActivityTimes = {}
        statusDf = self.tables['statusActivity']
        for uid in self.uids:
            userData = statusDf.query("uid==" + str(uid))
            userActivityTimes[uid] = self.get_daily_active_time(userData, 'UNIX_TIMESTAMP(`timestamp`)')

        # put data into array
        dfcand = []
        for uid in self.uids:
            userDataList = []
            userData = userActivityTimes[uid]
            for day in self.days:
                if day in userData:
                    userDataList.append(userData[day])
                else:
                    userDataList.append(0)
            dfcand.append(userDataList)
        self.tables['activityTimeData'] = pd.DataFrame(dfcand, columns=self.days, index=self.uids)
        return

    def get_standup_point(self):
        missionDf = self.tables['missions']
        missionDf = self.filter_table(missionDf, 'missionType', 'MAIN')
        missionDf = self.filter_table(missionDf, 'missionResult', 'Success')
        self.tables['standupPoint'] = self.agg_by_date_user(missionDf, 'missionDate', 'missionPoint', 'sum')

    def get_add_step_point(self):
        addStepDf = self.tables['missionsView']
        addStepDf = self.filter_table(addStepDf, 'missionType', 'SUB1')
        addStepDf = self.filter_table(addStepDf, 'missionResult', 'Success')
        self.tables['AddStepPoint'] = self.agg_by_date_user(addStepDf, 'missionDate', 'missionPoint', 'sum')

    def get_standup_count(self):
        statusDf = self.tables['statusActivity']
        statusDf['date'] = statusDf['UNIX_TIMESTAMP(`timestamp`)'].apply(get_date_str_from_timestamp)
        self.tables['standupCount'] = self.agg_by_date_user(statusDf, 'date', 'status', 'sum')
        return

    def get_standup_continue(self):
        # mission 기준으로 연속 성공  (uid, date, missiontype, result)
        # missiontype == main
        # SFSSFSSS --> 연속성공 2+3 (최초 포함) / 연속성공 1+2 최초 미포함 / 연속성공
        missionDf = self.tables['missions']
        missionDf = self.filter_table(missionDf, 'missionType', 'MAIN')
        # groupby user/day --> build list for each day --> [0,1,1,0,0,1] from 'missionResult' col

    # def getSittingData(self) --> 활동시간으로 대체
    def run(self):
        self.get_standup_alarm()
        self.get_standup_data()
        self.get_standup_goal_success()
        self.get_add_step_data()
        self.get_add_step_data_total()
        self.get_add_step_goal_success()
        self.get_activity_data()
        self.get_standup_continue()  # 연속성공  SFSSFSSS ...
        self.get_standup_point()  # standup Point Each
        self.get_add_step_point()
        self.get_standup_count()  # standup Secondary : 일일 기립 횟수 total

        # return self.tables
