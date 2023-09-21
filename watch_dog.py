import pymysql
import numpy as np
from datetime import datetime, timedelta

# Watch_Dog class
class Watch_Dog():
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = pymysql.connect(**db_config)
        self.sql = ''

    def check_mission(self, mission_category):
        if mission_category == 'prime':
            self.sql = "SELECT * FROM modulemte.db_watchdog_mission_list WHERE category = '0';"
        else:
            self.sql = "SELECT * FROM modulemte.db_watchdog_mission_list WHERE category <> '0';"
        # 执行查询并获取结果
        with self.connection.cursor() as cursor:
            cursor.execute(self.sql)
            results = cursor.fetchall()
        # 返回任务列表结果
        return results

    def mission(self, para):
        if para['status'] == '1':
            if para['spec_desc'] == 'Max':
                self.sql = f"SELECT Max({para['indicator']}) FROM {para['db_name']};"
                with self.connection.cursor() as cursor:
                    cursor.execute(self.sql)
                    results = cursor.fetchone()
                    result = results[0]
                    if para['spec'] == 'yestoday':
                        # 获取系统时间相对昨天的日期
                        today = datetime.now()
                        yesterday = today - timedelta(days=1)
                        new_date_string = yesterday.strftime("%Y-%m-%d")
                        # 比较result代表的日期是否小于new_date_string
                        if result < new_date_string:
                            return (result, False)
                        else:
                            return (result, True)

    def record(self, para, res):
        # 获取当前系统时间
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if res[1]:
            self.sql = (f"INSERT INTO modulemte.db_watchdog_record "
                        f"(id, watch_id, spec, observe, judgement, analysis_run, charger) VALUES"
                        f"('{now}', '{para['id']}', '{para['spec']}','{res[0]}','0','-','auto')")
            print(self.sql)
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                self.connection.commit()

    def close_connection(self):
        # 关闭数据库连接
        if self.connection:
            self.connection.close()

class P_Chart():
    def __init__(self, observe, sample_size):
        self.observe = observe
        self.sample_size = sample_size

    def p_chart_judge(self):
        p_values = [obs / sample_size for obs, sample_size in zip(self.observe, self.sample_size)]
        # 计算总体P值均值和标准差
        mean_p = np.mean(p_values)
        std_p = np.std(p_values)
        # 设置控制限
        n = len(observations)
        UCL = mean_p + 3 * (std_p / np.sqrt(n))
        LCL = mean_p - 3 * (std_p / np.sqrt(n))
        # 判异结果
        out_of_control = any(p > UCL or p < LCL for p in p_values)
        return out_of_control

def main():
    db_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'remoteuser',
        'password': 'password'}
    watch_dog = Watch_Dog(db_config)
    missionList = watch_dog.check_mission('prime')
    for mission in missionList:
        missionPara = {'id': mission[0],
                    'db_name': mission[4],
                    'indicator': mission[3],
                    'spec': mission[6],
                    'spec_desc': mission[5],
                    'status': mission[7],
                    'category': mission[2]}
        mission_result = watch_dog.mission(missionPara)
        watch_dog.record(missionPara, mission_result)
    watch_dog.close_connection()


if __name__ == "__main__":
    main()