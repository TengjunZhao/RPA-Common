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
            self.sql = "SELECT * FROM modulemte.db_watchdog_mission_list WHERE category = '0' AND STATUS = '1';"
        else:
            self.sql = "SELECT * FROM modulemte.db_watchdog_mission_list WHERE category <> '0' AND STATUS = '1';"
        # 执行查询并获取结果
        with self.connection.cursor() as cursor:
            cursor.execute(self.sql)
            results = cursor.fetchall()
        # 返回任务列表结果
        return results

    def mission(self, para):
        # 获取系统时间相对昨天的日期
        today = datetime.now()
        yesterday = today - timedelta(days=1)
        date_formats = ["%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"]  # 支持的日期格式列表
        self.sql = (f"SELECT `indicator` FROM modulemte.db_watchdog_mission_list "
                    f"WHERE type = '{para['type']}' AND category = '0';")
        with self.connection.cursor() as cursor:
            cursor.execute(self.sql)
            indicator = cursor.fetchone()[0]
        self.sql = (f"SELECT MIN(id) FROM modulemte.db_watchdog_mission_list WHERE type = '{para['type']}';")
        with self.connection.cursor() as cursor:
            cursor.execute(self.sql)
            prime_watch_id = cursor.fetchone()[0]
        self.sql = (f"SELECT `judgement` FROM modulemte.db_watchdog_record "
                    f"WHERE watch_id = {prime_watch_id} AND DATE(id) = CURDATE() ORDER BY id DESC LIMIT 1")
        with self.connection.cursor() as cursor:
            cursor.execute(self.sql)
            try:
                prime_result = cursor.fetchone()[0]
            except TypeError:
                prime_result = '1'
        if para['spec_desc'] == 'Max':
            self.sql = f"SELECT Max({para['indicator']}) FROM {para['db_name']};"
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                results = cursor.fetchone()
                result = results[0]
                if para['spec'] == 'yestoday':
                    for date_format in date_formats:
                        try:
                            result_date = datetime.strptime(str(result), date_format)
                            # 检查result是否是昨天的日期
                            if result_date.date() == yesterday.date():
                                return (result, True)
                            else:
                                return (result, False)
                        except ValueError:
                            continue  # 如果解析失败，尝试下一个日期格式
        elif para['spec_desc'] in ['>','<', '>=', '<='] and prime_result == '0':
            desc_mapping = {
                '>': '<=',
                '<': '>=',
                '>=': '<',
                '<=': '>',
            }
            spec_desc = desc_mapping.get(para['spec_desc'], para['spec_desc'])

            if indicator == 'workdt':
                searchDate = yesterday.strftime("%Y%m%d")
            self.sql = (f"SELECT COUNT(*) FROM {para['db_name']} "
                        f"WHERE {indicator} = '{searchDate}' AND {para['indicator']} {spec_desc} {para['spec']};")
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                results = cursor.fetchone()[0]
                # 判断results是否为空
                if results == 0:
                    return (results, True)
                else:
                    return (results, False)
        elif para['spec_desc'] == 'P_Chart' and prime_result == '0':
            if indicator == 'workdt':
                search_end = yesterday.strftime("%Y%m%d")
                search_start = (yesterday - timedelta(days=30)).strftime("%Y%m%d")
            elif indicator == 'date_val' or indicator == 'date':
                search_end = yesterday.strftime("%Y-%m-%d")
                search_start = (yesterday - timedelta(days=30)).strftime("%Y-%m-%d")
            indicator_list = para['indicator'].split(',')
            groupSizeField = indicator_list[0]
            sampleField =  indicator_list[1]
            self.sql = (f"SELECT {indicator},SUM({groupSizeField}), SUM({sampleField}) FROM {para['db_name']} "
                        f"WHERE {indicator} BETWEEN '{search_start}' AND '{search_end}' "
                        f"GROUP BY {indicator} ORDER BY {indicator} ASC;")
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                results = cursor.fetchall()
            fields = []
            group_size = []
            sample = []
            for item in results:
                fields.append(item[0])
                group_size.append(item[1])
                sample.append(item[2])
            p = P_Chart(fields, sample, group_size, para['spec'])
            result = p.p_chart_judge()
            if result:
                result_string = "["
                for item in result:
                    result_string += "("
                    for key, value in item.items():
                        result_string += f"{value}, "
                    result_string = result_string.rstrip(", ")  # 去除末尾的逗号和空格
                    result_string += "), "

                result_string = result_string.rstrip(", ")  # 去除最后一个逗号和空格
                result_string += "]"
                return (result_string, False)
            else:
                return ('-', True)
        elif para['spec_desc'] == 'Obsolete' and prime_result == '0':
            indicator_list = para['indicator'].split(',')
            searchDate = yesterday.strftime("%Y-%m-%d")
            self.sql = (f"SELECT {indicator_list[0]},{indicator_list[1]} FROM {para['db_name']} WHERE {indicator} = '{searchDate}';")
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                results = cursor.fetchall()
            fields = []
            group_size = []
            sample = []
            for item in results:
                fields.append(item[0])
                group_size.append('')
                sample.append(item[1])
            p = P_Chart(fields, sample, group_size, para['spec'])
            result = p.obsolete()
            if result:
                result_string = "["
                for item in result:
                    result_string += "("
                    for key, value in item.items():
                        result_string += f"{value}, "
                    result_string = result_string.rstrip(", ")  # 去除末尾的逗号和空格
                    result_string += "), "

                result_string = result_string.rstrip(", ")  # 去除最后一个逗号和空格
                result_string += "]"
                return (result_string, False)
            else:
                return ('-', True)

    def record(self, para, res):
        # 获取当前系统时间
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if res[1]:
            self.sql = (f"INSERT INTO modulemte.db_watchdog_record "
                        f"(id, watch_id, spec, observe, judgement, analysis_run, charger) VALUES"
                        f"('{now}', '{para['id']}', '{para['spec']}','{res[0]}','0','-','auto')")
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                self.connection.commit()
        else:
            self.sql = (f"SELECT COUNT(*) from modulemte.db_watchdog_record WHERE observe = '{res[0]}';")
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                try:
                    exist = cursor.fetchone()[0]
                except:
                    exist = 0
            if exist != 0:
                return
            self.sql = (f"INSERT INTO modulemte.db_watchdog_record "
                        f"(id, watch_id, spec, observe, judgement, analysis_run, charger) VALUES"
                        f"('{now}', '{para['id']}', '{para['spec']}','{res[0]}','1','0','z130090')")
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                self.connection.commit()
            # 插入日常任务
            self.sql = "SELECT MAX(B_id) FROM modulemte.bus_detail"
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                maxB_id = cursor.fetchone()[0]
            self.sql = f"SELECT type FROM modulemte.db_watchdog_mission_list WHERE id = '{para['id']}'"
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                desc = cursor.fetchone()[0]
            # 计算新的 B_id
            str_id = str(maxB_id)[8:11]
            id = int(str_id) + 1
            if id < 100:
                if id < 10:
                    id = '00' + str(id)
                else:
                    id = '0' + str(id)
            today = datetime.now().strftime("%Y%m%d")
            B_id = today + str(id) + '5'
            self.sql = (f"INSERT INTO modulemte.bus_detail "
                        f"(B_id, P_id, B_Category, Occur_Time, Close_Time, Description, Solution, user_id, B_Status) VALUES"
                        f"('{B_id}', Null, '4','{now}',NULL, '{desc}','','z130090', '1')")
            with self.connection.cursor() as cursor:
                cursor.execute(self.sql)
                self.connection.commit()

    def close_connection(self):
        # 关闭数据库连接
        if self.connection:
            self.connection.close()


class P_Chart():
    def __init__(self, field, observe, sample_size, sigma):
        self.observe = observe
        self.sample_size = sample_size
        self.field = field
        self.sigma = float(sigma)

    def p_chart_judge(self):
        p_values = [float(obs) / float(sample_size) for obs, sample_size in zip(self.observe, self.sample_size)]
        # 计算总体P值均值和标准差
        mean_p = np.mean(p_values)
        std_p = np.std(p_values)
        # 判异结果
        abnormal_points = []
        for i, (obs, sample_size) in enumerate(zip(self.observe, self.sample_size)):
            p = float(obs) / float(sample_size)
            n = float(sample_size)
            UCL = mean_p + self.sigma * np.sqrt((p * (1 - p)) / n)
            LCL = mean_p - self.sigma * np.sqrt((p * (1 - p)) / n)
            if p > UCL or p < LCL:
                abnormal_points.append({
                    'field': self.field[i],  # 异常点的field
                    'p_value': round(p * 1000000),  # 异常点的p_value
                    'UCL': round(UCL * 1000000),  # 控制限的UCL
                    'LCL': round(LCL * 1000000),  # 控制限的LCL
                    'result': True  # 结果为异常
                })
        if abnormal_points:
            return abnormal_points
        else:
            return False  # 无异常

    def obsolete(self):
        outliers = []  # 用于存储离群点信息的列表
        mean_obs = float(np.mean(self.observe))
        std_obs = float(np.std(self.observe))
        # 标准差是0情况特殊处理
        if std_obs == 0:
            return False
        for i, obs in enumerate(self.observe):
            z_score = (float(obs) - mean_obs) / (self.sigma * std_obs)
            if abs(z_score) > 3:  # 判断是否为离群点，可以根据需要调整阈值
                outliers.append({
                    'field': self.field[i],
                    'observe': obs,
                    'result': True  # 表示是离群点
                })
        if not outliers:  # 如果离群点列表为空，返回 False
            return False
        else:
            return outliers


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
                    'type':mission[1],
                    'db_name': mission[4],
                    'indicator': mission[3],
                    'spec': mission[6],
                    'spec_desc': mission[5],
                    'status': mission[7],
                    'category': mission[2]}
        mission_result = watch_dog.mission(missionPara)
        watch_dog.record(missionPara, mission_result)

    missionList = watch_dog.check_mission('daily')
    for mission in missionList:
        missionPara = {'id': mission[0],
                       'type': mission[1],
                       'db_name': mission[4],
                       'indicator': mission[3],
                       'spec': mission[6],
                       'spec_desc': mission[5],
                       'status': mission[7],
                       'category': mission[2]}
        mission_result = watch_dog.mission(missionPara)
        if mission_result:
            watch_dog.record(missionPara, mission_result)

    watch_dog.close_connection()


if __name__ == "__main__":
    main()