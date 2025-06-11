import pymysql
import os
import requests
import json
from datetime import datetime
from datetime import timedelta

class DataBase:
    def __init__(self, main_para):
        self.host = main_para['host']
        self.user = main_para['user']
        self.password = main_para['password']
        self.database = main_para['database']
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.conn.cursor()

    def get_data(self, sql):
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        return data


    def close_db(self):
        self.cursor.close()
        self.conn.close()

    def get_max_pgm_date(self):
        sql = "SELECT MAX(time_send) FROM db_pgm"
        data = self.get_data(sql)
        return data[0][0]

    def duplicate_remove(self, list):
        result_list = []
        for item in list:
            id = item['processId']
            self.cursor.execute("SELECT * FROM db_pgm WHERE pgm_id=%s", (id,))
            row = self.cursor.fetchone()
            if not row:
                result_list.append(item)
        return result_list

    def regPGM(self, id, foler, name, sendTime, skUser):
        # 获取当前最大的bid
        self.cursor.execute(f"SELECT MAX(B_id) FROM bus_detail ")
        max_bid = self.cursor.fetchone()[0]
        current_date = datetime.now().strftime('%Y%m%d')
        downloadTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 如果没有找到最大的B_id，则将其设置为0
        if max_bid is None:
            max_bid = 0
            # 生成新的B_id，将最后三位自增
            max_bid_int = int(max_bid)
            max_bid = str(max_bid_int + 1).zfill(3)
            new_bid = f"{current_date}{max_bid}"
        else:
            max_bid_int = int(str(max_bid)[8:11])
            new_bid = str(max_bid_int + 1).zfill(3)
            new_bid = f"{current_date}{new_bid}{0}"
        # 任务添加模块
        user_id = task_generator(name)
        # 写入db_pgm表格
        dirSrc = os.path.join(r'\\172.27.7.188\Mod_TestE\03. PGM Download', foler, id)
        dirLocal = os.path.join(r'\\172.27.154.57\MTE Server\PGM', name)
        insert_query = """
                    INSERT INTO db_pgm (pgm_id, time_send, sk_hynix_charger, pgm_name, dir_local, dir_src, time_down, charger)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    time_send = VALUES(time_send),
                    sk_hynix_charger = VALUES(sk_hynix_charger),
                    pgm_name = VALUES(pgm_name),
                    dir_local = VALUES(dir_local),
                    dir_src = VALUES(dir_src),
                    time_down = VALUES(time_down),
                    charger = VALUES(charger);
                    """
        insert_data = (id, sendTime, skUser, name, dirLocal, dirSrc, downloadTime, user_id)
        self.cursor.execute(insert_query, insert_data)
        insert_query = """
                    INSERT INTO bus_detail (B_id, B_Category, Occur_Time, Description, Solution, user_id, B_Status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    B_Category = VALUES(B_Category),
                    Occur_Time = VALUES(Occur_Time),
                    Description = VALUES(Description),
                    Solution = VALUES(Solution),
                    user_id = VALUES(user_id),
                    B_Status = VALUES(B_Status);
                    """
        # 增加低收率任务自动关闭功能
        status = '0' if '저수율' in name else '1'
        insert_data_task = (new_bid, "6", downloadTime, id, name, user_id, status)
        self.cursor.execute(insert_query, insert_data_task)
        self.conn.commit()


class OMS:
    def __init__(self):
        self.loginUrl = 'https://apihtts.skhynix.com/auth/sign/in'
        self.listUrl = 'https://apihtts.skhynix.com/bpms/work'
        self.pgmUrl = 'https://apihtts.skhynix.com/bpms/official'
        self.fileUrl = 'https://apihtts.skhynix.com/mes/file/download'

    def logIn(self):
        headers = {'Content-Type': 'application/json'}  # 设置请求头为JSON格式
        data = {"id": "Z130157", "password": "qAC9WtGDgt5v6rfU/KmBFg=="}
        response = requests.post(self.loginUrl, data=json.dumps(data), headers=headers)
        if response.status_code == 200:
            response_data = response.json()
            session_token = response_data['token']
            authToken = {
                'Authorization': f"Bearer {session_token}",  # 假设使用Bearer令牌
                'cookie': response.cookies.get_dict()  # 获取cookie字典
            }
            return authToken
        return None

    def getList(self, authToken, beginDate, endDate):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Authorization': authToken['Authorization'], # 仅包含Authorization头
        }
        query_params = {
            'searchType': 'COMPLETED',
            'factoryId': ['OSMOD', 'OSMOT', 'OSFGMOT'],
            'companyId': 'HITECH',
            'assignedUserName': '',
            'processId': '',
            'processGroup': '',
            'workName': '',
            'organizationWorkIncluded': 'true',
            'delegationWorkIncluded': 'false',
            'beginDate': beginDate,
            'endDate': endDate
        }
        response = requests.get(self.listUrl, params=query_params, headers=headers)
        if response.status_code == 200:
            # 处理响应内容
            content = json.loads(response.text)
            # 处理内容
            result_list = []
            for item in content:
                process_id = item.get('processId')
                process_name = item.get('processName')
                start_date_time = item.get('startDateTime')
                skCharger = item.get('assignedUserName')
                # 如果 process_id、process_name 和 start_date_time 都存在，则添加到结果列表中
                if process_id and process_name and start_date_time:
                    result_list.append({
                        'processId': process_id,
                        'processName': process_name,
                        'startDateTime': start_date_time,
                        'skCharger': skCharger
                    })
            return result_list
        else:
            print(f"请求失败，状态码: {response.json()}")

    def getFile(self, authToken, id):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Authorization': authToken['Authorization'],  # 仅包含Authorization头
        }
        query_params = {
            'processId': id,
            'workSequence': '1',
        }
        response = requests.get(self.pgmUrl, params=query_params, headers=headers)
        if response.status_code == 200:
            # 处理响应内容
            content = json.loads(response.text).get('file')
            return content
        else:
            print(f"请求失败，状态码: {response.json()}")

    def fileDownload(self, authToken, id, fileList, foler):
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Authorization': authToken['Authorization'],  # 仅包含Authorization头
        }
        for item in fileList:
            print(item)
            filePath = os.path.join(foler, item['fileName'])
            if os.path.exists(filePath):
                # 如果文件存在，则删除源文件
                os.remove(filePath)
            query_params = {
                'id': item['fileDownloadId'],
                'userId': 'Z130157',
                'bpmsProcessId': id,
                'bpmsWorkSequence': '1'
            }
            response = requests.get(self.fileUrl, params=query_params, headers=headers)
            if response.status_code == 200:
                # 处理响应内容
                with open(filePath, "wb") as file:
                    file.write(response.content)
            else:
                print(f"请求失败，状态码: {response.json()}")




def main():
    dev = {
        'host': "localhost",
        'user': "remoteuser",
        'password': "password",
        'database': "modulemte",
        'folder': r'D:\03. PGM Download'
    }
    mfg = {
        'host': "172.27.154.57",
        'user': "remoteuser",
        'password': "password",
        'database': "modulemte",
        'folder': r'\\172.27.7.188\Mod_TestE\03. PGM Download'
    }

    myDB = DataBase(dev)
    # 获取查询时间，DB中最后一条记录-当前日期
    max_date = myDB.get_max_pgm_date().date().strftime('%Y-%m-%d')
    current_date = datetime.now().strftime('%Y-%m-%d')
    # 操作OMS获取PGM信息
    myOms = OMS()
    token = myOms.logIn()
    # 获取PGM List（去除已经登记的项目）
    searchPGM = myOms.getList(token, max_date, current_date)
    if searchPGM:
        searchPGM = myDB.duplicate_remove(searchPGM)
        for pgm in searchPGM:
            # 获取PGM下载列表
            id = pgm['processId']
            name = pgm['processName']
            sendTime = pgm['startDateTime']
            skCharger = pgm['skCharger']
            fileList = myOms.getFile(token,id)
            folderDate = datetime.strptime(sendTime, "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")
            # 创建文件夹并下载文件
            foler = os.path.join(dev['folder'], folderDate, id)
            if not os.path.exists(foler):
                os.makedirs(foler)
            myOms.fileDownload(token, id, fileList, foler)
            # 登记数据并登记任务
            myDB.regPGM(id, folderDate, name, sendTime, skCharger)
    else:
        myDB.close_db()
        return None
    myDB.close_db()


def task_generator(my_string):
    string_lower = my_string.lower()
    print(string_lower)
    task_mapping = {
        '저수율': 'z130157',
        'et': 'z130157',
        'ern': 'z130157',
        'sorting': 'z130157',
        'rd': 'z110160',
        'sd': 'z110447',
        'ud': 'z110447',
        'server': 'z110160',
        'sv': 'z110160',
        'client': 'z110447',
        'pc': 'z110447',
        'at': 'z110447',
        'arn': 'z110447',
        'ahn': 'z110447',
        'ars': 'z110447',
        'common': 'z110447',
        'bios': 'z110447',
        'unisdk': 'z130157',
        'smart': 'z110447'
    }
    for key in task_mapping:
        if key in string_lower:
            return task_mapping[key]

    return 'z130157'  # 如果没有匹配项，则返回默认值


if __name__ == '__main__':
    main()