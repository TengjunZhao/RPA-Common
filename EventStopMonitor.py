import pymysql
from datetime import datetime


# 处理数据库相关数据
class DataAquirer:
    def __init__(self, host):
        self.conn = pymysql.connect(
            host=host['host'],
            user=host['usr'],
            password=host['pwd'],
            db=host['db'],
            charset='utf8')
        self.cursor = self.conn.cursor()
        self.maxDate = self.getDate()

    # 获取数据库中最新的数据日期
    def getDate(self):
        sql = 'select max(`Date`) from db_event_et'
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return result[0]

    # 根据最新的日期，获取当天所有的设备
    def getEquip(self):
        sql = 'select distinct(EQUIP_Name) from db_event_et where `DATE` = %s'
        self.cursor.execute(sql, self.maxDate)
        result = self.cursor.fetchall()
        # 提取设备List
        equipList = []
        for equip in result:
            equipList.append(equip[0])
        return equipList

    def getRecord(self, equip):
        sql = ("select * from db_event_et "
               "where `EQUIP_Name` = %s and `Date` = %s"
               "and (Event2 = 'STOP' or Event2 = 'RUN')"
               "order by TRANSMISSION_TIME ASC")
        self.cursor.execute(sql, (equip, self.maxDate))
        result = self.cursor.fetchall()
        return result


# 梳理每个设备党日的Stop-Run时间
def filterList(lists):
    currentList = []
    record = {
        'lot': '',
        'stop': '',
        'run': '',
        'val': '',
    }
    for list in lists:
        # print(list[4], list[6])
        if not record['stop'] and list[4] == 'STOP':
            record['stop'] = list[6]
        elif list[4] == 'RUN':
            record['run'] = list[6]
        elif record['stop'] and record['run'] and list[4] == 'STOP':
            currentList.append(record)
            record = {
                'lot': '',
                'stop': '',
                'run': '',
                'val': '',
            }
    for list in currentList:
        print (list)
    return currentList


def main():
    host_prod = {
        'host': '172.27.154.57',
        'usr': 'remoteuser',
        'pwd': 'password',
        'db': 'cmsalpha',
    }
    host_test = {
        'host': 'localhost',
        'usr': 'remoteuser',
        'pwd': 'password',
        'db': 'cmsalpha',
    }
    myDataAquirer = DataAquirer(host_test)
    equipList = myDataAquirer.getEquip()
    for equip in equipList:
        print(equip)
        rows = myDataAquirer.getRecord(equip)
        list = filterList(rows)




if __name__ == "__main__":
    main()