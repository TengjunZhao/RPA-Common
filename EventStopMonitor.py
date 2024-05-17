import pymysql
from datetime import datetime, timedelta


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

    # 获取相比一监视日期前一天各个制品的收率
    def getStandard(self):
        # 求比self.maxDate前一天
        searchDate = self.maxDate - timedelta(days=1)
        sql = ("select product, sum(bin1_cnt)/sum(test_cnt) as p_yield from cmsalpha.db_primeyieldet dp "
               "where date_val =%s group by product ;")
        self.cursor.execute(sql, searchDate)
        result = self.cursor.fetchall()
        return result

    def importer(self, equip, list, standard):
        for l in list:
            sYield = None
            mark = 0
            # 查询Yield
            sql = ("select `product`, `yield`, `test_cnt` from db_primeyieldet where lot_id = %s")
            self.cursor.execute(sql, l['lot'])
            result = self.cursor.fetchone()
            if result:
                product = result[0]
                pYield = result[1]
                inQty = result[2]
            else:
                product = None
                pYield = None
                inQty = None
            if product:
                for s in standard:
                    if product == s[0]:
                        sYield = s[1] * 100
                        if pYield < sYield:
                            mark = 1
                        break
            sql = ("""
                        insert into db_eventmonitor_et 
                        (`EQUIP_Name`, `lot_no`, `STOP_TIME`, `RUN_TIME`, `time_val`, `yield`,`in_qty`, `standard`, `mark`) 
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        on duplicate key update 
                            `RUN_TIME` = values(`RUN_TIME`), 
                            `time_val` = values(`time_val`), 
                            `lot_no` = values(`lot_no`), 
                            `yield` = values(`yield`),
                            `in_qty` = values(`in_qty`),
                            `standard` = values(`standard`),
                            `mark` = values(`mark`);
                    """)
            self.cursor.execute(sql, (equip, l['lot'], l['stop'], l['run'], l['val'], pYield, inQty, sYield, mark))
            self.conn.commit()


# 梳理每个设备党日的Stop-Run时间
def filterList(lists):
    currentList = []
    resultList = []
    record = {
        'lot': '',
        'stop': '',
        'run': '',
        'val': '',
    }
    for list in lists:
        if not record['stop'] and list[4] == 'STOP':
            record['stop'] = list[6]
        elif record['stop'] and record['run'] and list[4] == 'STOP':
            currentList.append(record)
            record = {
                'lot': '',
                'stop': '',
                'run': '',
                'val': '',
            }
            record['stop'] = list[6]
        elif record['stop'] and list[4] == 'STOP':
            pass
        elif list[4] == 'RUN':
            record['run'] = list[6]
            record['lot'] = list[3]
    for list in currentList:
        val = list['run'] - list['stop']
        list['val'] = val
        # 只保留超时2分钟的记录
        if val.seconds > 300:
            resultList.append(list)
    return resultList


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
    standardYield = myDataAquirer.getStandard()
    print(standardYield)
    for equip in equipList:
        print(equip)
        rows = myDataAquirer.getRecord(equip)
        list = filterList(rows)
        # 将equip，List写入数据库
        myDataAquirer.importer(equip,list, standardYield)




if __name__ == "__main__":
    main()