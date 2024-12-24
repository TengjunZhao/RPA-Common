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

    def getLotList(self):
        # 获取昨日日期YYYYMMDD格式，并转换为字符串
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        sql = "SELECT distinct(lot_id), `workdt` from db_yielddetail WHERE workdt = %s"
        self.cursor.execute(sql, yesterday)
        data = self.cursor.fetchall()
        return data

    def getLotInform(self, lot):
        sql = ("SELECT oper_old,trans_time, in_qty, out_qty from db_yielddetail "
               "WHERE lot_id = %s"
               "ORDER BY trans_time asc")
        self.cursor.execute(sql, lot)
        data = self.cursor.fetchall()
        return data

    def insertOrUpdateData(self, lot, data):
        sql = ("INSERT INTO db_split_monitor (lot_id, workdt, oper, trans_time, in_qty, out_qty) "
               "VALUES (%s, %s, %s, %s, %s, %s) "
               "ON DUPLICATE KEY UPDATE "
               "workdt = VALUES(workdt), "
               "oper = VALUES(oper), "
               "in_qty = VALUES(in_qty), "
               "out_qty = VALUES(out_qty)")

        for record in data:
            self.cursor.execute(sql, (lot[0],lot[1], record[0], record[1], record[2], record[3]))
        self.conn.commit()

    def close_db(self):
        self.cursor.close()
        self.conn.close()


def main():
    main_para_local = {
        'host': 'localhost',
        'user': 'remoteuser',
        'password': 'password',
        'database': 'cmsalpha'
    }
    main_para_apply = {
        'host': '172.27.154.57',
        'user': 'remoteuser',
        'password': 'password',
        'database': 'cmsalpha'
    }
    # 连接数据库
    db = DataBase(main_para_local)
    # 查询数据
    lotList = db.getLotList()
    for lot in lotList:
        inform = db.getLotInform(lot[0])
        sum_bal = 0
        out = inform[0][3]
        for i in inform:
            inQty = i[2]
            bal = inQty - out
            out = i[3]
            sum_bal += bal
        if sum_bal:
            db.insertOrUpdateData(lot, inform)
            print(lot, inform)
    # 关闭数据库连接
    db.close_db()


if __name__ == '__main__':
    main()