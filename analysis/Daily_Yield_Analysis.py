import os
import glob
import pymysql
from openpyxl import load_workbook
import re
import time
import logging
import datetime
import ast
import scipy.stats as stats

class DBInfo:
    def __init__(self, host):
        self.host = host['host']
        self.user = host['user']
        self.password = host['password']
        self.database = host['database']
        self.con = self.connect_to_database()

    def connect_to_database(self):
        con = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        return con

    def execute_sql(self, sql, args=None):
        cur = self.con.cursor()
        cur.execute(sql, args)
        result = cur.fetchall()
        cur.close()
        return result

    def execute_query(self, table, columns, conditions, group_by=None, order_by=None):
        sql = f"SELECT {', '.join(columns)} FROM {table} WHERE {' AND '.join(conditions)}"
        if group_by:
            sql += f" GROUP BY {group_by}"
        if order_by:
            sql += f" ORDER BY {order_by}"
        return self.execute_sql(sql)

    def get_list(self, table, column, timeColumn, searchStartTime_str, searchEndTime_str, additional_conditions=None):
        conditions = [timeColumn + f" BETWEEN '{searchStartTime_str}' AND '{searchEndTime_str}'"]
        if additional_conditions:
            conditions.extend(additional_conditions)
        sql = f"SELECT DISTINCT {column} FROM {table} WHERE {' AND '.join(conditions)}"
        return [item[0] for item in self.execute_sql(sql)]


    def close(self):
        self.con.close()


class Analyzer:
    def __init__(self, db):
        self.db = db

    def cumData_by_equip(self, searchStart, searchEnd, columns, group_by, order_by):
        conditions = [
            f"workdt BETWEEN '{searchStart}' AND '{searchEnd}'",
            "oper_old = '5600'"
        ]
        return self.db.execute_query('db_yielddetail', columns, conditions, group_by, order_by)

    def prime_by_equip(self, searchStart, searchEnd, columns, group_by, order_by):
        conditions = [
            f"date_val BETWEEN '{searchStart}' AND '{searchEnd}'",
        ]
        return self.db.execute_query('db_primeyieldet', columns, conditions, group_by, order_by)

    def ETRawData(self, workdtStart, workdtEnd, dateValStart, dateValEnd):
        # 查询ET Yield信息
        db = self.db
        ETcumYield_byEquip = self.cumData_by_equip(
            workdtStart,
            workdtEnd,
            columns=["main_equip_id as equip", "sum(out_qty)/sum(in_qty) * 100 as c_yield"],
            group_by="main_equip_id",
            order_by="main_equip_id asc"
        )
        ETqty_byEquip = self.cumData_by_equip(
            workdtStart,
            workdtEnd,
            columns=["main_equip_id as equip", "sum(in_qty) as qty"],
            group_by="main_equip_id",
            order_by="main_equip_id asc"
        )
        ETPrimeYield_byEquip = self.prime_by_equip(
            dateValStart,
            dateValEnd,
            columns=["equip_id as equip", "sum(bin1_cnt)/sum(test_cnt)*100 as p_yield"],
            group_by="equip_id",
            order_by="equip_id asc"
        )
        combined_data = combine_data(ETcumYield_byEquip, ETPrimeYield_byEquip, ETqty_byEquip)
        return combined_data

def combine_data(yield_data, prime_yield_data,qty_data):
    combined_data = {}
    for equip, c_yield in yield_data:
        combined_data[equip] = {
            'c_yield': c_yield,
            'p_yield': None,
            'qty': None
        }
    for equip, qty in qty_data:
        if equip in combined_data:
            combined_data[equip]['qty'] = qty
        else:
            combined_data[equip] = {
                'c_yield': None,
                'p_yield': None,
                'qty': qty,
            }
    for equip, prime_yield in prime_yield_data:
        if equip in combined_data:
            combined_data[equip]['p_yield'] = prime_yield
        else:
            combined_data[equip] = {
                'c_yield': None,
                'p_yield': prime_yield,
                'qty': None,
            }
    return combined_data

def analyze_data(data):
    c_yields = [float(item['c_yield']) for item in data.values()]
    p_yields = [float(item['p_yield']) for item in data.values()]
    qtys = [float(item['qty']) for item in data.values()]

    # Kruskal-Wallis H Test for c_yield
    c_yield_stat, c_yield_p = stats.kruskal(*[float(item['c_yield']) for item in data.values()])
    if c_yield_p < 0.05:
        c_yield_diff = True
        worst_c_yield_equip = min(data.items(), key=lambda x: x[1]['c_yield'])[0]
    else:
        c_yield_diff = False
        worst_c_yield_equip = None

    # Kruskal-Wallis H Test for p_yield
    p_yield_stat, p_yield_p = stats.kruskal(*[float(item['p_yield']) for item in data.values()])
    if p_yield_p < 0.05:
        p_yield_diff = True
        worst_p_yield_equip = min(data.items(), key=lambda x: x[1]['p_yield'])[0]
    else:
        p_yield_diff = False
        worst_p_yield_equip = None

    # Kruskal-Wallis H Test for qty
    qty_stat, qty_p = stats.kruskal(*[float(item['qty']) for item in data.values()])
    if qty_p < 0.05:
        qty_diff = True
        lowest_qty_equip = min(data.items(), key=lambda x: x[1]['qty'])[0]
    else:
        qty_diff = False
        lowest_qty_equip = None

    results = {
        'c_yield_diff': c_yield_diff,
        'worst_c_yield_equip': worst_c_yield_equip,
        'p_yield_diff': p_yield_diff,
        'worst_p_yield_equip': worst_p_yield_equip,
        'qty_diff': qty_diff,
        'lowest_qty_equip': lowest_qty_equip
    }
    return results


def main():
    log_path = r'C:\Users\Tengjun Zhao\Desktop\test'
    host = {
        'host': 'localhost',
        'user': 'remoteuser',
        'password': 'password',
        'database': 'cmsalpha',
    }

    try:
        # 获取昨天以及三天前的日期，格式是YYYYMMDD/YYYY-MM-DD
        workdtEnd = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        workdtStart = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y%m%d')
        dateValEnd = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        dateValStart = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
        mydb = DBInfo(host)
        myAnalyzer = Analyzer(mydb)
        etData = myAnalyzer.ETRawData(workdtStart, workdtEnd, dateValStart, dateValEnd)
        print(etData)

        # 对数据进行分析
        analysis_results = analyze_data(etData)
        print(analysis_results)
    except Exception as e:
        logging.error("Error occurred", exc_info=True)
    finally:
        mydb.close()

if __name__ == '__main__':
    main()
