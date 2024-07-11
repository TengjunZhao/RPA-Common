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
import Daily_Yield_Analysis as dya


def main():
    host = {
        'host': 'localhost',
        'user': 'remoteuser',
        'password': 'password',
        'database': 'cmsalpha',
    }
    workdt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    dateStandard = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    dateStandard_dt = datetime.datetime.strptime(dateStandard, '%Y-%m-%d')
    searchStartTime = dateStandard_dt + datetime.timedelta(hours=7)
    searchEndTime = searchStartTime + datetime.timedelta(days=1)
    searchStartTime_str = searchStartTime.strftime('%Y-%m-%d %H:%M:%S')
    searchEndTime_str = searchEndTime.strftime('%Y-%m-%d %H:%M:%S')

    mydb = dya.DBInfo(host)
    operList = ['5600','5710','5700','5780']
    equipList = mydb.get_list(
        'db_testtime_analysis',
        'm_table',
        'workdt',
        workdt,
        workdt,
    )
    equipList = extract_prefixes(equipList)
    column = ['product_mode','tech_name','die_density','product_density',
              'module_type','oper','model','m_table','stddev_test_time',
              'min','max','quater1','quater2','quater3']
    condition =  ['workdt = ' + workdt]
    records = mydb.execute_query(
        'db_testtime_analysis',
        columns=column,
        conditions=condition,
        order_by='m_table asc'
    )
    # 保存满足条件的记录
    filtered_records = []

    # 遍历每一条记录
    for record in records:
        # 获取 stddev_test_time, min, max 列的值
        stddev_test_time = record[8]
        min_val = record[9]
        max_val = record[10]
        Q1 = record[11]
        Q2 = record[12]
        Q3 = record[13]
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        # 判断是否有离群点
        if min_val<lower_bound or max_val>upper_bound:
            filtered_records.append(record)
    for item in filtered_records:
        print(item)


def extract_prefixes(equip_list):
    prefixes = set()
    for equip in equip_list:
        prefix = re.match(r'^[A-Z]+[0-9]+', equip)
        if prefix:
            prefixes.add(prefix.group(0))
    return list(prefixes)


if __name__ == '__main__':
    main()
