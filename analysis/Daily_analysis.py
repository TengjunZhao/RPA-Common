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
from scipy.stats import shapiro
import numpy as np


class DataSource:
    # 构造函数
    def __init__(self, host):
        self.host = host['host']
        self.user = host['user']
        self.password = host['password']
        self.database = host['database']
        self.con = self.connect_to_database()

    # 连接数据库
    def connect_to_database(self):
        self.con = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        return self.con

    # 执行SQL语句
    def execute_sql(self, sql, args=None):
        cur = self.con.cursor()
        cur.execute(sql, args)
        result = cur.fetchall()
        cur.close()
        return result

    # 获取KPI列表
    def getKPIList(self, workdt):
        sql = "SELECT DISTINCT data_type FROM db_kpi_describe WHERE workdt = %s"
        return [item[0] for item in self.execute_sql(sql, (workdt,))]

    def KPIValue(self, workdt, type):
        sql = f"SELECT * FROM db_kpi_describe WHERE workdt = %s AND data_type = %s"
        return self.execute_sql(sql, (workdt, type))

    def query_metric(self, numerator, denominator, table, workdt, formula, dateField):
        """
        通用查询函数，用于计算指标。
        Args:
            numerator (str): 分子字段名
            denominator (str): 分母字段名
            table (str): 表名
            workdt (str): 查询日期
            formula (str): SQL公式，例如 "(1 - sum(numerator)/sum(denominator)) * 100"
            dateField (str): 日期字段名
        Returns:
            list: 查询结果列表
        """
        sql = f"SELECT {formula} FROM {table} WHERE {dateField} = %s"
        return [item[0] for item in self.execute_sql(sql, (workdt,))]

    def close(self):
        self.con.close()

def cumYield(workdt, data):
    KPI_Value = data.KPIValue(workdt, 'c_yield')
    print(KPI_Value)

def mainKPI(kpiList, workdt, data):
    for kpi in kpiList:
        print(f"【当前分析的指标是{kpi}】")
        KPI_Value = data.KPIValue(workdt, kpi)
        is_normal = KPI_Value[0][2]
        sigma = KPI_Value[0][5]
        min_name = KPI_Value[0][7]
        print(f"正态性：{is_normal}， sigma：{sigma}， 最小值：{min_name}")
    # workdt前一天日期
    workdtEnd = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    # retest rate指标查询
    retest_formula = f"(1 - sum({'retest_cnt'})/sum({'test_cnt'})) * 100"
    retest = data.query_metric('test_cnt', 'retest_cnt', 'db_et_retest', workdtEnd, retest_formula, 'date_val')
    print(retest)

    # DC指标查询
    dcfa_formula = f"sum({'dcfa_qty'})/sum({'in_qty'}) * 1000000"
    dcfa = data.query_metric('dcfa_qty', 'in_qty', 'db_dcfa', workdtEnd, dcfa_formula, 'workdt')
    print(dcfa)

def main():
    hostLocal = {
        'host': 'localhost',
        'user':'remoteuser',
        'password': 'password',
        'database': 'cmsalpha',
    }
    # 获取当前日期，格式YYYYMMDD
    workdt = (datetime.datetime.now()).strftime('%Y%m%d')
    mydata = DataSource(hostLocal)
    kpiList = mydata.getKPIList(workdt)
    mainKPI(kpiList, workdt, mydata)
    mydata.close()


if __name__ == '__main__':
    main()