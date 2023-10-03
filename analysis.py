import os
import glob
import pymysql
from openpyxl import load_workbook
import re
import time


def get_watch_issue(host):
    host['database'] = 'modulemte'
    con = pymysql.connect(host=host['host'], user=host['user'], password=host['password'], database=host['database'])
    cur = con.cursor()
    sql = "SELECT sub.id, sub.watch_id, sub.observe, sub.judgement, sub.type, sub.category, sub.db_name, " \
          "sub.indicator, sub.spec, sub.spec_desc, sub.remark " \
          "FROM ( " \
          "SELECT r.id, r.watch_id, r.observe, r.judgement, l.type, l.category, l.db_name, l.indicator, " \
          "l.spec, l.spec_desc, l.remark, ROW_NUMBER() OVER (PARTITION BY r.watch_id ORDER BY r.id DESC) AS rn " \
          "FROM modulemte.db_watchdog_record r " \
          "INNER JOIN modulemte.db_watchdog_mission_list l ON r.watch_id = l.id " \
          "WHERE r.judgement = '1' AND l.category = '1') AS sub WHERE sub.rn = 1;"
    cur.execute(sql)
    return cur.fetchall()


def main():
    local_host = {
        'host': 'localhost',
        'user': 'remoteuser',
        'password': 'password',
        'database': None,
    }
    ali_host = {
        'host': '121.40.116.199',
        'user': 'remoteuser',
        'password': 'password',
        'database': None,
    }
    issue_info = {
        'time': None,
        'spec_desc':  None,
        'observe': None,
        'indicator': None,
        'db_name': None,
        'remark': None
    }
    watch_issues = get_watch_issue(local_host)
    for issue in watch_issues:
        issue_info['spec_desc'] = issue[9]
        issue_info['observe'] = issue[2]
        issue_info['db_name'] = issue[6]
        issue_info['indicator'] = issue[7]
        issue_info['time'] = issue[0]
        issue_info['remark'] = issue[10]
        print(issue_info)


if __name__ == '__main__':
    main()
    os.system('pause')
