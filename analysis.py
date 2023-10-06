import os
import glob
import pymysql
from openpyxl import load_workbook
import re
import time
import logging


class CustomLogger:
    def __init__(self, log_path):
        self.log_path = log_path
        self.logger = self.setup_logger()

    def setup_logger(self):
        # 创建一个新的logger对象
        logger = logging.getLogger('custom_logger')
        logger.setLevel(logging.INFO)

        # 创建文件处理器并设置格式
        file_handler = logging.FileHandler(self.log_path)
        log_format = '[%(levelname)s] %(message)s'
        formatter = logging.Formatter(log_format)
        file_handler.setFormatter(formatter)

        # 将文件处理器添加到logger
        logger.addHandler(file_handler)

        return logger

    def log_issue_lot_info(self, info):
        self.logger.info('#####################Issue Lot Information:########################')
        self.logger.info(info)

    def log_equip_info(self, info):
        self.logger.info('#####################Equip Information:########################')
        self.logger.info(info)

    def log_device_info(self, info):
        self.logger.info('#####################Device Information:########################')
        self.logger.info(info)

    def log_rej_info(self, info):
        self.logger.info('#####################Rej Information:########################')
        self.logger.info(info)

    def log_support_data(self, info):
        self.logger.info('#####################Support Data:########################')
        self.logger.info(info)


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
    log_path = r'E:/sync/临时存放'

    # custom_logger.log_issue_lot_info('[WVC9N0489209][DC Trend 离群点][4.55]')
    # custom_logger.log_equip_info('[Table内][Site列联表]')
    # custom_logger.log_device_info('[Device间]')
    # custom_logger.log_rej_info('[Fail Item]')
    # custom_logger.log_support_data('[Run-Lot间]')
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
        # log_name = f"{issue_info['time'].strftime('%Y%m%d%H%M%S')}_{eval(issue_info['observe'][0][0])}.log"
        # log_dir = os.path.join(log_path, log_name)
        # custom_logger = CustomLogger(log_dir)
        # custom_logger.log_issue_lot_info(issue_info)
        # 在日期部分添加引号，将其表示为字符串
        pattern = r"\((.*?)\)"
        matches = re.findall(pattern, issue_info['observe'])
        extracted_data = [match.split(", ")[0:2] for match in matches]
        print(extracted_data)


if __name__ == '__main__':
    main()
    # os.system('pause')
