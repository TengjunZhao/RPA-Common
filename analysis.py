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


class CustomLogger:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.setup_logger()

    def setup_logger(self):
        # 创建一个新的logger对象
        self.logger = logging.getLogger(self.log_dir)
        self.logger.setLevel(logging.INFO)

        # 创建文件处理器并设置格式
        file_handler = logging.FileHandler(self.log_dir, encoding='utf-8')
        log_format = '[%(levelname)s] %(message)s'
        formatter = logging.Formatter(log_format)
        file_handler.setFormatter(formatter)

        # 将文件处理器添加到logger
        self.logger.addHandler(file_handler)

    def log_header(self, info):
        headers = {
            'info': 'Issue Lot Information',
            'equip': 'Equip Information',
            'device': 'Device Information',
            'rej': 'Rej Information',
            'support': 'Support Data',
        }
        if info in headers:
            header_text = headers[info]
            self.logger.info(f'#####################{header_text}:########################')
        else:
            self.logger.info('#####################Unknown Information:########################')

    def log_info(self, info):
        self.logger.info(info)

    def clear_logs(self):
        # 获取当前日志记录器的处理器列表
        handlers = self.logger.handlers
        # 遍历处理器并移除所有处理器的内容
        for handler in handlers:
            if isinstance(handler, logging.FileHandler):
                handler.acquire()
                handler.stream.truncate(0)  # 清空日志文件


class Data:
    def __init__(self, info, host, host_r, log):
        self.info = info
        self.host = host
        self.host_r = host_r
        self.log = log

    # 区分Watch任务种类，1为单Lot任务；2为指数监控
    def get_analysis_category(self):
        return 1 if int(self.info['watch_id']) in (6, 12) else 2

    def single(self, observe):
        lot_id = self.extract_lot_id(observe)

        # 1-1获取cum yield基本信息
        cum_yield_results = self.s_cum_yield(lot_id)
        self.log_results(cum_yield_results)

        # 1-2获取prime yield基本信息
        prime_yield_results = self.s_prime_et_yield(lot_id)
        self.log_results(prime_yield_results)

    def extract_lot_id(self, observe):
        match = re.search(r'[A-Z0-9]+', observe)
        if match:
            return match.group()
        return None

    def log_results(self, results):
        for result in results:
            result = ', '.join(map(str, result))
            self.log.log_info(str(result))

    def connect_to_database(self, database):
        return pymysql.connect(host=self.host['host'], user=self.host['user'],
                               password=self.host['password'], database=database)

    def execute_sql(self, connection, sql, params):
        with connection.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchall()
        return result

    def s_cum_yield(self, lot_id):
        con = self.connect_to_database('cmsalpha')
        sql = """SELECT trans_time, device, fab, owner, oper_old, equip_model,
                 main_equip_id, in_qty, out_qty, out_qty/in_qty*100 as yield
                 FROM db_yielddetail WHERE `lot_id` = %s
                 ORDER BY trans_time asc;"""
        return self.execute_sql(con, sql, lot_id)

    def s_prime_et_yield(self, lot_id):
        con = self.connect_to_database('cmsalpha')
        sql = """SELECT p.date_val, p.product, c.fab, c.owner, '5600' as oper, p.equip_id, p.test_cnt,
                 p.bin1_cnt, p.yield
                 FROM (SELECT DISTINCT lot_id, date_val, product, equip_id, test_cnt, bin1_cnt, yield
                       FROM db_primeyieldet WHERE lot_id = %s ) p
                 LEFT JOIN (SELECT DISTINCT lot_id, fab, owner FROM db_yielddetail ) c
                 ON p.lot_id = c.lot_id;"""
        return self.execute_sql(con, sql, lot_id)

    def getOneFactorData(self, table, factor, eventQty, caseQty, conditionField, condition):
        con = self.connect_to_database('cmsalpha')
        sql = f"""SELECT {factor} AS factor,
                        SUM({eventQty}) AS inQty,
                        SUM({caseQty}) AS outQty,
                        SUM({caseQty}) / SUM({eventQty}) AS new_yield
                 FROM {table} 
                 WHERE {conditionField} = %s
                 GROUP BY factor;"""
        result = self.execute_sql(con, sql, (condition,))
        processed_data = {}
        for row in result:
            factor = row[0]
            in_qty = int(row[1])
            out_qty = int(row[2])
            new_yield = float(row[3])
            processed_data[factor] = {'in_qty': in_qty, 'out_qty': out_qty, 'new_yield': new_yield}
        # 卡方独立性检验
        contingency_table = [[data['in_qty'], data['out_qty']] for data in processed_data.values()]
        chi2_stat, p_val, dof, expected = stats.chi2_contingency(contingency_table)
        # 返回处理后的数据以及相关性分析结果
        return processed_data, chi2_stat, p_val, dof, expected


def get_watch_issue(host):
    host['database'] = 'modulemte'
    con = pymysql.connect(host=host['host'], user=host['user'],
                          password=host['password'], database=host['database'],
                          charset='utf8')
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


def log_factor_analysis(a, factor_name, searchDate, custom_logger, DB):
    """
    对指定因素进行分析，并记录相关统计数据。

    参数:
    - a: 数据处理对象，拥有getOneFactorData方法。
    - factor_name: 要分析的因素名称（如'oper_old', 'device_cmf7', 'grade'）。
    - searchDate: 查询日期。
    - custom_logger: 自定义日志记录器，用于输出日志信息。
    - DB: 目标数据库
    """
    if DB == 'db_yielddetail':
        in_field = 'in_qty'
        out_field = 'out_qty'
        condition_field = 'workdt'
    data, chi2_stat, p_val, dof, expected = (
        a.getOneFactorData(DB, factor_name, in_field, out_field, condition_field, searchDate))
    judge = ''
    if float(p_val) <= 0.05:
        judge = '*'
    custom_logger.log_info(f'{factor_name}别P值：{p_val}{judge}')
    custom_logger.log_info(f'{factor_name}别数据：{data}')
    custom_logger.log_info(f'{factor_name}别期望：{expected}')


def main():
    log_path = r'C:\Users\Tengjun Zhao\Desktop\test'
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
        issue_info['watch_id'] = issue[1]
        issue_info['spec_desc'] = issue[9]
        issue_info['observe'] = issue[2]
        issue_info['db_name'] = issue[6]
        issue_info['indicator'] = issue[7]
        issue_info['time'] = issue[0].strftime("%Y-%m-%d %H:%M:%S")
        issue_info['remark'] = issue[10]
        # 提取Issue的事项
        pattern = r"\((.*?)\)"
        matches = re.findall(pattern, issue_info['observe'])
        extracted_data = [match.split(", ")[0:2] for match in matches]
        item = extracted_data[0][0].replace("-", "")
        value = extracted_data[0][1]
        # 创建Log
        log_name = f"{issue[0].strftime('%Y%m%d%H%M%S')}_{issue_info['remark']}_{item}_{value}.log"
        log_dir = os.path.join(log_path, log_name)
        custom_logger = CustomLogger(log_dir)
        # custom_logger.clear_logs()
        # issue基本信息
        custom_logger.log_header('info')
        custom_logger.log_info(issue_info['time'])
        custom_logger.log_info(issue_info['remark'])
        custom_logger.log_info('观测值：' + issue_info['observe'])
        a = Data(issue_info, local_host, ali_host, custom_logger)
        # 单独Lot Issue分析
        if a.get_analysis_category() == 1:
            a.single(issue_info['observe'])
        # 统计指数异常分析
        elif a.get_analysis_category() == 2:
            observe_str = re.sub(r'(\d{4}-\d{2}-\d{2})', r"'\1'", issue_info['observe'])
            observe_list = ast.literal_eval(observe_str)
            searchDate = str(observe_list[-1][0]).replace('-', '')
            print(searchDate)
            # ET Retest
            if issue_info['watch_id'] == 8:
                pass
            # Daily Yield
            elif issue_info['watch_id'] == 9:
                # 以下是使用定义好的函数的示例
                factor_names = ['oper_old', 'device_cmf7', 'grade', 'Device']
                for factor_name in factor_names:
                    log_factor_analysis(a, factor_name, searchDate, custom_logger, 'db_yielddetail')


if __name__ == '__main__':
    main()
    # os.system('pause')
