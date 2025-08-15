from datetime import datetime, timedelta
import dateutil.relativedelta
import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging
from logging.handlers import RotatingFileHandler
import numpy as np
from scipy import stats
# 引用fail_status_analysis的logger方法
from fail_status_analysis import setup_logger

from datetime import datetime


class ProductData:
    """产品数据类，用于存储特定产品的属性及相关生产数据"""

    def __init__(self, product_properties):
        self.properties = product_properties
        self.annual_data = {}  # {年份: {oper: {'in': int, 'out': int, 'fail': float}}}
        self.monthly_data = {}  # {年月: {oper: {'in': int, 'out': int, 'fail': float}}}

    def add_annual_oper_data(self, year, oper, in_qty, out_qty):
        """添加年度操作数据"""
        if year not in self.annual_data:
            self.annual_data[year] = {}
        fail_rate = round((in_qty - out_qty) / in_qty * 100, 2) if in_qty > 0 else 0.0
        self.annual_data[year][oper] = {
            'in': in_qty,
            'out': out_qty,
            'fail': fail_rate
        }

    def add_monthly_oper_data(self, year_month, oper, in_qty, out_qty):
        """添加月度操作数据"""
        if year_month not in self.monthly_data:
            self.monthly_data[year_month] = {}
        fail_rate = round((in_qty - out_qty) / in_qty * 100, 2) if in_qty > 0 else 0.0
        self.monthly_data[year_month][oper] = {
            'in': in_qty,
            'out': out_qty,
            'fail': fail_rate
        }

    def get_pivoted_annual_data(self):
        """将年度数据转换为透视表格式"""
        return self._pivot_data(self.annual_data)

    def get_pivoted_monthly_data(self):
        """将月度数据转换为透视表格式"""
        return self._pivot_data(self.monthly_data)

    def _pivot_data(self, data):
        """将数据转换为透视表格式"""
        rows = []
        for period, oper_data in data.items():
            row = {'period': period}
            for oper, values in oper_data.items():
                row[f'{oper}_in'] = values['in']
                row[f'{oper}_out'] = values['out']
                row[f'{oper}_fail'] = values['fail']
            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df = df.set_index('period')
        return df.sort_index()

    def calculate_annual_fail_stats(self):
        """计算年度fail数据统计：et fail、at fail、ttl fail"""
        return self._calculate_fail_stats(self.annual_data)

    def calculate_monthly_fail_stats(self):
        """计算月度fail数据统计：et fail、at fail、ttl fail"""
        return self._calculate_fail_stats(self.monthly_data)

    def _calculate_fail_stats(self, data):
        """
        通用fail数据计算与存储方法
        et fail: 5600工序的fail值
        at fail: 5710、5700、5780工序的fail总和
        ttl fail: 上述所有工序的fail总和（仅包含有数据的工序）
        """
        stats = []
        et_oper = '5600'
        at_opers = ['5710', '5700', '5780']
        all_opers = [et_oper] + at_opers

        for period, oper_data in data.items():
            # 计算et fail（仅5600工序）
            et_fail = oper_data[et_oper]['fail'] if et_oper in oper_data else None

            # 计算at fail（5710、5700、5780工序总和）
            at_sum = 0.0
            at_count = 0
            for oper in at_opers:
                if oper in oper_data:
                    at_sum += oper_data[oper]['fail']
                    at_count += 1
            at_fail = round(at_sum, 2) if at_count > 0 else None

            # 计算ttl fail（所有指定工序总和）
            ttl_sum = 0.0
            ttl_count = 0
            for oper in all_opers:
                if oper in oper_data:
                    ttl_sum += oper_data[oper]['fail']
                    ttl_count += 1
            ttl_fail = round(ttl_sum, 2) if ttl_count > 0 else None

            # 存储统计结果到数据字典
            fail_stats = {
                'et_fail': et_fail,
                'at_fail': at_fail,
                'ttl_fail': ttl_fail
            }
            oper_data['fail_stats'] = fail_stats  # 添加到当前周期的数据中

            stats.append({
                'period': period,
                'et_fail': et_fail,
                'at_fail': at_fail,
                'ttl_fail': ttl_fail
            })

        # 转换为DataFrame并排序
        if not stats:
            return pd.DataFrame(columns=['period', 'et_fail', 'at_fail', 'ttl_fail'])

        df = pd.DataFrame(stats)
        return df.set_index('period').sort_index()

    def get_product_display_name(self):
        """获取产品显示名称（仅属性值拼接，解决未定义问题）"""
        # 将所有属性值转换为字符串并拼接（如："DDR4 8G NQ"）
        return " ".join(str(v) for v in self.properties.values())

    def get_5600_in_qty(self, period, is_annual=True):
        """获取指定周期5600工序的in数量（空值返回0）"""
        data = self.annual_data if is_annual else self.monthly_data
        oper_data = data.get(period, {})
        return oper_data.get('5600', {}).get('in', 0)

    def __str__(self):
        prop_str = ", ".join([f"{k}={v}" for k, v in self.properties.items()])
        return f"ProductData[{prop_str}]"


# 实例化产品
def create_product_instances(property_df, device_property_list, lot_property_list):
    """
    根据属性DataFrame创建ProductData实例列表

    参数:
        property_df: 包含产品属性的DataFrame
        device_property_list: 设备属性列表
        lot_property_list: 批次属性列表

    返回:
        list: ProductData实例列表
    """
    product_instances = []

    for _, row in property_df.iterrows():
        # 提取设备属性
        device_props = {prop: row[prop] for prop in device_property_list if prop in row}

        # 提取批次属性
        lot_props = {prop: row[prop] for prop in lot_property_list if prop in row}

        # 合并属性并创建实例
        product_properties = {**device_props, **lot_props}
        product_instance = ProductData(product_properties)
        product_instances.append(product_instance)

    return product_instances


# 获取操作月的所有投产Device的属性列表
def get_property_list(db_config, start, end, devicePropertyList, lotPropertyList,
                      additional_conditions=None, logger=None):
    """
    从数据库中查询所需的属性列表，支持额外的WHERE条件限制

    参数:
        db_config: 数据库配置字典
        start: 开始日期，格式如'20250701'
        end: 结束日期，格式如'20250731'
        operList: 操作列表，如['5600', '5710']
        devicePropertyList: 设备属性列表，如['Product_Mode', 'Die_Density']
        lotPropertyList: 批次属性列表，如['grade']
        additional_conditions: 额外的查询条件列表，每个条件为元组
                              格式: [(表别名, 字段名, 操作符, 值列表), ...]
                              例如: [('dd', 'Module_Type', 'IN', ['SD', 'UD'])]
        logger: 日志记录器

    返回:
        pandas.DataFrame: 查询结果数据框，若出错则返回None
    """
    try:
        # 构建查询字段
        device_fields = [f"dd.{field}" for field in devicePropertyList]
        lot_fields = [f"dy.{field}" for field in lotPropertyList]
        select_fields = ", ".join(device_fields + lot_fields)
        # 构建WHERE子句条件
        where_clauses = [
            "dy.workdt BETWEEN :start_dt AND :end_dt"
        ]
        # 准备参数字典
        params = {'start_dt': start, 'end_dt': end}

        # 处理额外的查询条件
        condition_counter = 0
        if additional_conditions:
            for table_alias, field, operator, values in additional_conditions:
                # 确保操作符是支持的类型
                if operator.upper() not in ['IN', '=', '!=', '>', '<', '>=', '<=']:
                    raise ValueError(f"不支持的操作符: {operator}")

                # 为IN操作符创建多个占位符
                if operator.upper() == 'IN':
                    placeholders = ", ".join([f":cond_{condition_counter}_{i}" for i in range(len(values))])
                    where_clauses.append(f"{table_alias}.{field} {operator} ({placeholders})")

                    # 添加参数
                    for i, value in enumerate(values):
                        params[f'cond_{condition_counter}_{i}'] = value
                else:
                    # 处理其他操作符 (=, !=, >, < 等)
                    placeholder = f":cond_{condition_counter}"
                    where_clauses.append(f"{table_alias}.{field} {operator} {placeholder}")
                    params[placeholder[1:]] = values  # 移除占位符中的冒号

                condition_counter += 1

        # 构建完整的SQL查询
        sql = f"""
            SELECT {select_fields}
            FROM cmsalpha.db_yielddetail dy
            JOIN modulemte.db_deviceinfo dd ON dy.device = dd.Device
            WHERE {' AND '.join(where_clauses)}
            GROUP BY {select_fields}
            ORDER BY {select_fields} ASC
        """

        # 创建数据库连接
        conn_str = (f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                    f"{db_config['host']}:{db_config['port']}/?charset={db_config['charset']}")
        engine = create_engine(conn_str)

        # 执行查询
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if logger:
            logger.info(f"查询成功，返回{len(df)}条记录")
            if not df.empty:
                logger.info(f"查询结果:\n{df.to_string()}")
        return df

    except Exception as e:
        if logger:
            logger.error(f"查询属性列表失败: {str(e)}", exc_info=True)
        else:
            print(f"查询属性列表失败: {str(e)}")
        return None


def get_product_production_data(db_config, product, additional_conditions, time_type, device_property_list, logger=None):
    try:
        # 确定时间范围和格式
        if time_type == 'annual':
            # 前年、去年、今年
            last_month_last_day = datetime.now().replace(day=1) - timedelta(days=1)
            current_year = datetime.now().year
            years = [current_year - 2, current_year - 1, current_year]
            start_date = f"{years[0]}0101"
            end_date = last_month_last_day.strftime('%Y%m%d')
            # end_date = f"{years[-1]}1231"
            date_format = "SUBSTRING(dy.workdt, 1, 4)"  # 提取年份
            group_by = "dt, oper_old"
        else:  # monthly
            # 最近12个月，包含lastMonthLastDay所在月
            last_month_last_day = datetime.now().replace(day=1) - timedelta(days=1)
            start_date = (last_month_last_day - dateutil.relativedelta.relativedelta(months=11)).strftime('%Y%m%d')
            end_date = last_month_last_day.strftime('%Y%m%d')
            date_format = "SUBSTRING(dy.workdt, 1, 6)"  # 提取年月
            group_by = "dt, oper_old"

        # # 构建oper_list的参数占位符（如:oper_0, :oper_1, ...）
        # oper_placeholders = [f":oper_{i}" for i in range(len(oper_list))]

        # 构建产品属性条件
        where_conditions = [
            f"dy.workdt BETWEEN :start_date AND :end_date",  # 日期参数化
        ]
        # 准备所有参数（关键步骤：将所有占位符与值绑定）
        params = {
            "start_date": start_date,  # 绑定日期参数
            "end_date": end_date
        }
        # 添加产品属性过滤条件
        for prop_name, prop_value in product.properties.items():
            if prop_name in device_property_list:
                # 设备属性条件参数化（新增参数占位符）
                prop_placeholder = f":dd_{prop_name}"
                where_conditions.append(f"dd.{prop_name} = {prop_placeholder}")
            else:
                # 批次属性条件参数化（新增参数占位符）
                prop_placeholder = f":dy_{prop_name}"
                where_conditions.append(f"dy.{prop_name} = {prop_placeholder}")

        condition_counter = 0
        if additional_conditions:
            for table_alias, field, operator, values in additional_conditions:
                if operator.upper() not in ['IN', '=', '!=', '>', '<', '>=', '<=']:
                    raise ValueError(f"不支持的操作符: {operator}")

                if operator.upper() == 'IN':
                    # 处理IN条件（如dy.oper_old IN ('5600', '5710')）
                    placeholders = ", ".join([f":cond_{condition_counter}_{i}" for i in range(len(values))])
                    where_conditions.append(f"{table_alias}.{field} {operator} ({placeholders})")
                    # 绑定参数
                    for i, value in enumerate(values):
                        params[f'cond_{condition_counter}_{i}'] = value
                else:
                    # 处理其他条件（=, !=等）
                    placeholder = f":cond_{condition_counter}"
                    where_conditions.append(f"{table_alias}.{field} {operator} {placeholder}")
                    params[placeholder[1:]] = values  # 移除占位符中的冒号
                condition_counter += 1
        # 构建SQL查询
        sql = f"""
            SELECT 
                {date_format} as dt, 
                dy.oper_old, 
                SUM(in_qty) as in_qty, 
                SUM(out_qty) as out_qty
            FROM cmsalpha.db_yielddetail dy
            JOIN modulemte.db_deviceinfo dd ON dy.device = dd.Device
            WHERE {' AND '.join(where_conditions)}
            GROUP BY {group_by}
            ORDER BY dt ASC
        """

        # if logger:
        #     logger.debug(f"获取产品数据的SQL: {sql}")

        # 绑定产品属性参数（:dd_xxx 或 :dy_xxx）
        for prop_name, prop_value in product.properties.items():
            if prop_name in device_property_list:
                params[f"dd_{prop_name}"] = prop_value
            else:
                params[f"dy_{prop_name}"] = prop_value

        # 创建数据库连接
        conn_str = (f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                    f"{db_config['host']}:{db_config['port']}/?charset={db_config['charset']}")
        engine = create_engine(conn_str)

        # 执行查询并传入参数（关键：第二个参数是params）
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)  # 此处传入参数
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # 计算故障率
        if not df.empty:
            # 先计算原始值，再显式四舍五入并转换为float（确保2位小数精度）
            df['fail'] = ((df['in_qty'] - df['out_qty']) / df['in_qty'] * 100).round(2).astype(float)
            # 填充NaN为0.0（同样确保2位小数）
            df['fail'] = df['fail'].fillna(0.0).round(2)

        if logger:
            logger.debug(f"产品 {product} 的{time_type}数据: {len(df)}条记录")
            logger.debug(f"产品 {product} 的{time_type}数据:\n{df.to_string()}")
        return df

    except Exception as e:
        if logger:
            logger.error(f"获取产品 {product} 的{time_type}数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()


def populate_product_data(db_config, product_instances, additional_conditions, device_property_list, logger=None):
    """为所有产品实例填充生产数据"""
    for i, product in enumerate(product_instances):
        if logger:
            logger.info(f"处理产品 {i + 1}/{len(product_instances)}: {product}")

        # 获取年度数据时传入device_property_list
        annual_df = get_product_production_data(
            db_config, product, additional_conditions, 'annual', device_property_list, logger  # 新增参数
        )

        # 填充年度数据到产品实例（不变）
        if not annual_df.empty:
            for _, row in annual_df.iterrows():
                product.add_annual_oper_data(
                    int(row['dt']),  # 年份
                    row['oper_old'],
                    int(row['in_qty']),
                    int(row['out_qty'])
                )

        # 获取月度数据时传入device_property_list
        monthly_df = get_product_production_data(
            db_config, product, additional_conditions, 'monthly', device_property_list, logger  # 新增参数
        )

        # 填充月度数据到产品实例（不变）
        if not monthly_df.empty:
            for _, row in monthly_df.iterrows():
                product.add_monthly_oper_data(
                    row['dt'],  # 年月格式如'202501'
                    row['oper_old'],
                    int(row['in_qty']),
                    int(row['out_qty'])
                )


def process_all_products(db_config, pc_property_df, sv_property_df,
                         device_property_list, lot_property_list,
                         oper_list, additional_conditions_pc, additional_conditions_sv, logger=None):
    """处理所有PC和Server产品"""
    # 创建产品实例
    if logger:
        logger.info("创建PC产品实例...")
    pc_products = create_product_instances(
        pc_property_df, device_property_list, lot_property_list
    )

    if logger:
        logger.info("创建Server产品实例...")
    sv_products = create_product_instances(
        sv_property_df, device_property_list, lot_property_list
    )

    # 填充生产数据
    if logger:
        logger.info("填充PC产品数据...")
    populate_product_data(db_config, pc_products, additional_conditions_pc, device_property_list, logger)

    if logger:
        logger.info("填充Server产品数据...")
    populate_product_data(db_config, sv_products, additional_conditions_sv, device_property_list, logger)

    return pc_products, sv_products


def log_fail_tables(product_list, product_type, logger):
    """输出产品不良数据表格日志"""
    if not product_list:
        logger.info(f"没有{product_type}产品数据可输出")
        return

    # 获取需要展示的年度和月度周期
    current_year = datetime.now().year
    annual_periods = [current_year - 2, current_year - 1, current_year]
    annual_periods_str = [str(y) for y in annual_periods]

    last_month = datetime.now().replace(day=1) - timedelta(days=1)
    monthly_periods = [(last_month - dateutil.relativedelta.relativedelta(months=i)).strftime('%Y%m')
                       for i in range(11, -1, -1)]

    # 定义三种不良类型
    fail_types = [
        ('et_fail', f'{product_type} ET不良数据'),
        ('at_fail', f'{product_type} AT不良数据'),
        ('ttl_fail', f'{product_type} TTL不良数据')
    ]

    for fail_type, title in fail_types:
        logger.info(f"\n===== {title} =====")

        # 构建表格数据
        table_data = []
        headers = ["Product"] + annual_periods_str + monthly_periods
        table_data.append(headers)

        # 收集所有行数据
        for product in product_list:
            row = [product.get_product_display_name()]

            # 添加年度数据
            for year in annual_periods:
                year_data = product.annual_data.get(year, {})
                fail_val = year_data.get('fail_stats', {}).get(fail_type)
                row.append(f"{fail_val:.2f}" if fail_val is not None else "")

            # 添加月度数据
            for month in monthly_periods:
                month_data = product.monthly_data.get(month, {})
                fail_val = month_data.get('fail_stats', {}).get(fail_type)
                row.append(f"{fail_val:.2f}" if fail_val is not None else "")

            table_data.append(row)

        # 计算每列最大宽度
        col_widths = [max(len(str(row[i])) for row in table_data) for i in range(len(headers))]

        # 输出表头
        header_row = " | ".join([f"{str(h).ljust(col_widths[i])}" for i, h in enumerate(headers)])
        logger.info(header_row)

        # 输出分隔线
        separator = "-|-".join(["-" * w for w in col_widths])
        logger.info(separator)

        # 输出数据行
        for row in table_data[1:]:
            data_row = " | ".join([f"{str(cell).ljust(col_widths[i])}" for i, cell in enumerate(row)])
            logger.info(data_row)

        logger.info(f"===== {title} 结束 =====")


def log_qty_table(product_list, product_type, logger):
    """新增：输出5600工序in数量表（以5600 in为基准，空值用0）"""
    if not product_list:
        logger.info(f"没有{product_type}产品的QTY数据可输出")
        return

    # 周期定义（与不良表一致）
    current_year = datetime.now().year
    annual_periods = [current_year - 2, current_year - 1, current_year]
    annual_periods_str = [str(y) for y in annual_periods]

    last_month = datetime.now().replace(day=1) - timedelta(days=1)
    monthly_periods = [(last_month - dateutil.relativedelta.relativedelta(months=i)).strftime('%Y%m')
                       for i in range(11, -1, -1)]

    logger.info(f"\n===== {product_type} QTY数据（5600工序in数量） =====")

    # 表格数据（空值用0）
    table_data = []
    headers = ["Product"] + annual_periods_str + monthly_periods
    table_data.append(headers)

    for product in product_list:
        row = [product.get_product_display_name()]

        # 年度5600 in数量（空值→0）
        for year in annual_periods:
            in_qty = product.get_5600_in_qty(year, is_annual=True)
            row.append(str(in_qty))  # 整数格式

        # 月度5600 in数量（空值→0）
        for month in monthly_periods:
            in_qty = product.get_5600_in_qty(month, is_annual=False)
            row.append(str(in_qty))

        table_data.append(row)

    # 格式化输出
    col_widths = [max(len(str(row[i])) for row in table_data) for i in range(len(headers))]
    header_row = " | ".join([f"{h.ljust(col_widths[i])}" for i, h in enumerate(headers)])
    logger.info(header_row)
    logger.info("-|-".join(["-" * w for w in col_widths]))
    for row in table_data[1:]:
        data_row = " | ".join([f"{cell.ljust(col_widths[i])}" for i, cell in enumerate(row)])
        logger.info(data_row)

    logger.info(f"===== {product_type} QTY数据结束 =====")


def main(mode):
    # 初始化日志记录器
    log_name = datetime.today().strftime('%Y%m%d')
    try:
        # 数据库配置
        db_config = {
            'host': None,
            'user': 'remoteuser',
            'password': 'password',
            'database': 'cmsalpha',
            'charset': 'utf8mb4',
            'port': 3306,
        }

        # 根据模式选择数据库主机
        if mode == 'test':
            db_config['host'] = 'localhost'
            log_path = "C:/Users/Tengjun Zhao/Desktop"
            logger = setup_logger(
                log_path=log_path,
                log_name=log_name,
                file_level=logging.DEBUG,
                console_level=logging.DEBUG
            )
            logger.info("使用测试环境数据库(localhost)")
        else:
            db_config['host'] = '172.27.154.57'
            log_path = "C:/Users/Tengjun Zhao/Desktop"
            logger = setup_logger(
                log_path=log_path,
                log_name=log_name,
                file_level=logging.INFO,
                console_level=logging.INFO
            )
            logger.info("使用生产环境数据库(172.27.154.57)")
        logger.info("程序开始运行")

        # 获取上个月的最后一天
        lastMonthLastDay = datetime.now().replace(day=1) - timedelta(days=1)
        str_endWorkdt = lastMonthLastDay.strftime('%Y%m%d')
        lastMonthFirstDay = lastMonthLastDay.replace(day=1)
        str_startWorkdt = lastMonthFirstDay.strftime('%Y%m%d')
        # 当年的1月1日
        startWorkdt = datetime.now().replace(month=1, day=1)
        str_thisYearWorkdt = startWorkdt.strftime('%Y%m%d')
        # 3年前的1月1日
        thrWorkdt = startWorkdt - dateutil.relativedelta.relativedelta(years=3)
        str_thrWorkdt = thrWorkdt.strftime('%Y%m%d')

        # 准备作业参数
        operList = ['5600', '5710', '5700', '5780']
        devicePropertyList = ['Product_Mode', 'Product_Density']
        lotPropertyList = ['grade']
        pcType = ['SD', 'UD']
        svType = ['RD', 'LD']
        # Step1：获取属性列表
        logger.info("PC向列表")
        additional_conditions_pc = [
            ('dd', 'Module_Type', 'IN', pcType),  # dd.Module_Type IN ('SD', 'UD')
            ('dy', 'oper_old', 'IN', operList),
            ('dd', 'Product_Mode', '=', 'DDR4 DIMM'),
        ]
        pcPropertyList = get_property_list(
            db_config=db_config,
            start=str_startWorkdt,
            end=str_endWorkdt,
            devicePropertyList=devicePropertyList,
            lotPropertyList=lotPropertyList,
            additional_conditions=additional_conditions_pc,
            logger=logger
        )
        logger.info("Server向列表")
        additional_conditions_sv = [
            ('dd', 'Module_Type', 'IN', svType),  # dd.Module_Type IN ('RD', 'LD')
            ('dy', 'oper_old', 'IN', operList),
            ('dd', 'Product_Mode', '=', 'DDR4 DIMM'),
        ]
        svPropertyList = get_property_list(
            db_config=db_config,
            start=str_startWorkdt,
            end=str_endWorkdt,
            devicePropertyList=devicePropertyList,
            lotPropertyList=lotPropertyList,
            additional_conditions=additional_conditions_sv,
            logger=logger
        )

        pc_products, sv_products = process_all_products(
            db_config=db_config,
            pc_property_df=pcPropertyList,
            sv_property_df=svPropertyList,
            device_property_list=devicePropertyList,
            lot_property_list=lotPropertyList,
            oper_list=operList,
            additional_conditions_pc=additional_conditions_pc,
            additional_conditions_sv=additional_conditions_sv,
            logger=logger
        )

        for product in pc_products:
            annual_stats_df = product.calculate_annual_fail_stats()
            monthly_stats_df = product.calculate_monthly_fail_stats()

        for product in sv_products:
            annual_stats_df = product.calculate_annual_fail_stats()
            monthly_stats_df = product.calculate_monthly_fail_stats()

        # 输出日志表格（包含新增的qty表）
        log_fail_tables(pc_products, "PC", logger)
        log_fail_tables(sv_products, "SV", logger)
        log_qty_table(pc_products, "PC", logger)  # 新增QTY表
        log_qty_table(sv_products, "SV", logger)  # 新增QTY表

    except Exception as e:
        logger.critical(f"程序主流程出错: {str(e)}", exc_info=True)


if __name__ == '__main__':
    main('test')