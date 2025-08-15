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
        devicePropertyList = ['Product_Mode', 'Die_Density', 'Product_Density']
        lotPropertyList = ['grade']
        pcType = ['SD', 'UD']
        svType = ['RD', 'LD']
        # Step1：获取属性列表
        logger.info("PC向列表")
        additional_conditions_pc = [
            ('dd', 'Module_Type', 'IN', pcType),  # dd.Module_Type IN ('SD', 'UD')
            ('dy', 'oper_old', 'IN', operList)
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
            ('dy', 'oper_old', 'IN', operList)
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

        if pc_products:
            first_pc_product = pc_products[0]
            print(f"产品: {first_pc_product}")
            print("月度数据透视表:")
            print(first_pc_product.get_pivoted_monthly_data())

    except Exception as e:
        logger.critical(f"程序主流程出错: {str(e)}", exc_info=True)


if __name__ == '__main__':
    main('test')