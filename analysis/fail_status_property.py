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
        self.properties = product_properties  # 允许空字典
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
        """将数据转换为透视表格式（支持空数据）"""
        rows = []
        for period, oper_data in data.items():
            row = {'period': period}
            for oper, values in oper_data.items():
                row[f'{oper}_in'] = values['in']
                row[f'{oper}_out'] = values['out']
                row[f'{oper}_fail'] = values['fail']
            rows.append(row)

        return pd.DataFrame(rows).set_index('period').sort_index() if rows else pd.DataFrame()

    def calculate_annual_fail_stats(self):
        """计算年度fail数据统计：et fail、at fail、ttl fail"""
        return self._calculate_fail_stats(self.annual_data)

    def calculate_monthly_fail_stats(self):
        """计算月度fail数据统计：et fail、at fail、ttl fail"""
        return self._calculate_fail_stats(self.monthly_data)

    def _calculate_fail_stats(self, data):
        """通用fail数据计算（支持空数据）"""
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
            oper_data['fail_stats'] = {
                'et_fail': et_fail,
                'at_fail': at_fail,
                'ttl_fail': ttl_fail
            }
            stats.append({
                'period': period,
                'et_fail': et_fail,
                'at_fail': at_fail,
                'ttl_fail': ttl_fail
            })

        # 支持空统计结果
        return pd.DataFrame(stats).set_index('period').sort_index() if stats else \
            pd.DataFrame(columns=['period', 'et_fail', 'at_fail', 'ttl_fail'])

    def get_product_display_name(self):
        """获取产品显示名称（支持空属性，返回空字符串）"""
        return " ".join(str(v) for v in self.properties.values()) if self.properties else ""

    def get_5600_in_qty(self, period, is_annual=True):
        """获取指定周期5600工序的in数量（空值返回0）"""
        data = self.annual_data if is_annual else self.monthly_data
        oper_data = data.get(period, {})
        return oper_data.get('5600', {}).get('in', 0)

    def __str__(self):
        """支持空属性的字符串表示"""
        prop_str = ", ".join([f"{k}={v}" for k, v in self.properties.items()]) if self.properties else "无属性"
        return f"ProductData[{prop_str}]"


# 实例化产品（支持空属性列表）
def create_product_instances(property_df, device_property_list, lot_property_list, logger=None):
    """
    根据属性DataFrame创建ProductData实例列表

    参数:
        property_df: 包含产品属性的DataFrame
        device_property_list: 设备属性列表（允许空列表/None）
        lot_property_list: 批次属性列表（允许空列表/None）
        logger: 日志记录器（可选）

    返回:
        list: ProductData实例列表
    """
    # 强制转为列表（避免None导致的遍历错误）
    device_property_list = device_property_list if isinstance(device_property_list, list) else []
    lot_property_list = lot_property_list if isinstance(lot_property_list, list) else []

    product_instances = []
    # 记录空列表状态（便于调试）
    if not device_property_list and logger:
        logger.info("device_property_list为空，不提取设备属性")
    if not lot_property_list and logger:
        logger.info("lot_property_list为空，不提取批次属性")

    # 处理空DataFrame（避免遍历错误）
    if property_df.empty:
        if logger:
            logger.warning("property_df为空，不创建任何ProductData实例")
        return product_instances

    for _, row in property_df.iterrows():
        # 提取属性（空列表时返回空字典，安全无报错）
        device_props = {prop: row[prop] for prop in device_property_list if prop in row}
        lot_props = {prop: row[prop] for prop in lot_property_list if prop in row}

        # 合并属性（允许空字典）
        product_properties = {**device_props, **lot_props}
        product_instance = ProductData(product_properties)
        product_instances.append(product_instance)

    if logger:
        logger.info(f"成功创建{len(product_instances)}个ProductData实例")
    return product_instances


# 获取属性列表（核心修改：处理空属性列表的SQL语法）
def get_property_list(db_config, start, end, devicePropertyList, lotPropertyList,
                      additional_conditions=None, logger=None):
    """
    从数据库中查询所需的属性列表（支持空设备/批次属性列表）

    参数:
        db_config: 数据库配置字典
        start: 开始日期，格式如'20250701'
        end: 结束日期，格式如'20250731'
        devicePropertyList: 设备属性列表（允许空列表/None）
        lotPropertyList: 批次属性列表（允许空列表/None）
        additional_conditions: 额外查询条件（可选）
        logger: 日志记录器（可选）

    返回:
        pandas.DataFrame: 查询结果数据框，若出错则返回None
    """
    try:
        # 强制转为列表（避免None）
        devicePropertyList = devicePropertyList if isinstance(devicePropertyList, list) else []
        lotPropertyList = lotPropertyList if isinstance(lotPropertyList, list) else []

        # 构建查询字段（处理空列表：若均为空，用默认字段避免SQL错误）
        device_fields = [f"dd.{field}" for field in devicePropertyList] if devicePropertyList else []
        lot_fields = [f"dy.{field}" for field in lotPropertyList] if lotPropertyList else []
        all_fields = device_fields + lot_fields

        # 关键修复：若所有字段为空，设置默认字段（dy.device存在于JOIN后的表中）
        if not all_fields:
            if logger:
                logger.warning("devicePropertyList和lotPropertyList均为空，使用默认字段'dy.device'查询")
            all_fields = ["dy.device"]  # 默认字段确保SQL语法正确
        select_fields = ", ".join(all_fields)

        # 构建WHERE子句条件
        where_clauses = [f"dy.workdt BETWEEN :start_dt AND :end_dt"]
        params = {'start_dt': start, 'end_dt': end}

        # 处理额外条件（原逻辑保留，支持空条件）
        condition_counter = 0
        if additional_conditions and isinstance(additional_conditions, list):
            for table_alias, field, operator, values in additional_conditions:
                if operator.upper() not in ['IN', '=', '!=', '>', '<', '>=', '<=']:
                    raise ValueError(f"不支持的操作符: {operator}")

                if operator.upper() == 'IN':
                    placeholders = ", ".join([f":cond_{condition_counter}_{i}" for i in range(len(values))])
                    where_clauses.append(f"{table_alias}.{field} {operator} ({placeholders})")
                    for i, value in enumerate(values):
                        params[f'cond_{condition_counter}_{i}'] = value
                else:
                    placeholder = f":cond_{condition_counter}"
                    where_clauses.append(f"{table_alias}.{field} {operator} {placeholder}")
                    params[placeholder[1:]] = values
                condition_counter += 1

        # 构建SQL（GROUP BY与SELECT字段一致，避免语法错误）
        sql = f"""
            SELECT {select_fields}
            FROM cmsalpha.db_yielddetail dy
            JOIN modulemte.db_deviceinfo dd ON dy.device = dd.Device
            WHERE {' AND '.join(where_clauses)}
            GROUP BY {select_fields}
            ORDER BY {select_fields} ASC
        """

        # 执行查询
        conn_str = (f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                    f"{db_config['host']}:{db_config['port']}/?charset={db_config['charset']}")
        engine = create_engine(conn_str)

        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if logger:
            logger.info(f"属性列表查询成功，返回{len(df)}条记录")
            if not df.empty and logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"查询结果:\n{df.to_string()}")
        return df

    except Exception as e:
        if logger:
            logger.error(f"查询属性列表失败: {str(e)}", exc_info=True)
        else:
            print(f"查询属性列表失败: {str(e)}")
        return None


# 获取产品生产数据（支持空设备属性列表）
def get_product_production_data(db_config, product, additional_conditions, time_type, device_property_list, logger=None):
    try:
        # 强制转为列表（避免None）
        device_property_list = device_property_list if isinstance(device_property_list, list) else []
        # 记录空列表状态
        if not device_property_list and logger:
            logger.debug(f"产品 {product} 的device_property_list为空，所有属性按批次属性处理")

        # 时间范围逻辑（原逻辑保留）
        if time_type == 'annual':
            last_month_last_day = datetime.now().replace(day=1) - timedelta(days=1)
            current_year = datetime.now().year
            years = [current_year - 2, current_year - 1, current_year]
            start_date = f"{years[0]}0101"
            end_date = last_month_last_day.strftime('%Y%m%d')
            date_format = "SUBSTRING(dy.workdt, 1, 4)"
            group_by = "dt, oper_old"
        else:  # monthly
            last_month_last_day = datetime.now().replace(day=1) - timedelta(days=1)
            start_date = (last_month_last_day - dateutil.relativedelta.relativedelta(months=11)).strftime('%Y%m%d')
            end_date = last_month_last_day.strftime('%Y%m%d')
            date_format = "SUBSTRING(dy.workdt, 1, 6)"
            group_by = "dt, oper_old"

        # 构建WHERE条件（支持空产品属性）
        where_conditions = [f"dy.workdt BETWEEN :start_date AND :end_date"]
        params = {"start_date": start_date, "end_date": end_date}

        # 处理空产品属性（不添加属性过滤条件）
        if not product.properties:
            if logger:
                logger.debug(f"产品 {product} 的properties为空，不添加属性过滤条件")
        else:
            # 添加属性过滤（空device列表时，所有属性按批次处理）
            for prop_name, prop_value in product.properties.items():
                if prop_name in device_property_list:
                    prop_placeholder = f":dd_{prop_name}"
                    where_conditions.append(f"dd.{prop_name} = {prop_placeholder}")
                else:
                    prop_placeholder = f":dy_{prop_name}"
                    where_conditions.append(f"dy.{prop_name} = {prop_placeholder}")
                params[prop_placeholder[1:]] = prop_value  # 绑定参数（统一处理，避免遗漏）

        # 处理额外条件（原逻辑保留）
        condition_counter = 0
        if additional_conditions and isinstance(additional_conditions, list):
            for table_alias, field, operator, values in additional_conditions:
                if operator.upper() not in ['IN', '=', '!=', '>', '<', '>=', '<=']:
                    raise ValueError(f"不支持的操作符: {operator}")

                if operator.upper() == 'IN':
                    placeholders = ", ".join([f":cond_{condition_counter}_{i}" for i in range(len(values))])
                    where_conditions.append(f"{table_alias}.{field} {operator} ({placeholders})")
                    for i, value in enumerate(values):
                        params[f'cond_{condition_counter}_{i}'] = value
                else:
                    placeholder = f":cond_{condition_counter}"
                    where_conditions.append(f"{table_alias}.{field} {operator} {placeholder}")
                    params[placeholder[1:]] = values
                condition_counter += 1

        # 构建SQL
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

        # 执行查询
        conn_str = (f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                    f"{db_config['host']}:{db_config['port']}/?charset={db_config['charset']}")
        engine = create_engine(conn_str)

        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        # 计算故障率（支持空DataFrame）
        if not df.empty:
            df['fail'] = ((df['in_qty'] - df['out_qty']) / df['in_qty'] * 100).round(2).astype(float)
            df['fail'] = df['fail'].fillna(0.0).round(2)

        if logger and logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"产品 {product} 的{time_type}数据: {len(df)}条记录")
            if not df.empty:
                logger.debug(f"生产数据:\n{df.to_string()}")
        return df

    except Exception as e:
        if logger:
            logger.error(f"获取产品 {product} 的{time_type}数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()


# 填充产品数据（支持空属性列表）
def populate_product_data(db_config, product_instances, additional_conditions, device_property_list, logger=None):
    """为所有产品实例填充生产数据（支持空设备属性列表）"""
    # 强制转为列表（避免None）
    device_property_list = device_property_list if isinstance(device_property_list, list) else []

    if not product_instances:
        if logger:
            logger.warning("product_instances为空，不填充任何生产数据")
        return

    for i, product in enumerate(product_instances, 1):
        if logger:
            logger.info(f"处理产品 {i}/{len(product_instances)}: {product}")

        # 填充年度数据
        annual_df = get_product_production_data(
            db_config, product, additional_conditions, 'annual', device_property_list, logger
        )
        if not annual_df.empty:
            for _, row in annual_df.iterrows():
                product.add_annual_oper_data(
                    int(row['dt']), row['oper_old'], int(row['in_qty']), int(row['out_qty'])
                )
        else:
            if logger:
                logger.debug(f"产品 {product} 无年度生产数据")

        # 填充月度数据
        monthly_df = get_product_production_data(
            db_config, product, additional_conditions, 'monthly', device_property_list, logger
        )
        if not monthly_df.empty:
            for _, row in monthly_df.iterrows():
                product.add_monthly_oper_data(
                    row['dt'], row['oper_old'], int(row['in_qty']), int(row['out_qty'])
                )
        else:
            if logger:
                logger.debug(f"产品 {product} 无月度生产数据")


# 处理所有产品（支持空属性列表）
def process_all_products(db_config, pc_property_df, sv_property_df,
                         device_property_list, lot_property_list,
                         oper_list, additional_conditions_pc, additional_conditions_sv, logger=None):
    """处理所有PC和Server产品（支持空设备/批次属性列表）"""
    # 强制转为列表（避免None）
    device_property_list = device_property_list if isinstance(device_property_list, list) else []
    lot_property_list = lot_property_list if isinstance(lot_property_list, list) else []

    # 创建PC产品实例
    if logger:
        logger.info("开始创建PC产品实例...")
    pc_products = create_product_instances(
        pc_property_df, device_property_list, lot_property_list, logger=logger
    )

    # 创建Server产品实例
    if logger:
        logger.info("开始创建Server产品实例...")
    sv_products = create_product_instances(
        sv_property_df, device_property_list, lot_property_list, logger=logger
    )

    # 填充PC产品数据
    if logger:
        logger.info("开始填充PC产品生产数据...")
    populate_product_data(db_config, pc_products, additional_conditions_pc, device_property_list, logger)

    # 填充Server产品数据
    if logger:
        logger.info("开始填充Server产品生产数据...")
    populate_product_data(db_config, sv_products, additional_conditions_sv, device_property_list, logger)

    # 计算fail统计（原逻辑保留，支持空数据）
    for product in pc_products + sv_products:
        product.calculate_annual_fail_stats()
        product.calculate_monthly_fail_stats()

    return pc_products, sv_products


# 日志输出表格（支持空产品列表/空数据）
def log_fail_tables(product_list, product_type, logger):
    """输出产品不良数据表格日志（支持空数据）"""
    if not product_list:
        logger.info(f"没有{product_type}产品数据可输出（产品列表为空）")
        return

    # 周期定义（原逻辑保留）
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

        # 构建表格数据（支持空数据）
        table_data = [["Product"] + annual_periods_str + monthly_periods]  # 表头
        for product in product_list:
            row = [product.get_product_display_name()]
            # 年度数据（空值显示空字符串）
            for year in annual_periods:
                year_data = product.annual_data.get(year, {})
                fail_val = year_data.get('fail_stats', {}).get(fail_type)
                row.append(f"{fail_val:.2f}" if fail_val is not None else "")
            # 月度数据（空值显示空字符串）
            for month in monthly_periods:
                month_data = product.monthly_data.get(month, {})
                fail_val = month_data.get('fail_stats', {}).get(fail_type)
                row.append(f"{fail_val:.2f}" if fail_val is not None else "")
            table_data.append(row)

        # 格式化输出（支持空表格）
        if len(table_data) <= 1:
            logger.info("无不良数据可显示")
            logger.info(f"===== {title} 结束 =====\n")
            continue

        # 计算列宽
        col_widths = [max(len(str(row[i])) for row in table_data) for i in range(len(table_data[0]))]
        # 表头
        header_row = " | ".join([f"{str(h).ljust(col_widths[i])}" for i, h in enumerate(table_data[0])])
        logger.info(header_row)
        # 分隔线
        separator = "-|-".join(["-" * w for w in col_widths])
        logger.info(separator)
        # 数据行
        for row in table_data[1:]:
            data_row = " | ".join([f"{str(cell).ljust(col_widths[i])}" for i, cell in enumerate(row)])
            logger.info(data_row)

        logger.info(f"===== {title} 结束 =====\n")


# 日志输出QTY表格（支持空数据）
def log_qty_table(product_list, product_type, logger):
    """输出5600工序in数量表（支持空数据）"""
    if not product_list:
        logger.info(f"没有{product_type}产品的QTY数据可输出（产品列表为空）")
        return

    # 周期定义（原逻辑保留）
    current_year = datetime.now().year
    annual_periods = [current_year - 2, current_year - 1, current_year]
    annual_periods_str = [str(y) for y in annual_periods]

    last_month = datetime.now().replace(day=1) - timedelta(days=1)
    monthly_periods = [(last_month - dateutil.relativedelta.relativedelta(months=i)).strftime('%Y%m')
                       for i in range(11, -1, -1)]

    logger.info(f"\n===== {product_type} QTY数据（5600工序in数量） =====")

    # 构建表格数据（支持空数据）
    table_data = [["Product"] + annual_periods_str + monthly_periods]
    for product in product_list:
        row = [product.get_product_display_name()]
        # 年度数据（空值返回0）
        for year in annual_periods:
            row.append(str(product.get_5600_in_qty(year, is_annual=True)))
        # 月度数据（空值返回0）
        for month in monthly_periods:
            row.append(str(product.get_5600_in_qty(month, is_annual=False)))
        table_data.append(row)

    # 格式化输出
    if len(table_data) <= 1:
        logger.info("无QTY数据可显示")
        logger.info(f"===== {product_type} QTY数据结束 =====\n")
        return

    col_widths = [max(len(str(row[i])) for row in table_data) for i in range(len(table_data[0]))]
    header_row = " | ".join([f"{h.ljust(col_widths[i])}" for i, h in enumerate(table_data[0])])
    logger.info(header_row)
    logger.info("-|-".join(["-" * w for w in col_widths]))
    for row in table_data[1:]:
        data_row = " | ".join([f"{cell.ljust(col_widths[i])}" for i, cell in enumerate(row)])
        logger.info(data_row)

    logger.info(f"===== {product_type} QTY数据结束 =====\n")


def main(mode):
    # 初始化日志记录器
    log_name = datetime.today().strftime('%Y%m%d')
    try:
        # 数据库配置（原逻辑保留，建议将密码等敏感信息移到环境变量）
        db_config = {
            'host': None,
            'user': 'remoteuser',
            'password': 'password',
            'database': 'cmsalpha',
            'charset': 'utf8mb4',
            'port': 3306,
        }

        # 环境选择（原逻辑保留）
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

        # 时间范围定义（原逻辑保留）
        lastMonthLastDay = datetime.now().replace(day=1) - timedelta(days=1)
        str_endWorkdt = lastMonthLastDay.strftime('%Y%m%d')
        lastMonthFirstDay = lastMonthLastDay.replace(day=1)
        str_startWorkdt = lastMonthFirstDay.strftime('%Y%m%d')

        # 作业参数（可测试空列表：devicePropertyList = []，lotPropertyList = []）
        operList = ['5600', '5710', '5700', '5780']
        devicePropertyList = ['Product_Mode', 'Product_Density']  # 测试空列表时改为 []
        lotPropertyList = []  # 测试空列表时改为 []
        pcType = ['SD', 'UD']
        svType = ['RD', 'LD']

        # Step1：获取PC属性列表
        logger.info("开始查询PC产品属性列表...")
        additional_conditions_pc = [
            ('dd', 'Module_Type', 'IN', pcType),
            ('dy', 'oper_old', 'IN', operList),
            ('dd', 'Product_Mode', '=', 'DDR5 DIMM')
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

        # Step2：获取Server属性列表
        logger.info("开始查询Server产品属性列表...")
        additional_conditions_sv = [
            ('dd', 'Module_Type', 'IN', svType),
            ('dy', 'oper_old', 'IN', operList),
            ('dd', 'Product_Mode', '=', 'DDR5 DIMM')
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

        # Step3：处理所有产品（支持空属性列表）
        pc_products, sv_products = process_all_products(
            db_config=db_config,
            pc_property_df=pcPropertyList if pcPropertyList is not None else pd.DataFrame(),
            sv_property_df=svPropertyList if svPropertyList is not None else pd.DataFrame(),
            device_property_list=devicePropertyList,
            lot_property_list=lotPropertyList,
            oper_list=operList,
            additional_conditions_pc=additional_conditions_pc,
            additional_conditions_sv=additional_conditions_sv,
            logger=logger
        )

        # Step4：输出日志表格（支持空数据）
        log_fail_tables(pc_products, "PC", logger)
        log_fail_tables(sv_products, "SV", logger)
        log_qty_table(pc_products, "PC", logger)
        log_qty_table(sv_products, "SV", logger)

        logger.info("程序运行结束")

    except Exception as e:
        logger.critical(f"程序主流程出错: {str(e)}", exc_info=True)


if __name__ == '__main__':
    main('test')  # 测试时使用'test'模式，生产环境改为其他模式