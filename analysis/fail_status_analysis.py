from datetime import datetime, timedelta
import dateutil.relativedelta
import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging
from logging.handlers import RotatingFileHandler
import numpy as np
from scipy import stats


# 配置日志记录器
def setup_logger(log_path, log_name, max_bytes=10 * 1024 * 1024, backup_count=5):
    """
    设置日志记录器
    :param log_path: 日志文件保存路径
    :param log_name: 日志文件名
    :param max_bytes: 单个日志文件最大大小
    :param backup_count: 备份日志文件数量
    :return: 配置好的logger对象
    """
    # 确保日志目录存在
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    # 日志文件完整路径
    log_file = os.path.join(log_path, log_name)

    # 创建logger
    logger = logging.getLogger("ProductionAnalysis")
    logger.setLevel(logging.DEBUG)  # 设置最低日志级别

    # 定义日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 创建文件处理器，支持日志轮转
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 添加处理器到logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# 产品类：包括产品基本属性及状态
class product:
    def __init__(self, product_id, product_name, product_type, product_status):
        self.product_id = product_id
        self.product_name = product_name
        self.product_type = product_type
        self.product_status = product_status

    def __str__(self):
        return f"Product(ID: {self.product_id}, Name: {self.product_name}, Type: {self.product_type}, Status: {self.product_status})"


# 获取Daily Fail Status
def get_fail_ttl(db_config, workdt, logger):
    # 从db_failstatus中获取fail status指标
    query = text("""
            select oper_old, 
                   (1 - sum(out_qty) / sum(in_qty)) * 100 as fail 
            from cmsalpha.db_yielddetail dy 
            where workdt = :work_date 
            group by oper_old;
        """)
    try:
        logger.info(f"开始查询日期为 {workdt} 的Fail Status")

        # 构建数据库连接字符串
        conn_str = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}?"
            f"charset={db_config['charset']}"
        )

        with create_engine(conn_str).connect() as conn:
            # 执行参数化查询，传入workdt参数
            df = pd.read_sql(query.bindparams(work_date=workdt), conn)

        logger.info(f"查询成功，返回 {len(df)} 条记录")
        # 记录数据详情（可以根据需要调整日志级别或是否记录）
        if not df.empty:
            logger.debug(f"查询结果:\n{df.to_string()}")

    except Exception as e:
        error_msg = f"获取Fail Status时出错: {str(e)}"
        logger.error(error_msg, exc_info=True)  # exc_info=True会记录完整的堆栈信息
        df = pd.DataFrame()

    return df


# 获取oper别半年度数据
def get_fail_oper(db_config, oper, dt_semiYear, yesterday, logger):
    """
    查询指定工程在指定日期范围内的Fail Status数据，使用完全参数化查询

    参数:
        db_config: 数据库配置字典
        oper: 工程代码（如'5780'）
        dt_semiYear: 起始日期（格式'YYYYMMDD'）
        yesterday: 结束日期（格式'YYYYMMDD'）
        logger: 日志记录器

    返回:
        DataFrame: 包含月份(dt)和Fail Status(fail)的数据
    """
    # 完全参数化的SQL查询，所有变量均使用占位符
    query = text("""
        select 
            substring(workdt, 1, 6) as dt,  -- 提取年月部分(如202502)
            sum(in_qty) as in_qty,
            (1 - sum(out_qty) / sum(in_qty)) * 100 as fail 
        from 
            cmsalpha.db_yielddetail dy 
        where 
            workdt between :start_date and :end_date  -- 日期范围参数
            and oper_old = :operation  -- 工程代码参数
        group by 
            dt 
        order by 
            dt asc;
    """)

    try:
        # 验证输入参数格式
        if not all([
            len(dt_semiYear) == 8,
            len(yesterday) == 8,
            dt_semiYear.isdigit(),
            yesterday.isdigit()
        ]):
            raise ValueError(f"日期格式必须为8位数字(YYYYMMDD)，实际输入: {dt_semiYear} 至 {yesterday}")

        logger.info(f"执行参数化查询 - 工程: {oper}, 日期范围: {dt_semiYear} 至 {yesterday}")

        # 构建数据库连接字符串
        conn_str = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}?"
            f"charset={db_config['charset']}"
        )

        with create_engine(conn_str).connect() as conn:
            # 绑定参数并执行查询
            df = pd.read_sql(
                query.bindparams(
                    start_date=dt_semiYear,  # 绑定起始日期参数
                    end_date=yesterday,  # 绑定结束日期参数
                    operation=oper  # 绑定工程代码参数
                ),
                conn
            )

        record_count = len(df)
        if record_count > 0:
            logger.info(f"查询成功，返回 {record_count} 条记录 (月份范围: {df['dt'].min()} 至 {df['dt'].max()})")
            logger.debug(f"查询结果:\n{df.to_string()}")
        else:
            logger.warning(f"工程 {oper} 在 {dt_semiYear} 至 {yesterday} 期间无数据")

        return df

    except ValueError as ve:
        logger.error(f"参数验证失败: {str(ve)}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"查询执行失败 (工程: {oper}): {str(e)}", exc_info=True)
        return pd.DataFrame()


# 计算P控制图的控制限
def calculate_control_limits(df_oper, logger):
    """
    根据工序历史数据计算控制限

    参数:
        df_oper: 包含特定工序历史数据的DataFrame，需包含'in_qty'和'fail'列
        logger: 日志记录器

    返回:
        tuple: (p_bar, LCL, UCL) 平均不良率、下控制限、上控制限
    """
    try:
        # 检查必要的列是否存在
        required_cols = ['in_qty', 'fail']
        if not set(required_cols).issubset(df_oper.columns):
            missing = [col for col in required_cols if col not in df_oper.columns]
            raise ValueError(f"df_oper缺少必要列: {missing}")

        # 移除无效数据（in_qty为0或fail为空的记录）
        valid_df = df_oper[(df_oper['in_qty'] > 0) & (df_oper['fail'].notna())].copy()

        if valid_df.empty:
            logger.warning("没有有效的历史数据用于计算控制限")
            return (None, None, None)

        # 将fail从百分比转换为比例（除以100）
        valid_df['fail_rate'] = valid_df['fail'] / 100

        # 计算平均不良率p_bar（加权平均，考虑不同样本量）
        valid_df['total_fail'] = valid_df['fail_rate'] * valid_df['in_qty']
        total_in_qty = valid_df['in_qty'].sum()
        total_fail = valid_df['total_fail'].sum()
        p_bar = total_fail / total_in_qty if total_in_qty > 0 else 0

        # 计算平均样本量（用于控制限计算）
        n_avg = valid_df['in_qty'].mean()

        # 计算控制限（p控制图公式）
        if n_avg > 0 and p_bar > 0:
            # 标准差
            std_dev = np.sqrt(p_bar * (1 - p_bar) / n_avg)
            # 上控制限
            UCL = p_bar + 3 * std_dev
            # 下控制限（不小于0）
            LCL = max(p_bar - 3 * std_dev, 0)

            # 转换为百分比（与原始数据保持一致）
            p_bar_pct = p_bar * 100
            UCL_pct = UCL * 100
            LCL_pct = LCL * 100

            logger.info(f"控制限计算完成 - 平均不良率: {p_bar_pct:.4f}%, LCL: {LCL_pct:.4f}%, UCL: {UCL_pct:.4f}%")
            return (p_bar_pct, LCL_pct, UCL_pct)
        else:
            logger.warning("样本量或平均不良率为0，无法计算控制限")
            return (None, None, None)

    except Exception as e:
        logger.error(f"计算控制限时出错: {str(e)}", exc_info=True)
        return (None, None, None)


# 比较当前工序不良率与控制限
def compare_with_control_limits(df_ttl, oper, LCL, UCL, logger):
    """
    将当日工序不良率与控制限比较

    参数:
        df_ttl: 包含当日各工序不良率的DataFrame，需包含'oper_old'和'fail'列
        oper: 当前工序标识
        LCL: 下控制限（百分比）
        UCL: 上控制限（百分比）
        logger: 日志记录器

    返回:
        dict: 比较结果，包含判定状态和具体数值
    """
    try:
        # 提取当前工序的当日不良率
        oper_data = df_ttl[df_ttl['oper_old'] == oper]

        if oper_data.empty:
            logger.warning(f"工序 {oper} 无当日不良率数据")
            return {
                'oper': oper,
                'current_fail': None,
                'LCL': LCL,
                'UCL': UCL,
                'status': '无数据',
                'message': '未找到当日不良率记录'
            }

        current_fail = oper_data.iloc[0]['fail']

        # 判定逻辑
        if LCL is None or UCL is None:
            status = '无法判定'
            message = '控制限计算失败，无法进行判定'
        elif current_fail > UCL:
            status = '异常（偏高）'
            message = f"当日不良率({current_fail:.4f}%)超过上控制限({UCL:.4f}%)"
        elif current_fail < LCL:
            status = '异常（偏低）'
            message = f"当日不良率({current_fail:.4f}%)低于下控制限({LCL:.4f}%)"
        else:
            status = '正常'
            message = f"当日不良率({current_fail:.4f}%)在控制范围内"

        logger.info(f"工序 {oper} 判定结果: {status} - {message}")

        return {
            'oper': oper,
            'current_fail': current_fail,
            'LCL': LCL,
            'UCL': UCL,
            'status': status,
            'message': message
        }

    except Exception as e:
        logger.error(f"工序 {oper} 比较判定时出错: {str(e)}", exc_info=True)
        return {
            'oper': oper,
            'current_fail': None,
            'LCL': LCL,
            'UCL': UCL,
            'status': '判定失败',
            'message': f'比较过程出错: {str(e)}'
        }


# 针对异常工序进行分析
def analyze_abnormal_oper(db_config, oper, workdt, logger):
    """
    分析异常工序的详细不良原因，基于加权不良率（产量占比×不良率）进行帕累托分析

    参数:
        db_config: 数据库配置字典
        oper: 异常工序代码
        workdt: 异常日期（格式'YYYYMMDD'）
        logger: 日志记录器

    返回:
        tuple: (analysis_df, vital_factors_df)
               完整分析数据和主要不良原因数据（按80%规则筛选）
    """
    try:
        logger.info(f"开始分析异常工序 {oper} 在 {workdt} 的详细不良原因（基于加权不良率）")

        # 1. 执行详细SQL查询（保留原始字段）
        query = text("""
            select 
                dd.Product_Mode, 
                dd.Tech_Name, 
                dd.Die_Density, 
                dd.Product_Density, 
                dd.Module_Type, 
                dy.grade, 
                sum(in_qty) as in_qty,
                sum(out_qty) as out_qty,
                (1 - sum(out_qty)/sum(in_qty)) * 100 as fail  -- 原始不良率（百分比）
            from 
                cmsalpha.db_yielddetail dy 
            join 
                modulemte.db_deviceinfo dd on dy.device = dd.Device 
            where 
                dy.workdt = :work_date 
                and dy.oper_old = :operation 
            group by 
                dd.Product_Mode, dd.Tech_Name, dd.Die_Density, 
                dd.Product_Density, dd.Module_Type, dy.grade
            order by 
                in_qty desc;
        """)

        # 构建数据库连接
        conn_str = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}?"
            f"charset={db_config['charset']}"
        )

        with create_engine(conn_str).connect() as conn:
            df = pd.read_sql(
                query.bindparams(work_date=workdt, operation=oper),
                conn
            )

        if df.empty:
            logger.warning(f"未查询到工序 {oper} 在 {workdt} 的详细产品数据")
            return (pd.DataFrame(), pd.DataFrame())

        logger.info(f"成功获取 {len(df)} 条产品详细数据用于分析")
        logger.debug(f"原始分析数据:\n{df.to_string(index=False)}")

        # 2. 计算加权不良率（核心优化点）
        # 2.1 计算该工序当日总投入量（用于计算产量占比）
        total_in_qty = df['in_qty'].sum()
        if total_in_qty == 0:
            logger.warning("该工序当日总投入量为0，无法计算加权不良率")
            return (df, pd.DataFrame())

        # 2.2 计算每个产品的产量占比（in_qty占比）
        df['in_qty_ratio'] = df['in_qty'] / total_in_qty  # 占比（0-1之间）

        # 2.3 计算加权不良率 = 产量占比 × 原始不良率（兼顾产量和不良率）
        df['weighted_fail'] = df['in_qty_ratio'] * df['fail']

        # 3. 基于加权不良率的帕累托分析
        # 3.1 按加权不良率降序排序（替代原有的单纯按fail排序）
        df_sorted = df.sort_values('weighted_fail', ascending=False).copy()

        # 3.2 计算加权不良率的累积百分比（用于80%规则筛选）
        total_weighted_fail = df_sorted['weighted_fail'].sum()
        if total_weighted_fail == 0:
            logger.warning("所有产品加权不良率均为0，无法筛选主要不良原因")
            return (df_sorted, pd.DataFrame())

        df_sorted['cumulative_percent'] = df_sorted['weighted_fail'].cumsum() / total_weighted_fail * 100

        # 4. 筛选主要不良原因（累积加权不良率≤80%的因素）
        vital_factors_df = df_sorted[df_sorted['cumulative_percent'] <= 80].copy()

        # 5. 输出主要不良原因（包含加权相关指标）
        if not vital_factors_df.empty:
            logger.info(f"\n===== 主要不良原因（按加权不良率80%规则筛选，共 {len(vital_factors_df)} 项） =====")
            # 打印完整属性（新增产量占比和加权不良率）
            logger.info(
                "\n" +  # 换行使表头独立
                vital_factors_df[
                    ['Product_Mode', 'Tech_Name', 'Die_Density',
                     'Product_Density', 'Module_Type', 'grade',
                     'in_qty', 'in_qty_ratio', 'fail', 'weighted_fail', 'cumulative_percent']
                ].to_string(index=False, float_format='%.4f')  # 格式化浮点数显示
            )
        else:
            logger.warning("未找到符合80%规则的主要不良原因（可能数据分布均匀）")

        return (df_sorted, vital_factors_df)

    except Exception as e:
        logger.error(f"分析异常工序时出错: {str(e)}", exc_info=True)
        return (pd.DataFrame(), pd.DataFrame())


# 针对产品分析分析该工序含量的变化
def analyze_product_proportion(vital_df, db_config, oper, dt_semiYear, yesterday, logger, trend_threshold=0.01):
    """
    分析vital_df中每种产品的每月产量占比及趋势（增加/稳定/降低）

    参数:
        vital_df: 主要不良原因产品数据（含Product_Mode等属性）
        db_config: 数据库配置
        oper: 目标工序
        dt_semiYear: 起始日期（YYYYMMDD）
        yesterday: 结束日期（YYYYMMDD）
        logger: 日志记录器
        trend_threshold: 趋势判断阈值（斜率绝对值小于此值视为稳定）

    返回:
        dict: 键为产品唯一标识，值为包含每月占比和趋势的分析结果
    """
    product_trend_results = {}

    try:
        # 1. 先查询该工序在时间范围内的每月总in_qty（用于计算占比）
        total_in_query = text("""
            select 
                substring(workdt, 1, 6) as dt,  -- 月份（如202502）
                sum(in_qty) as total_in_qty     -- 工序当月总投入量
            from 
                cmsalpha.db_yielddetail 
            where 
                oper_old = :oper 
                and workdt between :start and :end 
            group by 
                dt 
            order by 
                dt asc;
        """)

        with create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                           f"{db_config['host']}:{db_config['port']}/{db_config['database']}?"
                           f"charset={db_config['charset']}").connect() as conn:
            # 获取工序每月总投入量
            total_in_df = pd.read_sql(
                total_in_query.bindparams(oper=oper, start=dt_semiYear, end=yesterday),
                conn
            )

        if total_in_df.empty:
            logger.warning(f"工序 {oper} 在 {dt_semiYear} 至 {yesterday} 期间无总产量数据，无法计算占比")
            return product_trend_results

        # 2. 遍历vital_df中的每种产品，计算其每月占比
        # 产品唯一标识：由vital_df中的6个属性共同确定
        product_attrs = ['Product_Mode', 'Tech_Name', 'Die_Density',
                         'Product_Density', 'Module_Type', 'grade']

        for _, product_row in vital_df.iterrows():
            # 提取当前产品的属性（用于筛选数据）
            product_info = {attr: product_row[attr] for attr in product_attrs}
            # 生成产品唯一标识（用于日志和结果键）
            product_id = " |".join([f"{v}" for k, v in product_info.items()])
            logger.info(f"开始分析产品: {product_id} 的每月占比趋势")

            # 3. 查询该产品在时间范围内的每月in_qty
            product_in_query = text("""
                select 
                    substring(workdt, 1, 6) as dt,  -- 月份
                    sum(in_qty) as product_in_qty   -- 产品当月投入量
                from 
                    cmsalpha.db_yielddetail dy
                    join modulemte.db_deviceinfo dd on dy.device = dd.Device
                where 
                    dy.oper_old = :oper 
                    and dy.workdt between :start and :end 
                    and dd.Product_Mode = :pm 
                    and dd.Tech_Name = :tn 
                    and dd.Die_Density = :dd 
                    and dd.Product_Density = :pd 
                    and dd.Module_Type = :mt 
                    and dy.grade = :g 
                group by 
                    dt 
                order by 
                    dt asc;
            """)

            with create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                               f"{db_config['host']}:{db_config['port']}/{db_config['database']}?"
                               f"charset={db_config['charset']}").connect() as conn:
                product_in_df = pd.read_sql(
                    product_in_query.bindparams(
                        oper=oper,
                        start=dt_semiYear,
                        end=yesterday,
                        pm=product_info['Product_Mode'],
                        tn=product_info['Tech_Name'],
                        dd=product_info['Die_Density'],
                        pd=product_info['Product_Density'],
                        mt=product_info['Module_Type'],
                        g=product_info['grade']
                    ),
                    conn
                )

            # 4. 合并数据计算占比（产品月投入 / 工序月总投入 * 100%）
            # 确保月份对齐（左连接，保留所有工序有数据的月份）
            merge_df = pd.merge(
                total_in_df,
                product_in_df,
                on='dt',
                how='left'
            ).fillna(0)  # 产品当月无数据时，投入量视为0

            # 计算占比（避免除零错误）
            merge_df['ratio'] = np.where(
                merge_df['total_in_qty'] > 0,
                (merge_df['product_in_qty'] / merge_df['total_in_qty']) * 100,
                0
            )

            # 5. 趋势分析（基于线性回归斜率判断）
            # 生成月份索引（1,2,3...代表第1个月、第2个月...）
            merge_df['month_index'] = range(1, len(merge_df) + 1)

            # 线性回归拟合：y = kx + b（k为斜率，反映趋势）
            if len(merge_df) >= 2:  # 至少2个月份才能判断趋势
                x = merge_df['month_index'].values
                y = merge_df['ratio'].values
                slope, _ = np.polyfit(x, y, 1)  # 一阶多项式拟合（线性）

                # 根据斜率判断趋势
                if slope > trend_threshold:
                    trend = "逐渐增加"
                elif slope < -trend_threshold:
                    trend = "逐渐降低"
                else:
                    trend = "稳定"
            else:
                trend = "数据不足（<2个月）"

            # 6. 整理结果
            result = {
                'monthly_data': merge_df[['dt', 'total_in_qty', 'product_in_qty', 'ratio']],
                'trend': trend,
                'slope': slope if len(merge_df) >= 2 else None,
                'product_info': product_info
            }

            # 日志输出关键信息
            logger.info(f"产品 {product_id} 趋势分析结果: {trend}")
            logger.debug(f"每月占比数据:\n{merge_df[['dt', 'ratio']].to_string(index=False)}")

            product_trend_results[product_id] = result

    except Exception as e:
        logger.error(f"产品趋势分析出错: {str(e)}", exc_info=True)

    return product_trend_results


# 针对产品分析半年度不良率趋势
def analyze_product_failRate(db_config, oper, vital_df, dt_semiYear, yesterday, logger,
                             stability_threshold=0.5, significance_level=0.05):
    """
    参数:
        db_config: 数据库配置字典
        oper: 目标工序代码
        vital_df: 主要不良产品数据（含Product_Mode等属性）
        dt_semiYear: 半年起始日期（YYYYMMDD）
        yesterday: 结束日期（YYYYMMDD）
        logger: 日志记录器
        stability_threshold: 稳定性阈值（斜率绝对值≤此值视为稳定，单位：%/月）
        significance_level: 统计显著性水平（默认0.05）

    返回:
        dict: 键为产品唯一标识，值包含：
            - monthly_data: 半年度每月原始数据（dt, sum_in, sum_out, fail_rate）
            - conclusion: 趋势结论（字符串）
            - kpi: 考核指标（字典）
    """
    combined_results = {}

    try:
        if vital_df.empty:
            logger.warning("vital_df为空，无需执行半年度分析")
            return combined_results

        product_attrs = ['Product_Mode', 'Tech_Name', 'Die_Density',
                         'Product_Density', 'Module_Type', 'grade']

        # 1. 遍历主要不良产品，获取半年度数据（原Step6）
        for _, product_row in vital_df.iterrows():
            product_info = {attr: product_row[attr] for attr in product_attrs}
            product_id = " |".join([f"{v}" for k, v in product_info.items()])
            logger.info(f"开始分析产品 {product_id} 的半年度数据及趋势（工序：{oper}）")

            # 1.1 查询半年度每月数据
            query = text("""
                select 
                    substring(workdt, 1, 6) as dt,
                    sum(in_qty) as sum_in,
                    sum(out_qty) as sum_out
                from 
                    cmsalpha.db_yielddetail dy
                    join modulemte.db_deviceinfo dd on dy.device = dd.Device
                where 
                    dy.oper_old = :oper 
                    and dy.workdt between :start_dt and :end_dt 
                    and dd.Product_Mode = :pm 
                    and dd.Tech_Name = :tn 
                    and dd.Die_Density = :dd 
                    and dd.Product_Density = :pd 
                    and dd.Module_Type = :mt 
                    and dy.grade = :g 
                group by 
                    dt 
                order by 
                    dt asc;
            """)

            conn_str = (
                f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
                f"{db_config['host']}:{db_config['port']}/{db_config['database']}?"
                f"charset={db_config['charset']}"
            )

            with create_engine(conn_str).connect() as conn:
                monthly_df = pd.read_sql(
                    query.bindparams(
                        oper=oper, start_dt=dt_semiYear, end_dt=yesterday,
                        pm=product_info['Product_Mode'], tn=product_info['Tech_Name'],
                        dd=product_info['Die_Density'], pd=product_info['Product_Density'],
                        mt=product_info['Module_Type'], g=product_info['grade']
                    ),
                    conn
                )

            # 1.2 处理原始数据（计算不良率）
            if monthly_df.empty:
                logger.warning(f"产品 {product_id} 无半年度数据")
                combined_results[product_id] = {
                    "monthly_data": pd.DataFrame(),
                    "conclusion": "无数据",
                    "kpi": None
                }
                continue

            # 计算每月不良率（处理除零问题）
            monthly_df['fail_rate'] = np.where(
                monthly_df['sum_in'] > 0,
                (1 - monthly_df['sum_out'] / monthly_df['sum_in']) * 100,
                0
            )
            monthly_df = monthly_df.sort_values('dt').reset_index(drop=True)
            valid_df = monthly_df[['dt', 'sum_in', 'sum_out', 'fail_rate']].copy()

            # 2. 趋势分析（原Step7）
            # 2.1 校验数据量（至少3个月）
            if len(valid_df) < 3:
                logger.warning(f"产品 {product_id} 有效数据不足3个月（仅{len(valid_df)}个月）")
                combined_results[product_id] = {
                    "monthly_data": valid_df,
                    "conclusion": f"数据不足（仅{len(valid_df)}个月，无法判断趋势）",
                    "kpi": None
                }
                continue

            # 2.2 线性回归分析趋势
            valid_df['month_index'] = range(1, len(valid_df) + 1)  # 月份连续索引
            x = valid_df['month_index'].values
            y = valid_df['fail_rate'].values
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

            # 2.3 判断趋势（稳定/恶化/改善）
            trend = "稳定"
            if p_value < significance_level:  # 趋势显著
                if slope > stability_threshold:
                    trend = "恶化"
                elif slope < -stability_threshold:
                    trend = "改善"
            else:
                trend = "稳定（趋势不显著）"

            # 2.4 计算考核指标
            kpi = {
                "半年度平均不良率（%）": round(np.mean(y), 4),
                "最高不良率（%）及月份": (round(np.max(y), 4), valid_df.loc[y.argmax(), 'dt']),
                "最低不良率（%）及月份": (round(np.min(y), 4), valid_df.loc[y.argmin(), 'dt']),
                "每月平均变化率（%/月）": round(slope, 4),
                "趋势显著性（p值）": round(p_value, 4),
                "首尾月变化幅度（%）": round(
                    ((y[-1] - y[0]) / y[0]) * 100, 2
                ) if y[0] != 0 else None
            }

            # 2.5 生成结论描述
            conclusion = (
                f"产品 {product_id} 半年度不良率趋势为：{trend}。"
                f"平均不良率 {kpi['半年度平均不良率（%）']}%，"
                f"每月平均变化 {kpi['每月平均变化率（%/月）']}%，"
                f"首尾月变化 {kpi['首尾月变化幅度（%）']}%。"
            )

            # 3. 保存结果
            combined_results[product_id] = {
                "monthly_data": valid_df,
                "conclusion": conclusion,
                "kpi": kpi
            }

            logger.info(f"产品 {product_id} 分析完成：{conclusion}")
            logger.debug(
                f"详细数据：\n{valid_df.drop(columns=['month_index']).to_string(index=False)}"
            )

    except Exception as e:
        logger.error(f"半年度趋势分析合并方法出错：{str(e)}", exc_info=True)

    return combined_results

# 主函数
def main(mode):
    # 初始化日志记录器
    log_path = "C:/Users/Tengjun Zhao/Desktop"
    log_name = datetime.today().strftime('%Y%m%d')
    logger = setup_logger(log_path, log_name)
    logger.info("程序开始运行")

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
            logger.info("使用测试环境数据库(localhost)")
        else:
            db_config['host'] = '172.27.154.57'
            logger.info("使用生产环境数据库(172.27.154.57)")

        # 作业时间计算
        yesterday_date = datetime.now() - timedelta(days=1)
        yesterday = yesterday_date.strftime('%Y%m%d')
        dt_semiYear_date = yesterday_date - dateutil.relativedelta.relativedelta(months=6)
        dt_semiYear_date = dt_semiYear_date.replace(day=1)
        dt_semiYear = dt_semiYear_date.strftime('%Y%m%d')

        logger.info(f"计算日期: 昨天={yesterday}, 半年前月初={dt_semiYear}")

        # Step1： 获取Daily Fail Status TTL
        logger.info("Step1: 开始获取每日Fail Status数据")
        df_ttl = get_fail_ttl(db_config, yesterday, logger)

        # Step2：逐个工程查询半年度数据
        logger.info("Step2: 开始处理半年度数据查询")
        operList = ['5600', '5710', '5700', '5780']
        if not df_ttl.empty:
            logger.info(f"成功获取每日Fail Status，共 {len(df_ttl)} 个工程")
            logger.info(f"各工程半年度Fail Status情况")
            results = []
            for oper in operList:
                # 2.1 获取半年度数据
                df_oper = get_fail_oper(db_config, oper, dt_semiYear, yesterday, logger)
                # 2.2计算控制限
                p_bar, LCL, UCL = calculate_control_limits(df_oper, logger)
                # Step3. 与当日数据比较
                result = compare_with_control_limits(df_ttl, oper, LCL, UCL, logger)
                results.append(result)
                # Step4. 分析异常工序
                full_df, vital_df = analyze_abnormal_oper(
                    db_config=db_config,
                    oper=oper,
                    workdt=yesterday,  # 异常日期
                    logger=logger
                )
                # Step5. 针对主要不良产品分析产品占比变化（排查产品结构变化导致的）
                if not vital_df.empty:
                    trend_analysis = analyze_product_proportion(
                        vital_df=vital_df,
                        db_config=db_config,
                        oper=oper,  # 当前分析的工序
                        dt_semiYear=dt_semiYear,
                        yesterday=yesterday,
                        logger=logger,
                        trend_threshold=0.02  # 可根据实际需求调整阈值
                    )
                # Step6. 针对主要不良产品分析半年度变化
                    semi_annual_result = analyze_product_failRate(
                        db_config=db_config,
                        oper=oper,
                        vital_df=vital_df,
                        dt_semiYear=dt_semiYear,
                        yesterday=yesterday,
                        logger=logger,
                        stability_threshold=0.3,  # 例如：每月变化≤0.3%视为稳定
                        significance_level=0.05
                    )
                # Step7. 针对主要不良产品分析设备差异

        else:
            logger.warning("未获取到任何每日Fail Status")

        logger.info("程序运行结束")

    except Exception as e:
        logger.critical(f"程序主流程出错: {str(e)}", exc_info=True)


if __name__ == '__main__':
    main('mfg')
