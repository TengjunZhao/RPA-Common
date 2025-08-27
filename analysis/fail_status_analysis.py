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
def setup_logger(
        log_path,
        log_name,
        max_bytes=10 * 1024 * 1024,
        backup_count=5,
        file_level=logging.INFO,  # 文件日志级别（默认只记录INFO及以上）
        console_level=logging.INFO  # 控制台日志级别（默认只显示INFO及以上）
):
    """
    设置日志记录器，可分别控制文件和控制台的日志级别

    参数:
        file_level: 日志文件记录级别（如logging.DEBUG、logging.INFO）
        console_level: 控制台输出级别
    """
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    log_file = os.path.join(log_path, log_name)

    logger = logging.getLogger("ProductionAnalysis")
    logger.setLevel(logging.DEBUG)  # 根日志级别设为最低（保证子处理器能生效）
    logger.handlers = []  # 清空已有处理器，避免重复输出

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 1. 文件处理器（控制文件日志级别）
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(file_level)  # 设置文件日志级别
    logger.addHandler(file_handler)

    # 2. 控制台处理器（控制控制台输出级别）
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(console_level)  # 设置控制台输出级别
    logger.addHandler(console_handler)

    return logger


# 产品类：包括产品基本属性及状态
class Product:
    """产品类，封装产品属性及所有相关分析方法"""

    def __init__(self, product_info, db_config, logger):
        # 1. 产品核心属性（从vital_df提取的6个关键属性）
        self.product_mode = product_info['Product_Mode']
        self.tech_name = product_info['Tech_Name']
        self.die_density = product_info['Die_Density']
        self.product_density = product_info['Product_Density']
        self.module_type = product_info['Module_Type']
        self.grade = product_info['grade']

        # 2. 外部依赖（数据库配置、日志器）
        self.db_config = db_config
        self.logger = logger

        # 3. 分析结果存储（便于后续取用）
        self.proportion_result = None  # 占比趋势分析结果
        self.failrate_result = None  # 不良率趋势分析结果

        # 4. 唯一标识（用于日志和区分）
        self.product_id = " ".join([
            f"{self.product_mode}", f"{self.tech_name}",
            f"{self.die_density}", f"{self.product_density}",
            f"{self.module_type}", f"{self.grade}"
        ])

    def __str__(self):
        return f"Product[{self.product_id}]"

    # ------------------------------
    # 封装分析方法：占比趋势分析
    # ------------------------------
    def analyze_proportion(self, oper, dt_semiYear, yesterday, trend_threshold=0.01):
        """分析当前产品在工序中的每月占比趋势（原analyze_product_proportion）"""
        self.logger.info(f"\n===== 开始分析 {self} 的占比趋势 =====")
        try:
            # 1. 查询工序每月总投入量
            total_in_query = text("""
                select substring(workdt, 1, 6) as dt, sum(in_qty) as total_in_qty 
                from cmsalpha.db_yielddetail 
                where oper_old = :oper and workdt between :start and :end 
                group by dt order by dt asc;
            """)
            conn_str = self._get_conn_str()
            with create_engine(conn_str).connect() as conn:
                total_in_df = pd.read_sql(
                    total_in_query.bindparams(oper=oper, start=dt_semiYear, end=yesterday),
                    conn
                )

            if total_in_df.empty:
                self.logger.warning(f"工序 {oper} 无总产量数据，无法计算占比")
                return None

            # 2. 查询当前产品的每月投入量
            product_in_query = text("""
                select substring(workdt, 1, 6) as dt, sum(in_qty) as product_in_qty 
                from cmsalpha.db_yielddetail dy
                join modulemte.db_deviceinfo dd on dy.device = dd.Device
                where dy.oper_old = :oper 
                    and dy.workdt between :start and :end 
                    and dd.Product_Mode = :pm 
                    and dd.Tech_Name = :tn 
                    and dd.Die_Density = :dd 
                    and dd.Product_Density = :pd 
                    and dd.Module_Type = :mt 
                    and dy.grade = :g 
                group by dt order by dt asc;
            """)

            with create_engine(conn_str).connect() as conn:
                product_in_df = pd.read_sql(
                    product_in_query.bindparams(
                        oper=oper,
                        start=dt_semiYear,
                        end=yesterday,
                        pm=self.product_mode,
                        tn=self.tech_name,
                        dd=self.die_density,
                        pd=self.product_density,
                        mt=self.module_type,
                        g=self.grade
                    ),
                    conn
                )

            # 3. 计算占比及趋势
            merge_df = pd.merge(total_in_df, product_in_df, on='dt', how='left').fillna(0)
            merge_df['ratio'] = np.where(
                merge_df['total_in_qty'] > 0,
                (merge_df['product_in_qty'] / merge_df['total_in_qty']) * 100,
                0
            )

            # 趋势判断
            merge_df['month_index'] = range(1, len(merge_df) + 1)
            trend = "数据不足（<2个月）"
            slope = None
            if len(merge_df) >= 2:
                x = merge_df['month_index'].values
                y = merge_df['ratio'].values
                slope, _ = np.polyfit(x, y, 1)
                if slope > trend_threshold:
                    trend = "逐渐增加"
                elif slope < -trend_threshold:
                    trend = "逐渐降低"
                else:
                    trend = "稳定"

            # 保存结果
            self.proportion_result = {
                'trend': trend,
                'slope': slope,
                'monthly_data': merge_df
            }

            # 日志输出
            self.logger.info(f"{self} 占比趋势：{trend}")
            self.logger.debug(f"每月占比数据:\n{merge_df[['dt', 'ratio']].to_string(index=False)}")
            return self.proportion_result

        except Exception as e:
            self.logger.error(f"{self} 占比分析出错: {str(e)}", exc_info=True)
            return None

    # ------------------------------
    # 封装分析方法：不良率趋势分析
    # ------------------------------
    def analyze_fail_rate(self, oper, dt_semiYear, yesterday,
                          stability_threshold=0.5, significance_level=0.05):
        """分析当前产品的半年度不良率趋势（原analyze_product_failRate）"""
        self.logger.info(f"\n===== 开始分析 {self} 的不良率趋势 =====")
        try:
            # 1. 查询产品半年度数据
            query = text("""
                select substring(workdt, 1, 6) as dt, sum(in_qty) as sum_in, sum(out_qty) as sum_out
                from cmsalpha.db_yielddetail dy
                join modulemte.db_deviceinfo dd on dy.device = dd.Device
                where dy.oper_old = :oper 
                    and dy.workdt between :start_dt and :end_dt 
                    and dd.Product_Mode = :pm 
                    and dd.Tech_Name = :tn 
                    and dd.Die_Density = :dd 
                    and dd.Product_Density = :pd 
                    and dd.Module_Type = :mt 
                    and dy.grade = :g 
                group by dt order by dt asc;
            """)

            conn_str = self._get_conn_str()
            with create_engine(conn_str).connect() as conn:
                monthly_df = pd.read_sql(
                    query.bindparams(
                        oper=oper,
                        start_dt=dt_semiYear,
                        end_dt=yesterday,
                        pm=self.product_mode,
                        tn=self.tech_name,
                        dd=self.die_density,
                        pd=self.product_density,
                        mt=self.module_type,
                        g=self.grade
                    ),
                    conn
                )

            if monthly_df.empty:
                self.logger.warning(f"{self} 无半年度数据")
                return None

            # 2. 计算不良率及趋势
            monthly_df['fail_rate'] = np.where(
                monthly_df['sum_in'] > 0,
                (1 - monthly_df['sum_out'] / monthly_df['sum_in']) * 100,
                0
            )
            valid_df = monthly_df[['dt', 'sum_in', 'sum_out', 'fail_rate']].copy()

            # 趋势判断
            trend = "数据不足（<3个月）"
            kpi = None
            if len(valid_df) >= 3:
                valid_df['month_index'] = range(1, len(valid_df) + 1)
                x = valid_df['month_index'].values
                y = valid_df['fail_rate'].values
                slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)

                trend = "稳定"
                if p_value < significance_level:
                    if slope > stability_threshold:
                        trend = "恶化"
                    elif slope < -stability_threshold:
                        trend = "改善"
                else:
                    trend = "稳定（趋势不显著）"

                # 计算考核指标
                kpi = {
                    "半年度平均不良率（%）": round(np.mean(y), 4),
                    "最高不良率（%）及月份": (round(np.max(y), 4), valid_df.loc[y.argmax(), 'dt']),
                    "每月平均变化率（%/月）": round(slope, 4),
                    "首尾月变化幅度（%）": round(((y[-1] - y[0]) / y[0]) * 100, 2) if y[0] != 0 else None
                }

            # 保存结果
            self.failrate_result = {
                'conclusion': f"{self} 不良率趋势：{trend}" + (
                    f"，平均不良率 {kpi['半年度平均不良率（%）']}%" if kpi else ""),
                'kpi': kpi,
                'monthly_data': valid_df
            }

            # 日志输出
            self.logger.info(self.failrate_result['conclusion'])
            self.logger.debug(f"详细数据：\n{valid_df[['dt', 'fail_rate']].to_string(index=False)}")
            return self.failrate_result

        except Exception as e:
            self.logger.error(f"{self} 不良率分析出错: {str(e)}", exc_info=True)
            return None

    # ------------------------------
    # 封装分析方法：不良率趋势分析
    # ------------------------------
    def analyze_equip_diff(self, workdt, oper):
        """
        分析该产品在特定工序所有设备上的不良数据是否存在显著差异
        :param workdt: 分析日期（格式：'YYYYMMDD'）
        :param oper: 目标工序（如'5600'）
        :return: 分析结果字典，包含卡方检验结果和结论
        """
        self.logger.info(f"\n===== 开始分析 {self} 在工序 {oper} 于 {workdt} 的设备差异 =====")
        try:
            # 1. 构建参数化SQL查询（与analyze_fail_rate保持一致的风格）
            query = text("""
                select 
                    dy.main_equip_id as 设备名,
                    sum(dy.in_qty) as 总投入,
                    sum(dy.in_qty) - sum(dy.out_qty) as 不良数,
                    sum(dy.out_qty) as 合格数
                from cmsalpha.db_yielddetail dy 
                join modulemte.db_deviceinfo dd on dy.device = dd.Device 
                where 
                    dy.oper_old = :oper 
                    and dy.workdt = :workdt 
                    and dd.Product_Mode = :pm 
                    and dd.Tech_Name = :tn 
                    and dd.Die_Density = :dd 
                    and dd.Product_Density = :pd 
                    and dd.Module_Type = :mt 
                    and dy.grade = :g 
                group by dy.main_equip_id
                having sum(dy.in_qty) > 0  # 排除无投入的设备
                order by 设备名 asc;
            """)

            # 2. 复用数据库连接逻辑（与现有方法一致）
            conn_str = self._get_conn_str()
            with create_engine(conn_str).connect() as conn:
                equip_df = pd.read_sql(
                    query.bindparams(
                        oper=oper,
                        workdt=workdt,
                        pm=self.product_mode,
                        tn=self.tech_name,
                        dd=self.die_density,
                        pd=self.product_density,
                        mt=self.module_type,
                        g=self.grade
                    ),
                    conn
                )

            # 3. 数据校验（与现有方法的空数据处理逻辑一致）
            if equip_df.empty:
                self.logger.warning(f"{self} 在工序 {oper} 于 {workdt} 无设备生产数据")
                return None

            # 4. 卡方检验准备：构建观察频数矩阵（不良数+合格数）
            observed = equip_df[['不良数', '合格数']].values
            if observed.shape[0] < 2:
                self.logger.warning(f"设备数量不足（仅{observed.shape[0]}台），无法进行差异分析")
                return None

            # 5. 执行卡方检验（判断设备间差异）
            from scipy.stats import chi2_contingency
            chi2, p_value, dof, expected = chi2_contingency(observed)

            # 6. 计算设备不良率并排序（辅助分析）
            equip_df['不良率(%)'] = np.where(
                equip_df['总投入'] > 0,
                (equip_df['不良数'] / equip_df['总投入']) * 100,
                0
            ).round(4)
            equip_sorted = equip_df.sort_values('不良率(%)', ascending=False)

            # 7. 趋势结论生成（参考analyze_fail_rate的结论格式）
            alpha = 0.05  # 显著性水平与现有方法保持一致
            significant = p_value < alpha
            conclusion = f"{self} 在工序 {oper} 的设备间不良率"
            if significant:
                conclusion += f"存在显著差异（卡方值={chi2:.4f}，p值={p_value:.4f}，自由度={dof}），" \
                              f"表现最差设备为 {equip_sorted.iloc[0]['设备名']}（不良率 {equip_sorted.iloc[0]['不良率(%)']}%）"
            else:
                conclusion += f"无显著差异（卡方值={chi2:.4f}，p值={p_value:.4f}，自由度={dof}）"

            # 8. 保存结果（与现有方法的结果存储结构一致）
            self.equip_diff_result = {
                'conclusion': conclusion,
                'statistic': {
                    'chi2': round(chi2, 4),
                    'p_value': round(p_value, 4),
                    'dof': dof,
                    'significant': significant
                },
                'equipment_data': equip_sorted.to_dict('records'),
                'worst_equip': equip_sorted.iloc[0].to_dict() if not equip_sorted.empty else None
            }

            # 9. 日志输出（保持与现有方法一致的日志级别和格式）
            self.logger.info(self.equip_diff_result['conclusion'])
            self.logger.debug(
                f"设备详细数据:\n{equip_sorted[['设备名', '总投入', '不良数', '不良率(%)']].to_string(index=False)}")
            return self.equip_diff_result

        except Exception as e:
            self.logger.error(f"{self} 设备差异分析出错: {str(e)}", exc_info=True)
            return None

    # 辅助方法：生成数据库连接字符串
    # ------------------------------
    def _get_conn_str(self):
        """封装连接字符串生成逻辑，避免重复代码"""
        return (
            f"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}@"
            f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}?"
            f"charset={self.db_config['charset']}"
        )


# 获取Daily Fail Status
def get_fail_ttl(db_config, workdt, operList, logger):
    # 从db_failstatus中获取fail status指标
    query = text("""
            select oper_old, 
                   (1 - sum(out_qty) / sum(in_qty)) * 100 as fail 
            from cmsalpha.db_yielddetail dy 
            where workdt = :work_date and oper_old in :operList
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
            df = pd.read_sql(query.bindparams(work_date=workdt, operList=tuple(operList)), conn)

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


# 主函数
def main(mode):
    # 初始化日志记录器
    log_path = "C:/Users/Tengjun Zhao/Desktop"
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
            logger = setup_logger(
                log_path=log_path,
                log_name=log_name,
                file_level=logging.DEBUG,
                console_level=logging.DEBUG
            )
            logger.info("使用测试环境数据库(localhost)")
        else:
            db_config['host'] = '172.27.154.57'
            logger = setup_logger(
                log_path=log_path,
                log_name=log_name,
                file_level=logging.INFO,
                console_level=logging.INFO
            )
            logger.info("使用生产环境数据库(172.27.154.57)")
        logger.info("程序开始运行")
        # 作业时间计算
        yesterday_date = datetime.now() - timedelta(days=1)
        yesterday = yesterday_date.strftime('%Y%m%d')
        dt_semiYear_date = yesterday_date - dateutil.relativedelta.relativedelta(months=6)
        dt_semiYear_date = dt_semiYear_date.replace(day=1)
        dt_semiYear = dt_semiYear_date.strftime('%Y%m%d')

        logger.info(f"计算日期: 昨天={yesterday}, 半年前月初={dt_semiYear}")

        # Step1： 获取Daily Fail Status TTL
        logger.info("Step1: 开始获取每日Fail Status数据")
        operList = ['5600', '5710', '5700', '5780']
        df_ttl = get_fail_ttl(db_config, yesterday, operList, logger)

        # Step2：逐个工程查询半年度数据
        logger.info("Step2: 开始处理半年度数据查询")
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
                logger.info("Step3: 分析历史数据UCL，LCL并与日别数据比较")
                result = compare_with_control_limits(df_ttl, oper, LCL, UCL, logger)
                results.append(result)
                # Step4. 分析异常工序
                if result['status'] != '正常':
                    logger.info("Step4: 确定主要不良产品")
                    full_df, vital_df = analyze_abnormal_oper(
                        db_config=db_config,
                        oper=oper,
                        workdt=yesterday,  # 异常日期
                        logger=logger
                    )
                    # Step5. 针对主要不良产品分析产品占比变化（排查产品结构变化导致的）
                    if not vital_df.empty:
                        product_attrs = ['Product_Mode', 'Tech_Name', 'Die_Density',
                                         'Product_Density', 'Module_Type', 'grade']
                        # 遍历每个主要不良产品，创建Product对象并执行分析
                        for _, product_row in vital_df.iterrows():
                            product_info = {attr: product_row[attr] for attr in product_attrs}
                            # 实例化产品对象（封装属性和依赖）
                            product = Product(
                                product_info=product_info,
                                db_config=db_config,
                                logger=logger
                            )
                            logger.info(f"\n----- 开始处理 {product} -----")

                            # 调用对象方法执行分析（无需传递产品属性参数）
                            # Step5：占比趋势分析
                            logger.info("Step5: 分析产品占比变化趋势")
                            product.analyze_proportion(
                                oper=oper,
                                dt_semiYear=dt_semiYear,
                                yesterday=yesterday,
                                trend_threshold=0.02
                            )

                            # Step6：不良率趋势分析
                            logger.info("Step6: 分析产品半年度不良率趋势")
                            product.analyze_fail_rate(
                                oper=oper,
                                dt_semiYear=dt_semiYear,
                                yesterday=yesterday,
                                stability_threshold=0.3
                            )
                            # Step7: 分析产品在该工序设备别不良率
                            logger.info("Step7: 分析产品在该工序设备别不良率")
                            product.analyze_equip_diff(workdt=yesterday, oper=oper)

                            logger.info(f"----- {product} 分析完成 -----\n")
                    else:
                        logger.warning(f"工序 [{oper}] 无主要不良产品，跳过产品分析")
                else:
                    logger.info(f"===== 工序 [{oper}] 无需分析 =====\n")
                logger.info(f"===== 工序 [{oper}] 处理完成 =====\n")  # 工序结束符
        else:
            logger.warning("未获取到任何每日Fail Status")
        logger.info("程序运行结束")

    except Exception as e:
        logger.critical(f"程序主流程出错: {str(e)}", exc_info=True)


if __name__ == '__main__':
    main('test')
