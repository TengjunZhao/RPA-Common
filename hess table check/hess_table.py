import os.path
import pandas as pd
import sqlalchemy as sc
import openpyxl
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler


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


# 获取数据库中前6个月的制品列表
def get_hist_prod(host, user, password, ttl_oper_list, start, end):
    engine = sc.create_engine(f'mysql+pymysql://{user}:{password}@{host}/cmsalpha')
    sql = f"""
    SELECT
        distinct device_cmf7, tech
    FROM
        db_yielddetail
    WHERE
        oper_old IN ({','.join([f"'{op}'" for op in ttl_oper_list])})
    AND workdt BETWEEN '{start}'
    AND '{end}'
    """
    return pd.read_sql(sql, engine)


# 获取HESS列表
def get_hess_list(dir, date):
    try:
        df = pd.read_excel(dir, engine='openpyxl')
        df['source_file'] = dir  # 添加来源文件标识
        print(f"成功读取: {dir}, 行数: {len(df)}")
    except Exception as e:
        print(f"读取失败 {dir}: {str(e)}")
        return pd.DataFrame()

    # 将'적용일자'字段转换为字符串类型，确保与date类型一致
    df['적용일자'] = df['적용일자'].astype(str)
    
    # 只保留'적용일자'字段小于date的数据记录，或为空值的记录
    df = df[(df['적용일자'] < date) | (df['적용일자'].isna()) | (df['적용일자'] == 'nan')]
    return  df


def main(mode):
    if mode == 'test':
        host = 'localhost'
        dir = r'D:\Sync\业务报告\2026 项目改善\03 HESS Table Auto Check功能开发\28. HESS TABLE'
    else:
        host = '172.27.154.57'
        dir = r'\\172.27.7.188\Mod_TestE\28.HESS TABLE'
    user = 'remoteuser'
    password = 'password'
    hess_table_list =['ET.xlsx', 'AT.xlsx']
    et_oper_list = ['5600', '5665', '5670', '5675']
    at_oper_list = ['5710', '5700', '5780', '5781', '5782', '5783', '5784']
    ttl_oper_list = et_oper_list + at_oper_list

    logger = setup_logger(
        log_path=dir,
        log_name='hess_table.log'
    )
    # 确认log文件是否为空，并清楚log内容
    if os.path.exists(os.path.join(dir, 'hess_table.log')):
        open(os.path.join(dir, 'hess_table.log'), 'w').close()

    # 获取日期，六个月前-当前，格式YYYYMMDD
    nowWorkdt = datetime.now()
    str_endWorkdt = nowWorkdt.strftime('%Y%m%d')
    str_startWorkdt = (nowWorkdt - timedelta(days=180)).strftime('%Y%m%d')
    logger.info(f"{str_endWorkdt}开始执行: 确认早于{str_startWorkdt}的情况")
    # 确认数据库中前6个月的制品
    prod_list = get_hist_prod(host, user, password, ttl_oper_list, str_startWorkdt, str_endWorkdt)
    logger.info(f"数据库中前6个月的制品: {len(prod_list)}")
    # 逐行输出DataFrame
    for index, row in prod_list.iterrows():
        logger.info(f"  device_cmf7: {row['device_cmf7']}, tech: {row['tech']}")

    et_dir = os.path.join(dir, hess_table_list[0])
    at_dir = os.path.join(dir, hess_table_list[1])
    et_hess = get_hess_list(et_dir, str_startWorkdt)
    at_hess = get_hess_list(at_dir, str_startWorkdt)

    # 提取et_hess dataframe中的PROD_MODE_DESC, MOD_TECH字段并去除重复值，存入新的list中
    et_prod_list = list(et_hess[['PROD_MODE_DESC', 'PROD_TECH']].drop_duplicates().itertuples(index=False, name=None))
    at_prod_list = list(at_hess[['PROD_MODE_DESC', 'MOD_TECH']].drop_duplicates().itertuples(index=False, name=None))
    
    # 将prod_list DataFrame转换为set，便于快速查找
    prod_set = set(prod_list.itertuples(index=False, name=None))
        
    # 从et_prod_list中去除在prod_list中存在的元素
    et_not_in_db = [prod for prod in et_prod_list if prod not in prod_set]
        
    # 从at_prod_list中去除在prod_list中存在的元素
    at_not_in_db = [prod for prod in at_prod_list if prod not in prod_set]
    
    # 为ET不在数据库中的制品添加计数信息
    et_hess_dup = []
    for prod in et_not_in_db:
        # 在et_hess中查找符合条件的数据条数
        count = len(et_hess[(et_hess['PROD_MODE_DESC'] == prod[0]) & (et_hess['PROD_TECH'] == prod[1])])
        et_hess_dup.append({
            'PROD_MODE_DESC': prod[0],
            'PROD_TECH': prod[1],
            'count': count
        })
    
    # 为AT不在数据库中的制品添加计数信息
    at_hess_dup = []
    for prod in at_not_in_db:
        # 在at_hess中查找符合条件的数据条数
        count = len(at_hess[(at_hess['PROD_MODE_DESC'] == prod[0]) & (at_hess['MOD_TECH'] == prod[1])])
        at_hess_dup.append({
            'PROD_MODE_DESC': prod[0],
            'MOD_TECH': prod[1],
            'count': count
        })
    
    # 生成仅包含不在数据库中制品记录的ET DataFrame
    if et_not_in_db:
        # 构建筛选条件：匹配所有不在数据库中的制品组合
        et_conditions = [(et_hess['PROD_MODE_DESC'] == prod[0]) & (et_hess['PROD_TECH'] == prod[1]) 
                        for prod in et_not_in_db]
        # 使用OR连接所有条件
        et_mask = pd.concat([pd.Series(cond) for cond in et_conditions], axis=1).any(axis=1)
        et_hess_new = et_hess[et_mask].copy()
    else:
        et_hess_new = pd.DataFrame(columns=et_hess.columns)
    
    # 生成仅包含不在数据库中制品记录的AT DataFrame
    if at_not_in_db:
        # 构建筛选条件：匹配所有不在数据库中的制品组合（注意AT使用MOD_TECH字段）
        at_conditions = [(at_hess['PROD_MODE_DESC'] == prod[0]) & (at_hess['MOD_TECH'] == prod[1]) 
                        for prod in at_not_in_db]
        # 使用OR连接所有条件
        at_mask = pd.concat([pd.Series(cond) for cond in at_conditions], axis=1).any(axis=1)
        at_hess_new = at_hess[at_mask].copy()
    else:
        at_hess_new = pd.DataFrame(columns=at_hess.columns)
    
    logger.info(f"ET HESS新DataFrame行数: {len(et_hess_new)}")
    logger.info(f"AT HESS新DataFrame行数: {len(at_hess_new)}")
    
    # 将et_hess_new写入Excel文件
    et_output_path = os.path.join(dir, 'et_hess_new.xlsx')
    et_hess_new.to_excel(et_output_path, index=False, engine='openpyxl')
    logger.info(f"ET HESS新数据已保存至: {et_output_path}")
    
    # 将at_hess_new写入Excel文件
    at_output_path = os.path.join(dir, 'at_hess_new.xlsx')
    at_hess_new.to_excel(at_output_path, index=False, engine='openpyxl')
    logger.info(f"AT HESS新数据已保存至: {at_output_path}")
        
    # 一行一个元素输出ET制品列表（不在数据库中的）
    logger.info(f"ET HESS列表中不在数据库中的制品: {len(et_hess_dup)}")
    for prod in et_hess_dup:
        logger.info(f"  {prod}")
        
    # 一行一个元素输出AT制品列表（不在数据库中的）
    logger.info(f"AT HESS列表中不在数据库中的制品: {len(at_hess_dup)}")
    for prod in at_hess_dup:
        logger.info(f"  {prod}")

    logger.info("运行结束")

if __name__ == '__main__':
    main('test')