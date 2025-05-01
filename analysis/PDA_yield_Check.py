import pandas as pd
import sqlalchemy
import datetime
from sqlalchemy import text

# 获取当前日期，以YYYYMMDD格式展示
def nowWorkdt():
    today = datetime.datetime.now()
    return today.strftime('%Y%m%d')


# 从db_lotcheck中获取最新的workdt
def LatestWorkdt(engine):
    try:
        query = text("SELECT MAX(workdt) as latest_workdt FROM db_lotcheck")
        with engine.connect() as conn:
            result = pd.read_sql(query, conn)
        return result['latest_workdt'].values[0]
    except Exception as e:
        print(f"Error: {e}")
        return None


# 获取最新的lot list
def getLotList(engine, start, end):
    try:
        query = text("""
            SELECT workdt, device, fab, owner, grade, lot_id,
                   oper_old, trans_time, in_qty, out_qty 
            FROM db_yielddetail 
            WHERE workdt BETWEEN :start AND :end
            AND in_qty <> out_qty
        """)
        with engine.connect() as conn:
            result = pd.read_sql(query, conn, params={'start': start, 'end': end})
        return result
    except Exception as e:
        print(f"Error: {e}")
        return None


# 写入db_lotcheck
def writeLotCheck(engine, df):
    if df is None:
        return False
    try:
        # 增加 yield = out_qty/in_qty，处理除零情况
        df['yield'] = df.apply(
            lambda x: (x['out_qty'] / x['in_qty'] * 100) if x['in_qty'] != 0 else None,
            axis=1
        )
        # 去除 in_qty, out_qty 两列
        df = df.drop(columns=['in_qty', 'out_qty'])
        df.columns = ['workdt', 'device', 'fab', 'owner', 'grade', 'lot', 'oper', 'transtime', 'yield']
        # 构建批量插入的 SQL 语句（使用参数化查询）
        columns = ', '.join(df.columns)
        placeholders = ', '.join([f':{col}' for col in df.columns])
        update_clause = ', '.join([f"{col}=VALUES({col})" for col in df.columns])
        query = text(
            f"INSERT INTO db_lotcheck ({columns}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )
        # 准备数据（转为字典列表）
        data = df.to_dict('records')
        # 使用连接上下文管理器执行批量插入
        with engine.connect() as connection:
            with connection.begin():
                # 使用 executemany 执行批量插入
                connection.execute(query, data)
        print("Data successfully written to db_lotcheck.")
        return True
    except Exception as e:
        print(f"Error writing to db_lotcheck: {e}")
        return False


# 逐行比对PDA
def checkPDA(engine):
    query = text("""
        SELECT dl.fab, dl.oper, dl.grade, dl.owner, dl.device,
               dd.Product_Mode, dd.Module_Type, dd.Product_Density, 
               dd.Die_Density, dd.Tech_Name 
        FROM db_lotcheck dl
        JOIN modulemte.db_deviceinfo dd ON dl.device = dd.Device
        WHERE pda_check is NULL
        GROUP BY fab, oper, grade, owner, Product_Mode, Module_Type, 
                 Tech_Name, Die_Density, Module_Density
    """)

    with engine.connect() as conn:
        pdaCondition = pd.read_sql(query, conn)

    pdaCondition['low_yield_reverse'] = None
    pdaCondition['ext_low_yield_reverse'] = None

    for index, row in pdaCondition.iterrows():
        params = {
            'mode': row['Product_Mode'],
            'fab': row['fab'],
            'oper': row['oper'],
            'grade': row['grade'],
            'owner': row['owner'],
            'type': row['Module_Type'],
            'tech': row['Tech_Name'],
            'pkg': row['Die_Density'],
            'density': row['Product_Density']
        }

        # 优先完全匹配
        query = text("""
            SELECT low_yield_reverse, ext_low_yield_reverse 
            FROM db_pda 
            WHERE prodtype = :mode
              AND fab = :fab
              AND oper = :oper
              AND grade = :grade
              AND owner = :owner
              AND module_type = :type
              AND tech = :tech
              AND pkg_density = :pkg
              AND module_density = :density
            LIMIT 1
        """)

        with engine.connect() as conn:
            pda = pd.read_sql(query, conn, params=params)

        if pda.empty:
            # 尝试owner为空的查询
            query = text("""
                SELECT low_yield_reverse, ext_low_yield_reverse 
                FROM db_pda 
                WHERE prodtype = :mode
                  AND fab = :fab
                  AND oper = :oper
                  AND grade = :grade
                  AND (owner IS NULL OR owner = '')
                  AND module_type = :type
                  AND tech = :tech
                  AND pkg_density = :pkg
                  AND module_density = :density
                LIMIT 1
            """)
            with engine.connect() as conn:
                pda = pd.read_sql(query, conn, params=params)

        if not pda.empty:
            pdaCondition.at[index, 'low_yield_reverse'] = pda.iloc[0]['low_yield_reverse']
            pdaCondition.at[index, 'ext_low_yield_reverse'] = pda.iloc[0]['ext_low_yield_reverse']

    return pdaCondition

# 获取Lotcheck表中所有待查数据
def getCheckLot(engine):
    query = text("""
            SELECT workdt, device, fab, oper, grade, owner, 
                   lot, transtime, yield 
            FROM db_lotcheck 
            WHERE pda_check IS NULL
        """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    return df

# 对比Lot list与PDA
def comparePDA(engine, df, pda):
    # 使用单个连接处理所有操作
    with engine.connect() as conn:
        with conn.begin():  # 开启事务
            for index, row in df.iterrows():
                # 参数化查询设备信息
                device_query = text("""
                    SELECT Product_Mode, Tech_Name, Die_Density, 
                           Product_Density, Module_Type 
                    FROM modulemte.db_deviceinfo 
                    WHERE device = :device
                """)
                res = pd.read_sql(
                    device_query,
                    conn,
                    params={'device': row['device']}
                )

                if len(res) == 0:
                    continue

                # 获取设备属性（避免使用type作为变量名）
                device_attrs = {
                    'mode': res['Product_Mode'].iloc[0],
                    'tech': res['Tech_Name'].iloc[0],
                    'pkg': res['Die_Density'].iloc[0],
                    'density': res['Product_Density'].iloc[0],
                    'module_type': res['Module_Type'].iloc[0]
                }

                # 构建匹配条件
                match_conditions = [
                    ('fab', row['fab']),
                    ('oper', row['oper']),
                    ('grade', row['grade']),
                    ('owner', row['owner']),
                    ('Product_Mode', device_attrs['mode']),
                    ('Module_Type', device_attrs['module_type']),
                    ('Tech_Name', device_attrs['tech']),
                    ('Die_Density', device_attrs['pkg']),
                    ('Product_Density', device_attrs['density'])
                ]

                # 首次匹配（包含owner）
                mask = True
                for col, val in match_conditions:
                    if col in pda.columns:
                        mask &= (pda[col] == val)
                match = pda[mask]

                # 二次匹配（不包含owner）
                if len(match) == 0:
                    mask = True
                    for col, val in match_conditions:
                        if col in pda.columns and col != 'owner':
                            mask &= (pda[col] == val)
                    match = pda[mask]

                # 计算结果
                if len(match) > 0:
                    threshold = match.iloc[0]  # 取第一条匹配记录
                    if row['yield'] >= threshold['low_yield_reverse']:
                        result = 0
                    elif row['yield'] > threshold['ext_low_yield_reverse']:
                        result = 1
                    else:
                        result = 2

                    # 参数化更新
                    update_query = text("""
                        UPDATE db_lotcheck
                        SET pda_check = :result
                        WHERE lot = :lot
                        AND oper = :oper
                        AND transtime = :transtime
                    """)
                    conn.execute(
                        update_query,
                        {
                            'result': result,
                            'lot': row['lot'],
                            'oper': row['oper'],
                            'transtime': row['transtime']
                        }
                    )
    return True


def main(mode):
    # 数据库配置
    if mode == 'test':
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://"
            f"{'remoteuser'}:"
            f"{'password'}@"
            f"{'localhost'}:{3306}/{'cmsalpha'}"
        )
    else:
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://"
            f"{'remoteuser'}:"
            f"{'password'}@"
            f"{'172.27.154.57'}:{3306}/{'cmsalpha'}"
        )
    # 获取当前日期，作为终止时间
    workdt = nowWorkdt()
    # 获取开始时间
    latestWorkdt = LatestWorkdt(engine)
    # 获取所有需要待查的lot
    df = getLotList(engine, latestWorkdt, workdt)
    # 将待查lot存入数据库
    writeLotCheck(engine, df)
    # 获取所有待确认lot
    df = getCheckLot(engine)
    # 获取lot所属的所有产品的PDA基准
    pda = checkPDA(engine)
    # 逐个lot对比PDA
    comparePDA(engine, df, pda)


if __name__ == '__main__':
    print(sqlalchemy.__version__)
    main('test')