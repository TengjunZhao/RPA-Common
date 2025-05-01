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
        # 执行 SQL 查询获取最新的 workdt
        query = "SELECT MAX(workdt) as latest_workdt FROM db_lotcheck"
        result = pd.read_sql(query, engine)
        latest_workdt = result['latest_workdt'].values[0]
        return latest_workdt
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        # 关闭数据库连接
        engine.dispose()


# 获取最新的lot list
def getLotList(engine, start, end):
    try:
        query = (f"SELECT workdt, device, fab, owner, grade, lot_id,"
                 f"oper_old, trans_time, in_qty, out_qty FROM db_yielddetail WHERE workdt BETWEEN '{start}' AND '{end}'"
                 f"and in_qty <> out_qty;"
                 )
        result = pd.read_sql(query, engine)
        return result
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        # 关闭数据库连接
        engine.dispose()


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
    # 确认所有需要查询的PDA信息
    query = (f"SELECT dl.fab, dl.oper, dl.grade, dl.owner, dl.device, "
             f"dd.Product_Mode, dd.Module_Type, dd.Product_Density, dd.Die_Density, dd.Tech_Name "
             f"FROM db_lotcheck dl "
             f"JOIN modulemte.db_deviceinfo dd ON dl.device = dd.Device "
             f"WHERE pda_check is NULL "
             f"GROUP BY fab, oper, grade, owner, Product_Mode, Module_Type, Tech_Name, Die_Density, Module_Density;")
    pdaCondition = pd.read_sql(query, engine)

    # 初始化结果列
    pdaCondition['low_yield_reverse'] = None
    pdaCondition['ext_low_yield_reverse'] = None

    # 逐行对比lot信息与PDA信息
    for index, row in pdaCondition.iterrows():
        mode = row['Product_Mode']
        fab = row['fab']
        oper = row['oper']
        grade = row['grade']
        owner = row['owner']
        type = row['Module_Type']
        tech = row['Tech_Name']
        pkg = row['Die_Density']
        density = row['Product_Density']

        # 优先查找完全匹配的记录
        query = (f"""
            SELECT low_yield_reverse, ext_low_yield_reverse 
            FROM db_pda dp 
            WHERE prodtype = '{mode}' 
              AND fab = '{fab}' 
              AND oper = '{oper}' 
              AND grade = '{grade}'
              AND owner = '{owner}' 
              AND module_type = '{type}'
              AND tech = '{tech}' 
              AND pkg_density = '{pkg}' 
              AND module_density = '{density}'
            LIMIT 1;
        """)
        pda = pd.read_sql(query, engine)

        # 如果没有完全匹配的记录，查找owner为空的记录
        if pda.empty:
            query = (f"""
                SELECT low_yield_reverse, ext_low_yield_reverse 
                FROM db_pda dp 
                WHERE prodtype = '{mode}' 
                  AND fab = '{fab}' 
                  AND oper = '{oper}' 
                  AND grade = '{grade}'
                  AND (owner IS NULL OR owner = '') 
                  AND module_type = '{type}'
                  AND tech = '{tech}' 
                  AND pkg_density = '{pkg}' 
                  AND module_density = '{density}'
                LIMIT 1;
            """)
            pda = pd.read_sql(query, engine)

        # 如果找到记录，更新到pdaCondition中
        if not pda.empty:
            pdaCondition.at[index, 'low_yield_reverse'] = pda.iloc[0]['low_yield_reverse']
            pdaCondition.at[index, 'ext_low_yield_reverse'] = pda.iloc[0]['ext_low_yield_reverse']
    return pdaCondition

# 获取Lotcheck表中所有待查数据
def getCheckLot(engine):
    query = (f"SELECT workdt, device, fab, oper, grade, owner, lot, transtime, yield "
             f"FROM db_lotcheck "
             f"WHERE pda_check is NULL;")
    df = pd.read_sql(query, engine)
    return df

# 对比Lot list与PDA
def comparePDA(engine, df, pda):
    # 遍历df中的每一行
    for index, row in df.iterrows():
        # 从df中获取lot属性
        fab = row['fab']
        oper = row['oper']
        grade = row['grade']
        owner = row['owner']
        device = row['device']
        mYield = row['yield']
        lot = row['lot']
        transTime = row['transtime']
        # 根据Device信息获取其他属性
        query = (f"SELECT Product_Mode, Tech_Name, Die_Density, Product_Density, Module_Type "
                 f"FROM modulemte.db_deviceinfo WHERE device = '{device}'")
        res = pd.read_sql(query, engine)
        if len(res) == 0:
            continue
        mode = res['Product_Mode'].iloc[0]
        tech = res['Tech_Name'].iloc[0]
        pkg = res['Die_Density'].iloc[0]
        density = res['Product_Density'].iloc[0]
        type = res['Module_Type'].iloc[0]
        # 查找匹配的PDA条件
        match = pda[
            (pda['fab'] == fab)&
            (pda['oper'] == oper) &
            (pda['grade'] == grade) &
            (pda['owner'] == owner) &
            (pda['Product_Mode'] == mode)&
            (pda['Module_Type'] == type) &
            (pda['Tech_Name'] == tech) &
            (pda['Die_Density'] == pkg) &
            (pda['Product_Density'] == density)
        ]
        if len(match) == 0:
            match = pda[
                (pda['fab'] == fab) &
                (pda['oper'] == oper) &
                (pda['grade'] == grade) &
                (pda['Product_Mode'] == mode) &
                (pda['Module_Type'] == type) &
                (pda['Tech_Name'] == tech) &
                (pda['Die_Density'] == pkg) &
                (pda['Product_Density'] == density)
            ]
        # result = 0: 正常，1: lyld，2: eyld
        if mYield >= match['low_yield_reverse'].values[0]:
            result = 0
        elif mYield > match['ext_low_yield_reverse'].values[0]:
            result = 1
        else:
            result = 2
        # 更新db_lotcheck中pda_check字段
        update_query = f"""
            UPDATE db_lotcheck
            SET pda_check = {result}
            WHERE 
              lot = '{lot}'
              AND oper = '{oper}'
              AND transtime = '{transTime}';
        """
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(update_query))
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
    main('test')