import sys
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt


# 获取KPI指数信息: 最近一天的workdt, sigma值
def get_kpi_info(engine, kpi):
    query = text(f"SELECT workdt, sigma FROM db_kpi_describe "
                 f"WHERE data_type = '{kpi}' "
                 f"ORDER BY workdt DESC "
                 f"LIMIT 1;")
    with engine.connect() as connection:
        result = connection.execute(query).fetchone()
    return result if result else None


# 分析KPI相关指标
def analyze_kpi(engine, kpi, workdt):
    # 计算workdt：格式YYYYMMDD，前30天的wokrdt
    workdt = pd.to_datetime(workdt, format='%Y%m%d')
    start_date = workdt - pd.Timedelta(days=30)
    start_date = start_date.strftime('%Y%m%d')
    end_date = workdt.strftime('%Y%m%d')
    if kpi == 'c_yield':
        # 从db_yielddetail中获取cum yield指标
        query = text(f"SELECT workdt, SUM(in_qty) AS s_in, SUM(out_qty) AS s_out, "
                     f"ROUND(SUM(out_qty)/SUM(in_qty)*100,2) AS {kpi} "
                     f"FROM db_yielddetail "
                     f"WHERE workdt BETWEEN '{start_date}' AND '{end_date}' "
                     f"GROUP BY workdt;")
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        # 对KPI指标进行统计学分析
        stats = df[kpi].describe()
        z_scores = np.abs(stats.zscore(df[kpi]))
        pass
# 主函数
def main(mode):
    if mode == 'test':
        # 创建数据库连接
        dbAddress = 'localhost'
    else:
        dbAddress = '172.27.154.57'
    # 创建数据库连接
    engine = create_engine(f'mysql+pymysql://remoteuser:password@{dbAddress}/cmsalpha')
    kpiList = ['c_yield']
    # 设置中文字体
    plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
    plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
    for kpi in kpiList:
        # 获取cum yield指标
        kpiRes = get_kpi_info(engine, kpi)
        if kpiRes:
            # 成功搜索到指标信息后进行分析
            workdt = kpiRes.workdt
            sigma = kpiRes.sigma
            print(f"最近一天的workdt: {workdt}, sigma值: {sigma}")
            if sigma <= 3.7:
                print(f"{kpi}的sigma值低于3.5，需进行分析。")
                analyze_kpi(engine, kpi, workdt)
            else:
                print(f"{kpi}的sigma值高于3.5，无需进行分析。")
        else:
            print("未找到符合条件的指标信息。")
            sys.exit(1)


if __name__ == '__main__':
    main('test')