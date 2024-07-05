import pandas as pd
import pymysql
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# 数据库连接配置，请根据你的实际情况调整
connection = pymysql.connect(host='localhost',
                             user='remoteuser',
                             password='password',
                             database='cmsalpha')

# 查询数据
query = """
SELECT 
    dd.Product_Mode,
    dd.Tech_Name,
    dd.Die_Density,
    dd.Product_Density,
    dd.Module_Type,
    dts.oper,
    SUBSTRING(dts.table_id, 1, 5) AS m_table,
    dts.serial_no,
    dts.test_time
FROM 
    cmsalpha.db_test_scatter dts
JOIN 
    modulemte.db_deviceinfo dd ON dts.device = dd.Device 
WHERE  
    dts.start_time BETWEEN '2024-06-28' AND '2024-07-05' 
    AND dts.oper IN ('5600','5700', '5710');
"""

df = pd.read_sql(query, connection)

# 逐个产品类别和工序进行分析
product_groups = df.groupby(['Product_Mode', 'Tech_Name', 'Die_Density', 'Product_Density', 'Module_Type', 'oper'])

for (product_mode, tech_name, die_density, product_density, module_type, oper), group in product_groups:
    print(f"Analyzing {product_mode} {tech_name} {die_density} {product_density} {module_type} {oper}")

    plt.figure(figsize=(14, 7))
    sns.boxplot(x='m_table', y='test_time', data=group)
    plt.title(
        f'Test Time by Equipment for {product_mode} {tech_name} {die_density} {product_density} {module_type} {oper}')
    plt.xticks(rotation=45)
    plt.show()

    # 统计描述
    desc = group.groupby('m_table')['test_time'].describe()
    print(desc)

    # 正态性检验
    for m_table, sub_group in group.groupby('m_table'):
        k2, p = stats.normaltest(sub_group['test_time'])
        print(f"Normality test for {m_table}: p-value = {p}")

        if p < 0.05:
            print(f"The test time for {m_table} does not follow a normal distribution.")
        else:
            print(f"The test time for {m_table} follows a normal distribution.")


    # CPK计算
    def calculate_cpk(series, lsl, usl):
        mean = series.mean()
        std = series.std()
        cpk = min((usl - mean) / (3 * std), (mean - lsl) / (3 * std))
        return cpk


    for m_table, sub_group in group.groupby('m_table'):
        lsl = sub_group['test_time'].min()  # 假设最小值为下限
        usl = sub_group['test_time'].max()  # 假设最大值为上限
        cpk = calculate_cpk(sub_group['test_time'], lsl, usl)
        print(f"CPK for {m_table}: {cpk}")

# 关闭数据库连接
connection.close()
