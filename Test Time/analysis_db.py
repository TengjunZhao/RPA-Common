import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from datetime import datetime

# 数据库连接配置
db_config = {
    'host': 'localhost',
    'user': 'remoteuser',
    'password': 'password',
    'database': 'cmsalpha'
}

def get_lot_ids():
    # 连接到数据库
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    # 获取最大的 workdt
    cursor.execute("SELECT MAX(workdt) FROM cmsalpha.db_hibsr_at")
    max_workdt = cursor.fetchone()[0]

    # 查询每种 device, fab, grade 的一个 lot_id
    query = """
        SELECT lot_id 
        FROM cmsalpha.db_hibsr_at 
        WHERE workdt = %s 
        GROUP BY device, fab, grade
    """
    cursor.execute(query, (max_workdt,))
    results = cursor.fetchall()

    lot_ids = [row[0] for row in results]

    # 关闭数据库连接
    cursor.close()
    connection.close()

    return lot_ids, max_workdt

def analyze_test_time(lot_ids):
    # 连接到数据库
    connection = pymysql.connect(**db_config)

    lot_id_str = ','.join([f"'{lot_id}'" for lot_id in lot_ids])

    query = f"""
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
        dts.lot_id IN ({lot_id_str}) 
        AND dts.oper IN ('5600','5700', '5710');
    """

    df = pd.read_sql(query, connection)

    # 逐个产品类别和工序进行分析
    product_groups = df.groupby(['Product_Mode', 'Tech_Name', 'Die_Density', 'Product_Density', 'Module_Type', 'oper'])

    results = []

    for (product_mode, tech_name, die_density, product_density, module_type, oper), group in product_groups:
        print(f"Analyzing {product_mode} {tech_name} {die_density} {product_density} {module_type} {oper}")

        # 统计描述
        desc = group.groupby('m_table')['test_time'].describe().to_dict()
        print(desc)

        # 正态性检验
        normality = {}
        for m_table, sub_group in group.groupby('m_table'):
            k2, p = stats.normaltest(sub_group['test_time'])
            normality[m_table] = p

        # CPK计算
        cpk_values = {}

        def calculate_cpk(series, lsl, usl):
            mean = series.mean()
            std = series.std()
            cpk = min((usl - mean) / (3 * std), (mean - lsl) / (3 * std))
            return cpk

        for m_table, sub_group in group.groupby('m_table'):
            lsl = sub_group['test_time'].min()  # 假设最小值为下限
            usl = sub_group['test_time'].max()  # 假设最大值为上限
            cpk = calculate_cpk(sub_group['test_time'], lsl, usl)
            cpk_values[m_table] = cpk

        results.append({
            'Product_Mode': product_mode,
            'Tech_Name': tech_name,
            'Die_Density': die_density,
            'Product_Density': product_density,
            'Module_Type': module_type,
            'oper': oper,
            'description': desc,
            'normality': normality,
            'cpk_values': cpk_values,
            'sample_size': group['m_table'].value_counts().to_dict()
        })

    # 关闭数据库连接
    connection.close()

    return results

def save_results_to_db(results, workdt):
    # 连接到数据库
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    for result in results:
        for m_table in result['description']['mean']:
            sql = """
            INSERT INTO cmsalpha.db_testtime_analysis (product_mode, tech_name, die_density, product_density, module_type, oper, m_table, avg_test_time, stddev_test_time, cpk, normality_p_value, sample_size, workdt, analysis_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                avg_test_time = VALUES(avg_test_time),
                stddev_test_time = VALUES(stddev_test_time),
                cpk = VALUES(cpk),
                normality_p_value = VALUES(normality_p_value),
                sample_size = VALUES(sample_size),
                workdt = VALUES(workdt),
                analysis_date = NOW()
            """
            cursor.execute(sql, (
                result['Product_Mode'],
                result['Tech_Name'],
                result['Die_Density'],
                result['Product_Density'],
                result['Module_Type'],
                result['oper'],
                m_table,
                result['description']['mean'][m_table],
                result['description']['std'][m_table],
                result['cpk_values'][m_table],
                result['normality'][m_table],
                result['sample_size'][m_table],
                workdt
            ))

    connection.commit()

    # 关闭数据库连接
    cursor.close()
    connection.close()

def main():
    lot_ids, workdt = get_lot_ids()
    results = analyze_test_time(lot_ids)
    save_results_to_db(results, workdt)

if __name__ == '__main__':
    main()
