import pymysql
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
from datetime import datetime
import numpy as np

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
        SELECT lot_id FROM (
            SELECT lot_id, device, fab, grade,
                   ROW_NUMBER() OVER (PARTITION BY device, fab, grade ORDER BY lot_id) as row_num
            FROM cmsalpha.db_hibsr_at
            WHERE workdt = %s
        ) subquery
        WHERE row_num = 1;
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
    cursor = connection.cursor()

    lot_id_str = ','.join([f"'{lot_id}'" for lot_id in lot_ids])

    query = f"""
    SELECT 
        dd.Product_Mode,
        dd.Tech_Name,
        dd.Die_Density,
        dd.Product_Density,
        dd.Module_Type,
        dts.oper,
        dts.model,
        SUBSTRING(dts.table_id, 1, 5) AS m_table,
        dts.pgm,
        dts.serial_no,
        dts.test_time
    FROM 
        cmsalpha.db_test_scatter dts
    JOIN 
        modulemte.db_deviceinfo dd ON dts.device = dd.Device 
    WHERE  
        dts.lot_id IN ({lot_id_str}) 
        AND dts.oper IN ('5600','5700', '5710')
        AND dts.result = 'P';
    """

    df = pd.read_sql(query, connection)

    # 逐个产品类别和工序进行分析
    product_groups = df.groupby(['Product_Mode', 'Tech_Name', 'Die_Density',
                                 'Product_Density', 'Module_Type', 'oper', 'model', 'm_table', 'pgm'])

    results = []

    for (product_mode, tech_name, die_density, product_density, module_type, oper, model, m_table, pgm), group in product_groups:
        print(f"Analyzing {product_mode} {tech_name} {die_density} {product_density} {module_type} {oper} {model} {m_table} {pgm}")

        # 统计描述
        try:
            desc = group['test_time'].describe().to_dict()
            min_val = desc.get('min', None)
            max_val = desc.get('max', None)
            q1 = desc.get('25%', None)
            q2 = desc.get('50%', None)
            q3 = desc.get('75%', None)
            mean_val = desc.get('mean', None)
            stddev_val = desc.get('std', None)
        except Exception as e:
            print(f"Error in describe: {e}")
            min_val = max_val = q1 = q2 = q3 = mean_val = stddev_val = None

        # 正态性检验
        normality = {}
        try:
            if len(group) >= 8:
                k2, p = stats.normaltest(group['test_time'])
                normality[m_table] = p
            else:
                normality[m_table] = -1  # 或者其他合适的默认值
        except Exception as e:
            print(f"Error in normaltest: {e}")
            normality[m_table] = -1

        # CPK计算
        def calculate_cpk(series, lsl, usl):
            mean = series.mean()
            std = series.std()
            if std == 0:
                return -1
            cpk = min((usl - mean) / (3 * std), (mean - lsl) / (3 * std))
            return cpk

        try:
            lsl = min_val if min_val is not None else 0
            usl = max_val if max_val is not None else 1
            cpk = calculate_cpk(group['test_time'], lsl, usl)
            if np.isnan(cpk):
                cpk = -1
        except Exception as e:
            print(f"Error in CPK calculation: {e}")
            cpk = -1

        results.append({
            'Product_Mode': product_mode,
            'Tech_Name': tech_name,
            'Die_Density': die_density,
            'Product_Density': product_density,
            'Module_Type': module_type,
            'oper': oper,
            'model': model,
            'm_table': m_table,
            'pgm': pgm,
            'avg_test_time': mean_val,
            'stddev_test_time': stddev_val,
            'min': min_val,
            'max': max_val,
            'quater1': q1,
            'quater2': q2,
            'quater3': q3,
            'cpk': cpk,
            'normality_p_value': normality[m_table],
            'sample_size': group['m_table'].value_counts().to_dict().get(m_table, 0)
        })

    # 关闭数据库连接
    connection.close()

    return results

def save_results_to_db(results, workdt):
    # 连接到数据库
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    for result in results:
        print(result)  # 调试用，打印每个结果
        sql = """
        INSERT INTO cmsalpha.db_testtime_analysis (product_mode, tech_name, die_density, product_density, 
        module_type, oper, model, m_table, pgm, avg_test_time, stddev_test_time, min, max, quater1, quater2, quater3, cpk, normality_p_value, 
        sample_size, workdt, analysis_date)
        VALUES (%s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            model = VALUES(model),
            pgm = VALUES(pgm),
            avg_test_time = VALUES(avg_test_time),
            stddev_test_time = VALUES(stddev_test_time),
            min = VALUES(min),
            max = VALUES(max),
            quater1 = VALUES(quater1),
            quater2 = VALUES(quater2),
            quater3 = VALUES(quater3),
            cpk = VALUES(cpk),
            normality_p_value = VALUES(normality_p_value),
            sample_size = VALUES(sample_size),
            analysis_date = NOW()
        """
        cursor.execute(sql, (
            result['Product_Mode'],
            result['Tech_Name'],
            result['Die_Density'],
            result['Product_Density'],
            result['Module_Type'],
            result['oper'],
            result['model'],
            result['m_table'],
            result['pgm'],
            result['avg_test_time'] if result['avg_test_time'] is not None else -1,
            result['stddev_test_time'] if result['stddev_test_time'] is not None else -1,
            result['min'] if result['min'] is not None else -1,
            result['max'] if result['max'] is not None else -1,
            result['quater1'] if result['quater1'] is not None else -1,
            result['quater2'] if result['quater2'] is not None else -1,
            result['quater3'] if result['quater3'] is not None else -1,
            result['cpk'] if result['cpk'] is not None else -1,
            result['normality_p_value'] if result['normality_p_value'] is not None else -1,
            result['sample_size'],
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
