import pandas as pd
import re
import pymysql


def read_excel(path):
    df = pd.read_excel(path)
    df = df.iloc[:, :13]
    df.head()
    # 转换列名（简化示例，实际操作需要完整映射）
    column_mapping = {
        'Equip ID': 'Equip_ID',
        'Fun11': 'Dut11',
        'Fun12': 'Dut12',
        'Fun13': 'Dut13',
        'Fun14': 'Dut14',
        'Fun21': 'Dut21',
        'Fun22': 'Dut22',
        'Fun23': 'Dut23',
        'Fun24': 'Dut24',
        'Fun31': 'Dut31',
        'Fun32': 'Dut32',
        'Fun33': 'Dut33',
        'Fun34': 'Dut34'
    }
    df['Equip ID'] = df['Equip ID'].apply(lambda x: x.split('-')[0] if '-' in x else x)
    # 重命名列
    df = df.rename(columns=column_mapping)
    # 选择与数据库对应的列
    df = df[list(column_mapping.values())]
    # 转换百分比到浮点数（例如：'1.20%' -> 1.2）
    for col in df.columns[1:]:  # 跳过Equip_ID列
        df[col] = df[col].str.rstrip('%').astype('float') / 100
    # Remove the row where 'Equip ID' is 'TTL'
    df = df[df['Equip_ID'] != 'TTL']
    # Display the last few rows to ensure the 'TTL' row has been removed
    df.tail()
    return df


def import_data(config, df):
    conn = pymysql.connect(**config)
    try:
        with conn.cursor() as cursor:
            # 清理数据表
            cursor.execute("TRUNCATE TABLE db_retest_rt")
            # 插入数据
            for index, row in df.iterrows():
                sql = """
                INSERT INTO db_retest_rt (Equip_ID, Dut11, Dut12, Dut13, Dut14, Dut21, Dut22, Dut23, Dut24, Dut31, Dut32, Dut33, Dut34) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                row['Equip_ID'], row['Dut11'], row['Dut12'], row['Dut13'], row['Dut14'], row['Dut21'], row['Dut22'],
                row['Dut23'], row['Dut24'], row['Dut31'], row['Dut32'], row['Dut33'], row['Dut34']))
            # 提交事务
            conn.commit()
    finally:
        conn.close()


def main():
    # 读取Excel文件
    file_path = r'C:\Users\Tengjun Zhao\Desktop\Retest RT.xlsx'
    df = read_excel(file_path)
    db_config = {
        'host': 'localhost',
        'user': 'remoteuser',
        'password': 'password',
        'db': 'cmsalpha',
        'charset': 'utf8mb4',
    }
    import_data(db_config, df)


if __name__ == '__main__':
    main()