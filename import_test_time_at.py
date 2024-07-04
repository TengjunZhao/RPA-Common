import pandas as pd
import pymysql
from datetime import datetime, timedelta
import os


def read_xls(path):
    # 尝试读取Excel文件
    try:
        # 跳过文件中的非数据行，这需要您根据实际文件调整skiprows的值
        df = pd.read_excel(path, skiprows=13)  # 示例中跳过前10行
    except Exception as e:
        print(f"读取Excel文件时发生错误: {e}")
        exit()
    known_columns = [
        "Device", "Lot No", "Oper\n(from)", "Trans Date", "Equipment 1",
        "장비모델", "PGM1", "TIME1", "Sap Code", "Owner", "Grade", "Fab"
    ]
    # 从df_corrected中选择这些列，忽略不存在的列
    df = df.loc[:, df.columns.isin(known_columns)]
    # 定义替换规则
    replacements = [
        ('Device', 'device'),
        ('Lot No', 'lot_id'),
        ('Fab', 'fab'),
        ('Oper\n(from)', 'oper_old'),
        ('Trans Date', 'trans_time'),
        ('Equipment 1', 'main_equip_id'),
        ('장비모델', 'equip_model'),
        ('PGM1', 'pgm'),
        ('TIME1', 'test_time'),
        ('Sap Code', 'release_no'),
        ('Owner', 'owner'),
        ('Grade', 'grade')
        # 可以继续添加其他替换规则
    ]
    # 应用替换规则到列名
    new_columns = []
    for column in df.columns:
        for old, new in replacements:
            column = column.replace(old, new)
        new_columns.append(column)
    # 更新 DataFrame 列名
    df.columns = new_columns
    # 将 NaN 替换为 ""
    df.fillna("", inplace=True)
    # 调整个字段数据类型
    df['trans_time'] = pd.to_datetime(df['trans_time'])
    df['workdt'] = df['trans_time'].apply(calculate_workdt)
    df['test_time'] = pd.to_numeric(df['test_time'], errors='coerce')
    return df


# 计算workdt字段
def calculate_workdt(occurred_date):
    if pd.isnull(occurred_date):
        return None
    occurred_datetime = pd.to_datetime(occurred_date)
    if occurred_datetime.time() < datetime.strptime('07:00:00', '%H:%M:%S').time():
        workdt = occurred_datetime - timedelta(days=1)
    else:
        workdt = occurred_datetime
    return workdt.strftime('%Y%m%d')


def import_data(df):
    # 数据库连接配置，请根据你的实际情况调整
    connection = pymysql.connect(host='localhost',
                                 user='remoteuser',
                                 password='password',
                                 database='cmsalpha',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            # 定义 SQL 插入语句
            sql = """
                INSERT INTO db_hibsr_at (
                    `device`, `fab`, `owner`, `grade`, `lot_id`, 
                    `oper_old`, `trans_time`, `main_equip_id`,`equip_model`,
                    `pgm`, `test_time`, `release_no`, `workdt`
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    `workdt` = VALUES(`workdt`), `device` = VALUES(`device`), `fab` = VALUES(`fab`),
                    `oper_old` = VALUES(`oper_old`), `trans_time` = VALUES(`trans_time`),
                    `main_equip_id` = VALUES(`main_equip_id`), `equip_model` = VALUES(`equip_model`),
                    `pgm` = VALUES(`pgm`), `test_time` = VALUES(`test_time`), `release_no` = VALUES(`release_no`), 
                    `owner` = VALUES(`owner`), `grade` = VALUES(`grade`);
                """
            # 将DataFrame中的数据转换为用于插入的元组列表
            records = [tuple(x) for x in df.to_numpy()]

            try:
                # 循环插入每行数据
                for record in records:
                    if has_none(record):
                        cursor.execute(sql, record)
                    # print(sql)
                connection.commit()  # 提交事务
                print("数据成功插入或更新到MySQL数据库。")
            except Exception as e:
                print(f"插入数据时发生错误: {e}")

def has_none(tup):
    return not any(element is None for element in tup)

def main():
    # Excel文件路径
    # 便利目录下的xlsx文件
    target_dir = r'C:\Users\Tengjun Zhao\Desktop\新建文件夹'
    for file in os.listdir(target_dir):
        if file.endswith('.xlsx'):
            file_path = os.path.join(target_dir, file)
            data = read_xls(file_path)
            import_data(data)
            # os.remove(file_path)

if __name__ == '__main__':
    main()
    os.system('pause')