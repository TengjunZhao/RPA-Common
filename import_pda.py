import pandas as pd
import pymysql
from datetime import datetime, timedelta
import os


def read_xls(path):
    # 尝试读取Excel文件
    try:
        # 跳过文件中的非数据行，这需要您根据实际文件调整skiprows的值
        df = pd.read_excel(path, skiprows=12)  # 示例中跳过前12行
    except Exception as e:
        print(f"读取Excel文件时发生错误: {e}")
        exit()
    known_columns = [
        'FAB','Oper','Oper Desc', 'Grade','DataGbn', 'Owner','ProdType', 'Module Type',
        'Module Density','PKG Density',	'Tech',	'Low Yield','Low Yield Reverse', 'GRT Low Yield',
        'GRT Low Yield Reverse','Ext. Low Yield', 'Ext. Low Yield Reverse','Class Code','Min Qty', 'Flash Code',
        'Controller Type', 'History Code', 'GEN', 'No of Die', 'Update User', '   Update Time   '
    ]
    # 从df_corrected中选择这些列，忽略不存在的列
    df = df.loc[:, df.columns.isin(known_columns)]
    # 定义替换规则
    replacements = [
        ('FAB', 'fab'),
        ('Oper', 'oper'),
        ('Oper Desc', 'oper_desc'),
        ('Grade', 'grade'),
        ('DataGbn', 'datagbn'),
        ('Owner', 'owner'),
        ('ProdType', 'prodtype'),
        ('Module Type', 'module_type'),
        ('Module Density', 'module_density'),
        ('PKG Density', 'pkg_density'),
        ('Tech', 'tech'),
        ('Low Yield', 'low_yield'),
        ('Low Yield Reverse', 'low_yield_reverse'),
        ('GRT Low Yield', 'grt_low_yield'),
        ('GRT Low Yield Reverse', 'grt_low_yield_reverse'),
        ('Ext. Low Yield', 'ext_low_yield'),
        ('Ext. Low Yield Reverse', 'ext_low_yield_reverse'),
        ('Class Code', 'class_code'),
        ('Min Qty', 'min_qty'),
        ('Flash Code', 'flash_code'),
        ('Controller Type', 'controller_type'),
        ('History Code', 'history_code'),
        ('GEN', 'gen'),
        ('No of Die', 'no_of_die'),
        ('Update User', 'update_user'),
        ('   Update Time   ', 'updatetime')
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


def import_data(db_config, df):
    # 数据库连接配置，请根据你的实际情况调整
    connection = pymysql.connect(**db_config)

    with connection:
        with connection.cursor() as cursor:
            # 定义 SQL 插入语句
            sql = """
            INSERT INTO db_pda (
                `fab`, `oper`, `oper_desc`, `grade`, `datagbn`, 
                `owner`, `prodtype`, `module_type`, `module_density`,
                `pkg_density`, `tech`, `low_yield`, `low_yield_reverse`,
                `grt_low_yield`, `grt_low_yield_reverse`, `ext_low_yield`,
                `ext_low_yield_reverse`, `class_code`, `min_qty`, `flash_code`,
                `controller_type`, `history_code`, `gen`, `no_of_die`,
                `update_user`, `updatetime`
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s
            ) ON DUPLICATE KEY UPDATE
                `oper_desc` = VALUES(`oper_desc`),
                `low_yield` = VALUES(`low_yield`),
                `low_yield_reverse` = VALUES(`low_yield_reverse`),
                `grt_low_yield` = VALUES(`grt_low_yield`),
                `grt_low_yield_reverse` = VALUES(`grt_low_yield_reverse`),
                `ext_low_yield` = VALUES(`ext_low_yield`),
                `ext_low_yield_reverse` = VALUES(`ext_low_yield_reverse`),
                `class_code` = VALUES(`class_code`),
                `min_qty` = VALUES(`min_qty`),
                `flash_code` = VALUES(`flash_code`),
                `controller_type` = VALUES(`controller_type`),
                `gen` = VALUES(`gen`),
                `no_of_die` = VALUES(`no_of_die`),
                `update_user` = VALUES(`update_user`),
                `updatetime` = VALUES(`updatetime`);
            """
            # 将DataFrame中的数据转换为用于插入的元组列表
            records = [tuple(x) for x in df.to_numpy()]

            try:
                # 循环插入每行数据
                for record in records:
                    # 判断oper是否为空，如果为空则跳过插入，为非法记录
                    if record[1] != '':
                        print(record)
                        cursor.execute(sql, record)
                    # print(sql)
                connection.commit()  # 提交事务
                print("数据成功插入或更新到MySQL数据库。")
            except Exception as e:
                print(f"插入数据时发生错误: {e}")


def main(mode):
    if mode == 'test':
        db_config = {
            'host': 'localhost',
            'user': 'remoteuser',
            'password': 'password',
            'database': 'cmsalpha',
            'charset': 'utf8mb4',
            'port': 3306,
        }
        sourceDir = r'D:\Sync\临时存放\source.xlsx'
    else:
        db_config = {
            'host': '172.27.154.57',
            'user': 'remoteuser',
            'password': 'password',
            'database': 'cmsalpha',
            'charset': 'utf8mb4',
            'port': 3306,
        }
        sourceDir = r'\\172.27.7.188\Mod_TestE\22. PDA Auto\source.xlsx'
    df = read_xls(sourceDir)
    import_data(db_config, df)


if __name__ == '__main__':
    main('test')
    os.system('pause')