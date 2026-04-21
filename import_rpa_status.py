import pandas as pd
import pymysql
from datetime import datetime, timedelta
import os

def read_xls(path):
    """
    读取 Excel 文件，将数据转换为符合 db_rpa_run_status 表的 DataFrame
    """
    try:
        # 假设 Excel 第一行为列名，无额外跳行；若实际文件有固定表头前的空行，可调整 skiprows
        df = pd.read_excel(path, header=0)
    except Exception as e:
        print(f"读取 Excel 文件时发生错误: {e}")
        exit()

    # 期望的 Excel 列名（与文件中的列名完全一致）
    expected_columns = [
        '业务名称', '机器人', '运行状态', '创建人',
        '开始运行时间', '结束运行时间', '业务进度'
    ]
    # 只保留存在的列，避免因列缺失而报错
    existing_cols = [col for col in expected_columns if col in df.columns]
    df = df[existing_cols]

    # --- 重命名为数据库字段名（部分字段稍后计算）---
    rename_map = {
        '业务名称': 'name',
        '机器人': 'robot_id',
        '运行状态': 'status_text',      # 临时列，后续转为数字
        '创建人': 'created_by',
        '开始运行时间': 'begin_at',
        '结束运行时间': 'end_at'
        # '业务进度' 不导入数据库，忽略
    }
    df.rename(columns=rename_map, inplace=True)

    # --- 添加常量列 title 和空列 process_name ---
    df['title'] = 'MTE'                     # 固定值
    df['process_name'] = None               # 数据库中该字段允许 NULL

    # --- 计算 exec_time（秒数，整数）---
    # 将开始/结束时间转为 datetime 类型
    df['begin_at'] = pd.to_datetime(df['begin_at'], errors='coerce')
    df['end_at'] = pd.to_datetime(df['end_at'], errors='coerce')
    # 计算时间差（秒）
    df['exec_time'] = (df['end_at'] - df['begin_at']).dt.total_seconds().fillna(0).astype(int)
    # 若结束时间为空或早于开始时间，exec_time 置为 0 或 NULL（这里设为 0）
    df.loc[df['end_at'].isna() | df['begin_at'].isna(), 'exec_time'] = None

    # --- 状态映射：文本 → 数字 ---
    status_map = {
        '执行成功': 2,
        '执行失败': 3,
        '手动停止': 1
    }
    df['status'] = df['status_text'].map(status_map)
    # 无法映射的状态设为 NULL（数据库允许）
    df.drop('status_text', axis=1, inplace=True)

    # --- 处理空值 ---
    # 对于字符串字段，空值替换为 None（对应 MySQL 的 NULL）
    str_fields = ['name', 'robot_id', 'created_by', 'process_name']
    for field in str_fields:
        df[field] = df[field].where(pd.notna(df[field]), None)

    # 确保 datetime 字段的空值为 pd.NaT（插入时会转为 NULL）
    # 已经通过 pd.to_datetime 处理过

    # 最终只保留数据库需要的列（顺序可任意，但 VALUES 需对应）
    final_columns = [
        'title', 'name', 'process_name', 'begin_at', 'end_at',
        'exec_time', 'created_by', 'status', 'robot_id'
    ]
    df = df[final_columns]

    return df


def import_data(db_config, df):
    """
    将 DataFrame 数据插入或更新到 MySQL 表 db_rpa_run_status
    """
    connection = pymysql.connect(**db_config)
    with connection:
        with connection.cursor() as cursor:
            # 获取数据库最大begin_at日期
            max_data = cursor.execute("SELECT MAX(begin_at) FROM db_rpa_run_status;")
            max_date = cursor.fetchone()[0]
            
            # 如果数据库中没有数据，设置为最小日期
            if max_date is None:
                max_date = datetime.min

            # 注意表名和数据库名：modulemte.db_rpa_run_status
            sql = """
            INSERT INTO db_rpa_run_status (
                title, name, process_name, begin_at, end_at,
                exec_time, created_by, status, robot_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                process_name = VALUES(process_name),
                end_at = VALUES(end_at),
                exec_time = VALUES(exec_time),
                created_by = VALUES(created_by),
                status = VALUES(status);
            """
            # 将 DataFrame 转为元组列表，并将 NaN 替换为 None
            records = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]

            try:
                for record in records:
                    # 主键之一 name 不能为空，若为空则跳过（避免插入无效记录）
                    begin_at = record[3]
                    # 跳过 begin_at 为空的记录
                    if begin_at is None or pd.isna(begin_at):
                        continue
                    # 只导入比数据库最大日期更新的数据
                    if begin_at <= max_date:
                        continue
                    if record[1] is not None and str(record[1]).strip() != '':
                        cursor.execute(sql, record)
                connection.commit()
                print("数据成功插入或更新到 MySQL 数据库 (modulemte.db_rpa_run_status)。")
            except Exception as e:
                print(f"插入数据时发生错误: {e}")
                connection.rollback()


def main(mode):
    if mode == 'test':
        db_config = {
            'host': 'localhost',
            'user': 'remoteuser',
            'password': 'password',
            'database': 'modulemte',      # 修改为你的数据库名
            'charset': 'utf8mb4',
            'port': 3306,
        }
        sourceDir = r'D:\Sync\业务报告\2026 项目改善\02 【RPA】RPA运行诊断系统\业务日志.xlsx'
    else:
        db_config = {
            'host': '172.27.154.57',
            'user': 'remoteuser',
            'password': 'password',
            'database': 'modulemte',
            'charset': 'utf8mb4',
            'port': 3306,
        }
        sourceDir = r'\\172.27.7.188\Mod_TestE\27. RPA operation diagnosis system\业务日志.xlsx'

    df = read_xls(sourceDir)
    import_data(db_config, df)


if __name__ == '__main__':
    main('test')
    os.system('pause')