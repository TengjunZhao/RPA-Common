import pymysql
import time
import os
import zipfile
import datetime


def Backup_db(backup_dir, info, db, time_str):
    host = info['host']
    user = info['user']
    password = info['password']

    try:
        with pymysql.connect(host=host, user=user, password=password, database=db) as con, con.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]

            backup_file_path = os.path.join(backup_dir, f'{db}_{time_str}.sql')
            with open(backup_file_path, 'w', encoding='utf-8') as backup_file:
                # 字符集初始化
                backup_file.write("SET NAMES utf8mb4;\n")
                backup_file.write("SET character_set_client = utf8mb4;\n")
                backup_file.write("SET character_set_connection = utf8mb4;\n\n")

                for table in tables:
                    # 获取并处理表结构
                    cursor.execute(f"SHOW CREATE TABLE {table}")
                    create_table_sql = cursor.fetchone()[1]

                    # 修复：先替换排序规则
                    create_table_sql = create_table_sql.replace("utf8mb4_0900_ai_ci", "utf8mb4_unicode_ci")

                    # 修复：精确替换字符集（只替换utf8，避免重复替换utf8mb4）
                    if "CHARSET=utf8" in create_table_sql:
                        create_table_sql = create_table_sql.replace("CHARSET=utf8", "CHARSET=utf8mb4")

                    # 双重保险：移除可能的重复mb4（如utf8mb4mb4）
                    create_table_sql = create_table_sql.replace("utf8mb4mb4", "utf8mb4")

                    backup_file.write(f"DROP TABLE IF EXISTS `{table}`;\n")
                    backup_file.write(f"{create_table_sql};\n\n")

                    # 处理表数据
                    cursor.execute(f"SELECT * FROM {table}")
                    table_data = cursor.fetchall()
                    if table_data:
                        # 获取字段名
                        cursor.execute(f"DESCRIBE {table}")
                        columns = [col[0] for col in cursor.fetchall()]
                        columns_str = ", ".join([f"`{col}`" for col in columns])

                        # 处理每行数据
                        for row in table_data:
                            values = []
                            for value in row:
                                if value is None:
                                    values.append("NULL")
                                elif isinstance(value, (datetime.datetime, datetime.date)):
                                    # 日期时间格式化（外层用双引号，内层用单引号）
                                    values.append(f'"{value.strftime("%Y-%m-%d %H:%M:%S")}"')
                                elif isinstance(value, str):
                                    # 字符串转义（外层用双引号，内层单引号转义）
                                    escaped_value = value.replace("'", "\\'")
                                    values.append(f'"{escaped_value}"')
                                elif isinstance(value, (int, float, bool)):
                                    # 数字和布尔值
                                    values.append(str(value).lower() if isinstance(value, bool) else str(value))
                                else:
                                    # 其他类型（先转字符串再转义）
                                    str_value = str(value).replace("'", "\\'")
                                    values.append(f'"{str_value}"')
                            # 拼接INSERT语句
                            backup_file.write(f"INSERT INTO `{table}` ({columns_str}) VALUES ({', '.join(values)});\n")
                        backup_file.write("\n")

            print(f"数据库 {db} 导出成功!")
            return backup_file_path

    except pymysql.Error as e:
        print(f"数据库 {db} 备份失败: {str(e)}")
        return None


def compress(backup_dir, db, time_str):
    backup_file = os.path.join(backup_dir, f'{db}_{time_str}.sql')
    compressed_file = os.path.join(backup_dir, f'{db}_{time_str}.zip')
    if not os.path.exists(backup_file):
        print(f"备份文件 {backup_file} 不存在，跳过压缩")
        return

    with zipfile.ZipFile(compressed_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(backup_file, os.path.basename(backup_file))
    os.remove(backup_file)
    print(f"备份文件 {compressed_file} 压缩完成!")


def delete_history(num, dir_path):
    if not os.path.exists(dir_path):
        print(f"目录 {dir_path} 不存在，跳过删除历史文件")
        return

    current_date = datetime.datetime.now()
    expire_date = current_date - datetime.timedelta(days=num)
    zip_files = [f for f in os.listdir(dir_path) if f.endswith(".zip")]

    for zip_file in zip_files:
        file_name = os.path.splitext(zip_file)[0]
        date_part = file_name.split("_")[-1]
        try:
            file_date = datetime.datetime.strptime(date_part, "%Y%m%d")
            if file_date < expire_date:
                os.remove(os.path.join(dir_path, zip_file))
                print(f"已删除过期文件: {zip_file}")
        except ValueError:
            print(f"忽略无法解析日期的文件: {zip_file}")
    print("历史文件清理完成")


def main():
    db_info = {
        'host': 'localhost',
        'user': 'remoteuser',  # 替换为实际用户名
        'password': 'password'  # 替换为实际密码
    }
    backup_dir = r'D:/'  # 备份保存路径
    db_list = [ 'modulemte']  # 需要备份的数据库
    timestamp = time.strftime('%Y%m%d')

    for db in db_list:
        Backup_db(backup_dir, db_info, db, timestamp)
        compress(backup_dir, db, timestamp)

    delete_history(30, backup_dir)


if __name__ == '__main__':
    main()
