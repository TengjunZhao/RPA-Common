import pymysql
from datetime import datetime
import os


def main():
    tarDir = r'D:/'
    # 数据库连接配置
    db_config = {
        'host': 'localhost',
        'user': 'remoteuser',
        'password': 'password',
        'database': 'cmsalpha'
    }

    # 连接到数据库
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    # 获取最大的 workdt
    cursor.execute("SELECT MAX(workdt) FROM cmsalpha.db_hibsr_at")
    max_workdt = cursor.fetchone()[0]

    # 查询每种 device, fab, grade 的一个 lot_id
    query = """
        SELECT device, fab, grade, lot_id 
        FROM cmsalpha.db_hibsr_at 
        WHERE workdt = %s 
        GROUP BY device, fab, grade
    """
    cursor.execute(query, (max_workdt,))
    results = cursor.fetchall()

    # 生成文件名
    file_name = datetime.now().strftime("%Y%m%d") + ".txt"
    file_path = os.path.join(tarDir, file_name)
    # 写入到文件中
    with open(file_path, "w") as file:
        for row in results:
            line = f"{row[3]}\n"
            file.write(line)

    # 关闭数据库连接
    cursor.close()
    connection.close()

    print(f"Data has been written to {file_name}")


if __name__ == '__main__':
    main()
