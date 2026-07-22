import pymysql


# 获取Mysql cmsalpha.db_yielddetail中所有的device
def get_all_device(host, user, password, sheet_name: str):
    conn = pymysql.connect(host=host, user=user, password=password, db='cmsalpha')
    cursor = conn.cursor()
    sql = f"select distinct(device) from {sheet_name}"
    cursor.execute(sql)
    result = cursor.fetchall()
    result = [item[0] for item in result]
    conn.close()
    return result


# 对比两个list：list， checked， 返回checked中不在list中的元素
def compare_device(list1: list, checked: list):
    res = []
    for item in list1:
        if item not in checked:
            res.append(item)
    return res


def main(mode: str):
    if mode == 'dev':
        host = 'localhost'
        user = 'remoteuser'
        password = 'password'
    else:
        host = '172.27.154.57'
        user = 'remoteuser'
        password = 'password'

    device_yield = get_all_device(host, user, password, sheet_name='cmsalpha.db_yielddetail')
    device_checked = get_all_device(host, user, password, sheet_name='modulemte.db_deviceinfo')
    unchecked = compare_device(device_yield, device_checked)
    print(unchecked)
    return unchecked


if __name__ == '__main__':
    main(mode='dev')