import pymysql


def get_db(host, db):
    con = pymysql.connect(host=host['host'],
                          user=host['user'],
                          password=host['password'],
                          database=host['database'])
    cursor = con.cursor()
    sql = "SELECT MAX(workdt) FROM `{}`".format(db)
    cursor.execute(sql)
    return cursor.fetchone()[0]

def main():
    host = {
        'host': 'localhost',
        'user': 'remoteuser',
        'password': 'password',
        'database': 'cmsalpha',
    }
    result = get_db(host, 'db_lyld')
    print(result)
    return result


if __name__ == "__main__":
    main()