import os
import pymysql
import pandas as pd
from datetime import datetime, timedelta


def calculate_year(week, reference_date):
    """
    Calculate the year of the given ISO week number based on a reference date.
    Handles edge cases for weeks that belong to the previous or next year.
    """
    try:
        # Convert reference date to datetime
        reference_date = datetime.strptime(reference_date, '%Y-%m-%d')

        # Construct the first day of the given ISO week
        year = reference_date.year
        first_day_of_week = datetime.strptime(f'{year}-W{int(week)}-1', "%Y-W%U-%w")

        # Adjust for the first week of the next year
        if week == '01' and reference_date.month == 12:
            year += 1
        # Adjust for the last weeks of the previous year
        elif week in ['52', '53'] and first_day_of_week.month == 1:
            year -= 1

        return str(year)
    except Exception as e:
        print(f"Error calculating year for week {week}: {e}")
        return None


def clean_data(data):
    # Replace NaN values with empty strings
    data = data.fillna('')
    # Filter rows where OPER is not 5710, 5700, or 5780
    if 'OPER' in data.columns:
        data = data[data['OPER'].isin(['5710', '5700', '5780'])]
    if 'WW' in data.columns:
        reference_date = datetime.now().strftime('%Y-%m-%d')  # Use today's date as a reference
        data['year'] = data['WW'].apply(lambda ww: calculate_year(ww, reference_date))
    return data


def read_excel(file_path):
    """Read raw Excel content and process headers."""
    # Load the raw data
    raw_data = pd.read_excel(file_path, header=None)

    # Extract headers from the first three rows
    main_header = raw_data.iloc[0]  # First row for most headers
    merged_header = raw_data.iloc[1]  # Second row for W-CP merged headers

    # Dynamically determine the range of FAIL ITEM columns
    fail_item_start = 22  # Index for column W (0-based indexing)
    fail_item_end = len(merged_header) - 1  # Find last column based on merged headers
    while fail_item_end > fail_item_start and pd.isna(merged_header[fail_item_end]):
        fail_item_end -= 1
    # Combine headers: Use merged_header for FAIL ITEM columns, fallback to main_header if NaN
    headers = [
        merged_header[i] if fail_item_start <= i <= fail_item_end and pd.notna(merged_header[i]) else main_header[i]
        for i in range(len(main_header))
    ]

    # Assign headers to the data
    data = raw_data[3:-1]  # Skip first three rows
    data.columns = headers
    data.reset_index(drop=True, inplace=True)

    return data, fail_item_start, fail_item_end


def process_fail_items(data, fail_item_start, fail_item_end):
    """Process columns 22nd to 93rd to generate the fail_item field and fail_qty."""
    # Extract column names for the FAIL ITEM range
    fail_item_columns = data.columns[fail_item_start:fail_item_end]  # Dynamically determined range

    def extract_fail_items(row):
        fail_items = [str(col) for col, value in zip(fail_item_columns, row) if value == 1]
        return ", ".join(fail_items)

    # Process fail_item field
    fail_data = data.iloc[:, fail_item_start:fail_item_end]
    print(fail_data)
    data['fail_item'] = fail_data.apply(extract_fail_items, axis=1)

    # Add fail_qty column (last column in FAIL ITEM range)
    fail_qty_column = data.columns[fail_item_end]
    data['fail_qty'] = data[fail_qty_column]

    # Drop original fail item columns (22nd to 93rd)
    data.drop(columns=fail_item_columns, inplace=True)

    return data

def map_columns(row):
    """Map Excel columns to MySQL columns."""
    column_mapping = {
        'NO': 'd_id',
        'DEVICE': 'device',
        'LOT ID': 'lot_id',
        'SYSTEM': 'system_name',
        'TABLE': 'table_name',
        'FIX': 'fix',
        'FIX_SN': 'fix_sn',
        'SEQUENCE': 'sequence',
        'PROGRAM': 'program',
        'DIMM': 'dimm',
        'RANK': 'm_rank',
        'DQ': 'dq',
        'SPD LOT ID': 'spd_lot',
        'SERIAL NO': 'sn',
        'RUN ID': 'run',
        'WAFER': 'wafer',
        'X': 'x',
        'Y': 'y',
        'BANK': 'bank',
        'ROW': 'f_row',
        'COL': 'f_col',
        'FAIL Type': 'fail_type',
        'TOTAL': 'fail_qty',
        'MOV STAGE': 'stage',
        'OPER': 'oper',
        'FAB': 'fab',
        'VDD': 'vdd',
        'SPEED': 'speed',
        'TYPE': 'type',
        'GRD': 'grade',
        'OWN': 'owner',
        'Batch Code': 'batch_code',
        '종료일시': 'end_time',
        'WW': 'ww',
        'fail_item': 'item',
        'year': 'w_year'
    }
    # 创建映射后的字典
    mapped_row = {column_mapping.get(k, k): v for k, v in row.items() if column_mapping.get(k, k)}
    return mapped_row


def insert_or_update_data(data, host):
    """Insert or update data into the MySQL database."""
    connection = pymysql.connect(**host)
    cursor = connection.cursor()

    insert_query = """
    INSERT INTO db_abnormal_fault (
        d_id, device, lot_id, system_name, table_name, fix, fix_sn, sequence, program, 
        dimm, m_rank, dq, spd_lot, sn, run, wafer, x, y, bank, f_row, f_col, fail_type, item, stage,
        oper, fab, vdd, speed, type, grade, owner, batch_code, 
        end_time, ww, w_year
    ) VALUES (
         %(d_id)s, %(device)s, %(lot_id)s, %(system_name)s, %(table_name)s, %(fix)s, %(fix_sn)s, 
        %(sequence)s, %(program)s, %(dimm)s, %(m_rank)s, %(dq)s, %(spd_lot)s, %(sn)s, 
        %(run)s, %(wafer)s, %(x)s, %(y)s, %(bank)s, %(f_row)s, %(f_col)s, %(fail_type)s,
        %(item)s, %(stage)s, %(oper)s, %(fab)s, %(vdd)s, %(speed)s, %(type)s, 
        %(grade)s, %(owner)s, %(batch_code)s, %(end_time)s, %(ww)s, %(w_year)s
    )
    ON DUPLICATE KEY UPDATE
        device = VALUES(device),
        lot_id = VALUES(lot_id),
        system_name = VALUES(system_name),
        table_name = VALUES(table_name),
        fix = VALUES(fix),
        fix_sn = VALUES(fix_sn),
        sequence = VALUES(sequence),
        program = VALUES(program),
        dimm = VALUES(dimm),
        m_rank = VALUES(m_rank),
        dq = VALUES(dq),
        spd_lot = VALUES(spd_lot),
        run = VALUES(run),
        wafer = VALUES(wafer),
        x = VALUES(x),
        y = VALUES(y),
        bank = VALUES(bank),
        f_row = VALUES(f_row),
        f_col = VALUES(f_col),
        fail_type = VALUES(fail_type),
        item = VALUES(item),
        stage = VALUES(stage),
        oper = VALUES(oper),
        fab = VALUES(fab),
        vdd = VALUES(vdd),
        speed = VALUES(speed),
        type = VALUES(type),
        grade = VALUES(grade),
        owner = VALUES(owner),
        batch_code = VALUES(batch_code),
        ww = VALUES(ww),
        w_year = VALUES(w_year);
    """
    for _, row in data.iterrows():
        mapped_row = map_columns(row.to_dict())
        print("Mapped Row:", mapped_row)  # 打印每一行数据字典
        try:
            cursor.execute(insert_query, mapped_row)
        except Exception as e:
            print("Error executing SQL:")
            print(cursor.mogrify(insert_query, mapped_row))  # 这将打印完整的 SQL 语句和替换后的值
            raise e

    connection.commit()
    cursor.close()
    connection.close()


def main(mode):
    # 初始化基本参数：sourceDir：excel路径；host：数据库连接信息
    if mode == 'test':
        sourceDir = r'D:\Sync\临时存放\source.xlsx'
        host = {
            'host': 'localhost',
            'user': 'remoteuser',
            'password': 'password',
            'database': 'cmsalpha'
        }
    else:
        sourceDir = r'\\172.27.7.188\Mod_TestE\19. AT Abnormal Fail\source.xlsx'
        host = {
            'host': '172.27.154.57',
            'user':'remoteuser',
            'password': 'password',
            'database': 'cmsalpha'
        }
    # Step 1: Read raw Excel content
    data, fail_item_start, fail_item_end = read_excel(sourceDir)

    # Step 2: Process fail items (22nd to 93rd columns) and fail_qty
    processed_data = process_fail_items(data, fail_item_start, fail_item_end)

    # Step 3: Clean data
    cleaned_data = clean_data(processed_data)

    # Step 4: Write to (update) database
    insert_or_update_data(cleaned_data, host)

    # os.remove(sourceDir)


if __name__ == '__main__':
    main('test')