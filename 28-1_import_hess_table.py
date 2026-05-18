import pandas as pd
import pymysql
import os
import hashlib
import json
from typing import Dict, List, Optional


# ET表的ID字段列表
ET_ID_FIELDS = [
    'FACTORY', 'FAB', 'PROD_FAMILY_DESC', 'PROD_GROUP_DESC', 'PROD_MODE_DESC',
    'SPECIAL', 'EQUIP_MODEL', 'OPER', 'MOD_TYP', 'PROD_TECH', 'PKG_DENSITY',
    'ORGANIZ', 'OWNER', 'PROD_DENSITY', 'MOD_HEIGHT', 'DIE_TYPE', 'DIE_QTY',
    'VERSION', 'GRADE', 'MOD_SECTION', 'MASK_REV', 'SAP_HIST', 'CUST_INFO'
]

# AT表的ID字段列表
AT_ID_FIELDS = [
    'FAC', 'FAB', 'PROD_FAMILY_DESC', 'PROD_GRP_DESC', 'PROD_MODE_DESC',
    'OPER', 'MOD_TYPE', 'FORM_FACTOR', 'SPECIAL', 'GRADE', 'PKG_DENSITY',
    'EQP_MODEL', 'ORGANIZ', 'MOD_TECH', 'PROD_VER', 'MOD_SECTION', 'PROD_DEN',
    'SPECIAL_HANDLE', 'CUST_INFO', 'MK_REV', 'SAP_HIST', 'DIE TYPE', 'DIE QTY'
]

# ID映射表文件路径
MAPPING_FILE = 'hess_id_mapping.json'


def load_id_mapping() -> Dict:
    """加载ID映射表"""
    if os.path.exists(MAPPING_FILE):
        with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'et': {}, 'at': {}}


def save_id_mapping(mapping: Dict):
    """保存ID映射表"""
    with open(MAPPING_FILE, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def generate_id(row_data: pd.Series, id_fields: List[str]) -> str:
    """根据指定字段生成唯一ID"""
    # 提取字段值，处理缺失值
    values = []
    for field in id_fields:
        value = row_data.get(field, '')
        # 处理NaN和None
        if pd.isna(value) or value is None:
            values.append('')
        else:
            values.append(str(value).strip())
    
    # 使用MD5生成唯一ID
    key_str = '|'.join(values)
    return hashlib.md5(key_str.encode('utf-8')).hexdigest()


def read_excel_in_chunks(file_path: str, chunk_size: int = 1000) -> pd.DataFrame:
    """分段读取Excel文件,适用于大文件"""
    try:
        # 方法1: 尝试使用openpyxl引擎读取(默认)
        # 添加参数禁用样式处理，避免样式损坏导致的错误
        df = pd.read_excel(file_path, engine='openpyxl', dtype=str)
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        
        if file_size > 10:
            print(f"文件大小 {file_size:.2f}MB,使用分段读取")
        
        return df
    except Exception as e:
        print(f"读取Excel文件时发生错误: {e}")


def process_et_data(df: pd.DataFrame, mapping: Dict) -> tuple:
    """处理ET数据，生成ID并更新映射表"""
    ids = []
    et_mapping = mapping.get('et', {})
    
    for idx, row in df.iterrows():
        # 生成ID
        row_id = generate_id(row, ET_ID_FIELDS)
        ids.append(row_id)
        
        # 构建映射键（用于后续手动填充）
        mapping_key = '|'.join([str(row.get(field, '')) for field in ET_ID_FIELDS])
        if mapping_key not in et_mapping:
            et_mapping[mapping_key] = {
                'id': row_id,
                'fields': {field: str(row.get(field, '')) for field in ET_ID_FIELDS}
            }
    
    mapping['et'] = et_mapping
    df['id'] = ids
    return df, mapping


def process_at_data(df: pd.DataFrame, mapping: Dict) -> tuple:
    """处理AT数据，生成ID并更新映射表"""
    ids = []
    at_mapping = mapping.get('at', {})
    
    for idx, row in df.iterrows():
        # 生成ID
        row_id = generate_id(row, AT_ID_FIELDS)
        ids.append(row_id)
        
        # 构建映射键（用于后续手动填充）
        mapping_key = '|'.join([str(row.get(field, '')) for field in AT_ID_FIELDS])
        if mapping_key not in at_mapping:
            at_mapping[mapping_key] = {
                'id': row_id,
                'fields': {field: str(row.get(field, '')) for field in AT_ID_FIELDS}
            }
    
    mapping['at'] = at_mapping
    df['id'] = ids
    return df, mapping


def insert_to_database(df: pd.DataFrame, table_name: str, db_config: Dict):
    """将DataFrame数据插入数据库"""
    if df.empty:
        print(f"数据为空，跳过插入: {table_name}")
        return
    
    connection = None
    try:
        connection = pymysql.connect(**db_config)
        
        with connection.cursor() as cursor:
            # 构建INSERT语句
            columns = df.columns.tolist()
            # 过滤掉数据库中不存在的列
            cursor.execute(f"DESCRIBE {table_name}")
            db_columns = [row[0] for row in cursor.fetchall()]
            valid_columns = [col for col in columns if col in db_columns]
            
            if not valid_columns:
                print(f"没有有效的列可以插入: {table_name}")
                return
            
            placeholders = ', '.join(['%s'] * len(valid_columns))
            column_names = ', '.join([f'`{col}`' for col in valid_columns])
            sql = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
            sql += ', '.join([f'`{col}`=VALUES(`{col}`)' for col in valid_columns if col != 'id'])
            
            # 准备数据
            data = df[valid_columns].values.tolist()
            
            # 批量插入
            batch_size = 500
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                cursor.executemany(sql, batch)
                connection.commit()
                print(f"已插入 {min(i + batch_size, len(data))}/{len(data)} 条记录到 {table_name}")
        
        print(f"成功插入 {len(df)} 条记录到 {table_name}")
        
    except Exception as e:
        print(f"插入数据库失败: {table_name}, 错误: {e}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            connection.close()


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
        sourceDir = r'D:\Sync\业务报告\2026 项目改善\03 HESS Table Auto Check功能开发\28. HESS TABLE'
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
    
    # 加载ID映射表
    mapping = load_id_mapping()
    
    # 检索sourceDir目录下的所有excel文件
    excel_files = [f for f in os.listdir(sourceDir) if f.endswith('.xlsx')]
    
    for excel_file in excel_files:
        print(f"\n处理文件: {excel_file}")
        
        # 构建完整的文件路径
        file_path = os.path.join(sourceDir, excel_file)
        
        # 读取Excel文件
        df = read_excel_in_chunks(file_path)
        if df.empty:
            print(f"跳过空文件: {excel_file}")
            continue
        
        # 判断文件类型（ET或AT）
        file_upper = excel_file.upper()
        if 'ET' in file_upper:
            file_type = 'ET'
            table_name = 'cmsalpha.db_check_hess_et'
            print("识别为ET文件")
        elif 'AT' in file_upper:
            file_type = 'AT'
            table_name = 'cmsalpha.db_check_hess_at'
            print("识别为AT文件")
        else:
            print(f"无法识别文件类型，跳过: {excel_file}")
            continue
        
        # 处理数据并生成ID
        if file_type == 'ET':
            df, mapping = process_et_data(df, mapping)
        else:  # AT
            df, mapping = process_at_data(df, mapping)
        
        # 保存映射表
        save_id_mapping(mapping)
        
        # 插入数据库
        insert_to_database(df, table_name, db_config)
    
    print("\n所有文件处理完成！")
    print(f"ID映射表已保存到: {MAPPING_FILE}")
    print("请手动填充映射表中的业务代码信息")


if __name__ == '__main__':
    main('test')