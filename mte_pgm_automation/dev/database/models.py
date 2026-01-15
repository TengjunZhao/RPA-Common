"""
数据库模型定义 - 统一管理所有表结构的字段映射
"""
from typing import Dict, Any

# pgm_main 表结构定义
pgm_main_schema = {
    'draft_id': '',
    'process_id': '',
    'pgm_type': '',
    'status': '',
    'pgm_name': '',
    'tat': '',
    'create_time': '',
    'current_step': '',
    'sk_user': '',
    'hitech_user': '',
}

# db_oms_pgm 表结构定义
db_oms_pgm_schema = {
    'process_id': '',  # varchar(50)
    'process_type': '',  # varchar(100)
    'process_type_desc': '',  # varchar(200)
    'fac_id': '',  # varchar(50)
    'process_name': '',  # varchar(255)
    'draft_id': '',  # varchar(50), 主键
    'complete_yn': '',  # varchar(20)
    'process_status_code': '',  # varchar(50)
    'work_type_desc': '',  # varchar(100), 主键
    'work_prgs_mag_cd': '',  # varchar(20)
    'work_sequence': 0,  # int(11)
    'prev_work_sequence': None,  # int(11)
    'work_type': '',  # varchar(50)
    'work_status': '',  # varchar(50)
    'organ_name': '',  # varchar(100)
    'user_name': '',  # varchar(50)
    'user_id': '',  # varchar(50)
    'work_start_tm': '',  # varchar(20)
    'bp_id': '',  # varchar(100)
    'linked_bp_id': '',  # varchar(100)
    'work_type_no': None  # int(4)
}

# 数据库表结构映射字典
TABLE_SCHEMAS = {
    'pgm_main': pgm_main_schema,
    'db_oms_pgm': db_oms_pgm_schema,
    # 可以在这里添加更多的表结构定义
}

def get_table_schema(table_name: str) -> Dict[str, Any]:
    """
    获取指定表的结构定义
    
    Args:
        table_name: 表名
        
    Returns:
        表结构定义字典，如果表不存在则返回空字典
    """
    return TABLE_SCHEMAS.get(table_name, {})

def create_empty_record(table_name: str) -> Dict[str, Any]:
    """
    创建指定表的空记录（使用默认值）
    
    Args:
        table_name: 表名
        
    Returns:
        带有默认值的空记录字典
    """
    schema = get_table_schema(table_name)
    return {field: value if isinstance(value, (str, int, float, bool)) and value != {} else (
        {} if isinstance(value, dict) else None if value != 0 and value != 0.0 else value
    ) for field, value in schema.items()}

def get_required_fields(table_name: str) -> list:
    """
    获取指定表的必填字段列表
    
    Args:
        table_name: 表名
        
    Returns:
        必填字段列表
    """
    # 这里可以根据需要定义各表的必填字段
    required_fields_map = {
        'pgm_main': ['pgm_id', 'pgm_type'],
        'db_oms_pgm': ['draft_id', 'work_type_desc']
    }
    return required_fields_map.get(table_name, [])