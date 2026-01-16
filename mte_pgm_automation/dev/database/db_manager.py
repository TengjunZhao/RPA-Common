"""
数据库管理器 - 提供基本的增删查改功能和业务层操作
"""
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # core目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录

sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)
from utils.config_loader import get_config
from utils.logger import get_pgm_logger
from typing import Any, Dict, List, Optional, Union
import logging


class DBManager:
    """数据库管理器"""

    def __init__(self):
        """初始化数据库管理器"""
        self.config = get_config()
        self.logger = get_pgm_logger()
        self.db_config = self.config.get_database_config()
        self.engine = None
        self.Session = None
        self.session = None

        # 连接数据库
        self.connect()

    def connect(self):
        """连接数据库"""
        try:
            # 从配置中获取数据库连接信息
            connection_string = self._build_connection_string()
            
            self.engine = create_engine(
                connection_string,
                pool_size=self.db_config.get('pool_size', 10),
                max_overflow=self.db_config.get('max_overflow', 20),
                pool_timeout=self.db_config.get('pool_timeout', 30),
                pool_recycle=self.db_config.get('pool_recycle', 3600),
                echo=self.db_config.get('echo', False)
            )

            # 创建会话工厂
            self.Session = scoped_session(sessionmaker(bind=self.engine))
            self.session = self.Session()

            self.logger.info(f"✅ 数据库连接成功: {connection_string.split('@')[-1].split('/')[0]}")
        except Exception as e:
            self.logger.error(f"❌ 数据库连接失败: {str(e)}")
            raise

    def _build_connection_string(self) -> str:
        """构建数据库连接字符串"""
        db_type = self.db_config.get('type', 'mysql')
        username = self.db_config.get('username', '')
        password = self.db_config.get('password', '')
        host = self.db_config.get('host', 'localhost')
        port = self.db_config.get('port', 3306)
        database = self.db_config.get('database', '')

        if db_type == 'mysql':
            return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        elif db_type == 'postgresql':
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"
        elif db_type == 'sqlite':
            return f"sqlite:///{database}"
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")

    def _execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """执行查询操作"""
        try:
            result = self.session.execute(text(query), params or {})
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]
        except SQLAlchemyError as e:
            self.logger.error(f"❌ 查询执行失败: {str(e)}")
            raise

    def _execute_update(self, query: str, params: Optional[Dict] = None) -> int:
        """执行更新操作（INSERT, UPDATE, DELETE）"""
        try:
            result = self.session.execute(text(query), params or {})
            self.session.commit()
            return result.rowcount
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"❌ 更新执行失败: {str(e)}")
            raise

    def _execute_many(self, query: str, params_list: List[Dict]) -> int:
        """批量执行操作"""
        try:
            result = self.session.execute(text(query), params_list)
            self.session.commit()
            return result.rowcount
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"❌ 批量执行失败: {str(e)}")
            raise

    # 基本的增删查改保护方法
    def _select(self, table: str, columns: Union[str, List[str]] = "*", 
                where_clause: str = "", params: Optional[Dict] = None) -> List[Dict]:
        """保护方法：基础查询操作"""
        if isinstance(columns, list):
            columns_str = ", ".join(columns)
        else:
            columns_str = columns

        query = f"SELECT {columns_str} FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"

        return self._execute_query(query, params)

    def _insert(self, table: str, data: Dict[str, Any]) -> int:
        """保护方法：基础插入操作"""
        columns = ", ".join(data.keys())
        placeholders = ":" + ", :".join(data.keys())
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        return self._execute_update(query, data)

    def _update(self, table: str, data: Dict[str, Any], 
                where_clause: str, params: Optional[Dict] = None) -> int:
        """保护方法：基础更新操作"""
        set_clause = ", ".join([f"{key} = :{key}" for key in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        
        # 合并数据和条件参数
        all_params = {**data, **(params or {})}
        
        return self._execute_update(query, all_params)

    def _delete(self, table: str, where_clause: str, 
                params: Optional[Dict] = None) -> int:
        """保护方法：基础删除操作"""
        query = f"DELETE FROM {table} WHERE {where_clause}"
        
        return self._execute_update(query, params)

    def _count(self, table: str, where_clause: str = "", 
               params: Optional[Dict] = None) -> int:
        """保护方法：基础计数操作"""
        query = f"SELECT COUNT(*) as count FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result = self._execute_query(query, params)
        return result[0]['count'] if result else 0

    # 业务层公共方法示例
    def insert_record(self, table: str, record_data: Dict[str, Any]) -> int:
        """业务层方法：插入单条记录"""
        try:
            result = self._insert(table, record_data)
            self.logger.log_database_operation('INSERT', table, result, f"插入了 {result} 条记录")
            return result
        except Exception as e:
            self.logger.error(f"❌ 插入记录失败: {str(e)}")
            raise

    def select_records(self, table: str, columns: Union[str, List[str]] = "*", 
                      condition: str = "", params: Optional[Dict] = None) -> List[Dict]:
        """业务层方法：查询多条记录"""
        try:
            result = self._select(table, columns, condition, params)
            self.logger.log_database_operation('SELECT', table, len(result), f"查询了 {len(result)} 条记录")
            return result
        except Exception as e:
            self.logger.error(f"❌ 查询记录失败: {str(e)}")
            raise

    def select_single_record(self, table: str, columns: Union[str, List[str]] = "*", 
                           condition: str = "", params: Optional[Dict] = None) -> Optional[Dict]:
        """业务层方法：查询单条记录"""
        try:
            result = self._select(table, columns, condition, params)
            record = result[0] if result else None
            self.logger.log_database_operation('SELECT', table, 1 if record else 0, "查询单条记录")
            return record
        except Exception as e:
            self.logger.error(f"❌ 查询单条记录失败: {str(e)}")
            raise

    def update_records(self, table: str, update_data: Dict[str, Any], 
                      condition: str, params: Optional[Dict] = None) -> int:
        """业务层方法：更新记录"""
        try:
            result = self._update(table, update_data, condition, params)
            self.logger.log_database_operation('UPDATE', table, result, f"更新了 {result} 条记录")
            return result
        except Exception as e:
            self.logger.error(f"❌ 更新记录失败: {str(e)}")
            raise

    def delete_records(self, table: str, condition: str, 
                      params: Optional[Dict] = None) -> int:
        """业务层方法：删除记录"""
        try:
            result = self._delete(table, condition, params)
            self.logger.log_database_operation('DELETE', table, result, f"删除了 {result} 条记录")
            return result
        except Exception as e:
            self.logger.error(f"❌ 删除记录失败: {str(e)}")
            raise

    def batch_insert(self, table: str, records: List[Dict[str, Any]]) -> int:
        """业务层方法：批量插入记录"""
        try:
            if not records:
                return 0

            columns = ", ".join(records[0].keys())
            placeholders = ":" + ", :".join(records[0].keys())
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

            result = self._execute_many(query, records)
            self.logger.log_database_operation('INSERT', table, result, f"批量插入了 {result} 条记录")
            return result
        except Exception as e:
            self.logger.error(f"❌ 批量插入失败: {str(e)}")
            raise

    def record_exists(self, table: str, condition: str, 
                     params: Optional[Dict] = None) -> bool:
        """业务层方法：检查记录是否存在"""
        try:
            count = self._count(table, condition, params)
            return count > 0
        except Exception as e:
            self.logger.error(f"❌ 检查记录存在性失败: {str(e)}")
            raise

    def get_table_count(self, table: str, condition: str = "", 
                       params: Optional[Dict] = None) -> int:
        """业务层方法：获取表记录总数"""
        try:
            count = self._count(table, condition, params)
            return count
        except Exception as e:
            self.logger.error(f"❌ 获取表记录数失败: {str(e)}")
            raise

    def execute_custom_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """业务层方法：执行自定义查询"""
        try:
            result = self._execute_query(query, params)
            self.logger.log_database_operation('CUSTOM_QUERY', 'N/A', len(result), "执行自定义查询")
            return result
        except Exception as e:
            self.logger.error(f"❌ 执行自定义查询失败: {str(e)}")
            raise

    def execute_custom_update(self, query: str, params: Optional[Dict] = None) -> int:
        """业务层方法：执行自定义更新"""
        try:
            result = self._execute_update(query, params)
            self.logger.log_database_operation('CUSTOM_UPDATE', 'N/A', result, "执行自定义更新")
            return result
        except Exception as e:
            self.logger.error(f"❌ 执行自定义更新失败: {str(e)}")
            raise

    # 获取指定表的某字段的最大值记录
    def get_max_value(self, table: str, column: str) -> Optional[Dict]:
        try:
            query = f"SELECT * , MAX({column}) AS max_value FROM {table}"
            result = self._execute_query(query)
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"❌ 获取最大值记录失败: {str(e)}")
            raise

    # 获取某表指定条件下的最大字段记录（带参数版本）
    def get_max_value_by_condition_with_params(self, table: str, column: str, condition: str, params: Optional[Dict] = None) -> Optional[Dict]:
        try:
            query = f"SELECT * , MAX({column}) AS max_value FROM {table} WHERE {condition}"
            result = self._execute_query(query, params)
            return result[0] if result else None
        except Exception as e:
            self.logger.error(f"❌ 获取最大值记录失败: {str(e)}")
            raise

    def close(self):
        """关闭数据库连接"""
        try:
            if self.session:
                self.session.close()
            if self.Session:
                self.Session.remove()
            self.logger.info("🔒 数据库连接已关闭")
        except Exception as e:
            self.logger.error(f"❌ 关闭数据库连接失败: {str(e)}")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()


