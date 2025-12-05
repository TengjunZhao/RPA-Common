"""
数据库连接管理 - 统一的数据库连接池管理
"""
import pymysql
from pymysql import connections
from typing import Optional, Dict, Any
import threading
import time
from datetime import datetime
from utils.config_loader import get_config
from utils.logger import get_pgm_logger


class DBConnectionPool:
    """数据库连接池"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DBConnectionPool, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._connections = {}
        self._config = None
        self._logger = get_pgm_logger().get_logger('database')
        self._initialize_pool()

    def _initialize_pool(self):
        """初始化连接池"""
        try:
            config_loader = get_config()
            self._config = config_loader.get_database_config()

            self._logger.info(f"🔌 数据库连接池初始化 - 主机: {self._config.get('host', 'localhost')}")
            self._logger.info(f"📊 数据库: {self._config.get('database', 'modulemte')}")

        except Exception as e:
            self._logger.error(f"数据库连接池初始化失败: {str(e)}")
            raise

    def get_connection(self, autocommit: bool = True) -> connections.Connection:
        """
        获取数据库连接
        """
        thread_id = threading.get_ident()

        if thread_id in self._connections:
            conn = self._connections[thread_id]
            try:
                conn.ping(reconnect=True)
                return conn
            except:
                try:
                    conn.close()
                except:
                    pass
                del self._connections[thread_id]

        try:
            # 从配置获取字符集和排序规则
            charset = self._config.get('charset', 'utf8mb4')
            collation = self._config.get('collation', 'utf8mb4_0900_ai_ci')

            init_command = f"SET NAMES {charset} COLLATE {collation}"

            conn = pymysql.connect(
                host=self._config.get('host', 'localhost'),
                port=self._config.get('port', 3306),
                user=self._config.get('username', 'remoteuser'),
                password=self._config.get('password', 'password'),
                database=self._config.get('database', 'cmsalpha'),
                charset=charset,
                autocommit=autocommit,
                cursorclass=pymysql.cursors.DictCursor,
                init_command=init_command
            )

            self._connections[thread_id] = conn
            self._logger.debug(f"✅ 创建新的数据库连接 - 线程ID: {thread_id}")

            return conn

        except pymysql.Error as e:
            self._logger.error(f"数据库连接失败: {str(e)}")
            raise

    def close_all_connections(self):
        """关闭所有连接"""
        for thread_id, conn in list(self._connections.items()):
            try:
                conn.close()
                self._logger.debug(f"🔒 关闭数据库连接 - 线程ID: {thread_id}")
            except:
                pass

        self._connections.clear()
        self._logger.info("🔒 所有数据库连接已关闭")

    def get_connection_count(self) -> int:
        """获取当前连接数"""
        return len(self._connections)

    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            self._logger.error(f"数据库连接测试失败: {str(e)}")
            return False


class DBTransaction:
    """数据库事务管理器"""

    def __init__(self, connection_pool: Optional[DBConnectionPool] = None):
        """
        初始化事务管理器

        Args:
            connection_pool: 连接池实例，如果为None则使用全局实例
        """
        self.connection_pool = connection_pool or get_db_pool()
        self.connection = None
        self.cursor = None
        self.in_transaction = False

    def __enter__(self):
        """进入上下文管理器"""
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器"""
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

        self.close()

    def begin(self):
        """开始事务"""
        if self.in_transaction:
            raise RuntimeError("事务已开始")

        self.connection = self.connection_pool.get_connection(autocommit=False)
        self.cursor = self.connection.cursor()
        self.in_transaction = True

        get_pgm_logger().log_database_operation("BEGIN", "transaction")

    def commit(self):
        """提交事务"""
        if not self.in_transaction:
            raise RuntimeError("事务未开始")

        self.connection.commit()
        self.in_transaction = False

        get_pgm_logger().log_database_operation("COMMIT", "transaction")

    def rollback(self):
        """回滚事务"""
        if not self.in_transaction:
            raise RuntimeError("事务未开始")

        self.connection.rollback()
        self.in_transaction = False

        get_pgm_logger().log_database_operation("ROLLBACK", "transaction")

    def close(self):
        """关闭游标和连接"""
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        # 注意：这里不关闭连接，由连接池管理

    def execute(self, sql: str, params: Any = None) -> int:
        """
        执行SQL语句

        Args:
            sql: SQL语句
            params: 参数

        Returns:
            影响的行数
        """
        if not self.in_transaction:
            raise RuntimeError("事务未开始")

        try:
            affected = self.cursor.execute(sql, params)

            # 记录操作
            operation = sql.strip().split()[0].upper()
            get_pgm_logger().log_database_operation(
                operation,
                "various",  # 表名从SQL中解析较复杂，这里简化
                affected
            )

            return affected

        except pymysql.Error as e:
            get_pgm_logger().get_logger('database').error(f"SQL执行失败: {sql} - {str(e)}")
            raise

    def fetchone(self) -> Optional[Dict]:
        """获取一条记录"""
        if not self.in_transaction:
            raise RuntimeError("事务未开始")

        return self.cursor.fetchone()

    def fetchall(self) -> list:
        """获取所有记录"""
        if not self.in_transaction:
            raise RuntimeError("事务未开始")

        return self.cursor.fetchall()

    def lastrowid(self) -> Optional[int]:
        """获取最后插入的ID"""
        if not self.in_transaction:
            raise RuntimeError("事务未开始")

        return self.cursor.lastrowid


# 全局连接池实例
_connection_pool_instance = None


def get_db_pool() -> DBConnectionPool:
    """
    获取全局数据库连接池

    Returns:
        DBConnectionPool实例
    """
    global _connection_pool_instance

    if _connection_pool_instance is None:
        _connection_pool_instance = DBConnectionPool()

    return _connection_pool_instance


def execute_query(sql: str, params: Any = None, fetch: bool = True) -> Any:
    """
    快速执行查询（简单查询使用）

    Args:
        sql: SQL语句
        params: 参数
        fetch: 是否获取结果

    Returns:
        查询结果
    """
    pool = get_db_pool()
    conn = pool.get_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)

            if fetch:
                if sql.strip().upper().startswith('SELECT'):
                    return cursor.fetchall()
                else:
                    conn.commit()
                    return cursor.rowcount
            else:
                conn.commit()
                return None

    finally:
        # 注意：这里不关闭连接，由连接池管理
        pass


def test_database_connection() -> bool:
    """
    测试数据库连接

    Returns:
        连接是否成功
    """
    try:
        pool = get_db_pool()
        return pool.test_connection()
    except Exception as e:
        get_pgm_logger().get_logger('database').error(f"数据库连接测试失败: {str(e)}")
        return False