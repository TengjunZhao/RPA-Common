"""
SQLAlchemy连接管理器 - 专门处理字符集问题
"""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from utils.config_loader import get_config
from utils.logger import get_pgm_logger


class SQLAlchemyManager:
    """SQLAlchemy管理器"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SQLAlchemyManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self.engine = None
        self.Session = None
        self.logger = get_pgm_logger().get_logger('sqlalchemy')
        self._setup_engine()

    def _setup_engine(self):
        """设置SQLAlchemy引擎"""
        try:
            config = get_config().get_database_config()

            # 构建连接URL，特别注意字符集参数
            db_url = (
                f"mysql+pymysql://{config['username']}:{config['password']}"
                f"@{config['host']}:{config['port']}/{config['database']}"
                "?charset=utf8mb4"
            )

            # 创建引擎 - 解决字符集问题的关键配置
            self.engine = create_engine(
                db_url,
                pool_size=10,
                max_overflow=20,
                pool_recycle=3600,
                pool_pre_ping=True,
                echo=False,  # 生产环境设为False
                # 核心：设置连接参数
                connect_args={
                    'charset': 'utf8mb4',
                    'init_command': "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;"
                },
                # 设置执行选项
                execution_options={
                    'isolation_level': 'READ COMMITTED'
                }
            )

            # 创建会话工厂
            self.Session = scoped_session(sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False
            ))

            # 测试连接
            self.test_connection()

            self.logger.info("✅ SQLAlchemy引擎初始化完成")

        except Exception as e:
            self.logger.error(f"❌ SQLAlchemy引擎初始化失败: {str(e)}")
            raise

    def test_connection(self):
        """测试数据库连接"""
        try:
            with self.engine.connect() as conn:
                # 执行字符集检查
                result = conn.execute(text(
                    "SELECT @@character_set_connection, @@collation_connection"
                )).fetchone()

                charset, collation = result
                self.logger.info(f"📊 连接字符集: {charset}, 排序规则: {collation}")

                # 验证与表的排序规则匹配
                if collation != 'utf8mb4_0900_ai_ci':
                    self.logger.warning(f"⚠️  连接排序规则({collation})与表不匹配")

                    # 尝试设置正确的排序规则
                    conn.execute(text("SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci"))

                    # 再次检查
                    result = conn.execute(text(
                        "SELECT @@character_set_connection, @@collation_connection"
                    )).fetchone()

                    self.logger.info(f"📊 调整后字符集: {result[0]}, 排序规则: {result[1]}")

                return True

        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {str(e)}")
            return False

    def get_session(self):
        """获取数据库会话"""
        return self.Session()

    def close_session(self, session):
        """关闭数据库会话"""
        try:
            session.close()
        except:
            pass

    def execute_raw_sql(self, sql, params=None):
        """
        执行原始SQL语句（绕过ORM，用于复杂查询）

        Args:
            sql: SQL语句
            params: 参数

        Returns:
            执行结果
        """
        try:
            with self.engine.connect() as conn:
                # 确保使用正确的排序规则
                conn.execute(text("SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci"))

                if params:
                    result = conn.execute(text(sql), params)
                else:
                    result = conn.execute(text(sql))

                conn.commit()

                if sql.strip().upper().startswith('SELECT'):
                    return result.fetchall()
                else:
                    return result.rowcount

        except Exception as e:
            self.logger.error(f"执行SQL失败: {sql} - {str(e)}")
            raise

    def bulk_insert(self, table_name, data_list):
        """
        批量插入数据（使用原始SQL避免字符集问题）

        Args:
            table_name: 表名
            data_list: 数据列表

        Returns:
            插入的记录数
        """
        if not data_list:
            return 0

        # 获取列名
        first_row = data_list[0]
        columns = list(first_row.keys())
        columns_str = ', '.join(columns)

        # 构建占位符
        placeholders = ', '.join(['%s'] * len(columns))

        # 构建SQL
        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

        # 准备数据
        values_list = []
        for row in data_list:
            values = tuple(row.get(col) for col in columns)
            values_list.append(values)

        try:
            with self.engine.connect() as conn:
                # 确保使用正确的排序规则
                conn.execute(text("SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci"))

                result = conn.execute(text(sql), values_list)
                conn.commit()

                self.logger.info(f"✅ 批量插入 {result.rowcount} 条记录到 {table_name}")
                return result.rowcount

        except Exception as e:
            self.logger.error(f"批量插入失败: {str(e)}")
            raise


# 全局管理器实例
_sqlalchemy_manager = None

def get_sqlalchemy_manager() -> SQLAlchemyManager:
    """
    获取全局SQLAlchemy管理器

    Returns:
        SQLAlchemyManager实例
    """
    global _sqlalchemy_manager

    if _sqlalchemy_manager is None:
        _sqlalchemy_manager = SQLAlchemyManager()

    return _sqlalchemy_manager

def get_db_session():
    """
    获取数据库会话

    Returns:
        SQLAlchemy会话
    """
    manager = get_sqlalchemy_manager()
    return manager.get_session()

def execute_sql(sql, params=None):
    """
    执行原始SQL

    Args:
        sql: SQL语句
        params: 参数

    Returns:
        执行结果
    """
    manager = get_sqlalchemy_manager()
    return manager.execute_raw_sql(sql, params)