"""
日志系统 - 统一的日志管理
"""
import logging
import logging.handlers
import os
from pathlib import Path
from typing import Optional
from utils.config_loader import get_config


# dev/utils/logger.py - 修复 PGMLogger 类

class PGMLogger:
    """PGM自动化专用日志类"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PGMLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._loggers = {}
        self._setup_logging()

    def _setup_logging(self):
        """设置日志系统"""
        try:
            config = get_config()
            log_config = config.get_logging_config()

            # 基本配置
            log_level = getattr(logging, log_config.get('level', 'INFO').upper())
            log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            date_format = log_config.get('date_format', '%Y-%m-%d %H:%M:%S')

            # 配置根日志记录器
            root_logger = logging.getLogger()
            root_logger.setLevel(log_level)

            # 清除已有的处理器
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)

            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            console_formatter = logging.Formatter(log_format, date_format)
            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)

            # 文件处理器（轮转）
            log_file = log_config.get('file_path', './logs/pgm_automation.log')
            max_size = log_config.get('max_size_mb', 10) * 1024 * 1024  # 转换为字节
            backup_count = log_config.get('backup_count', 5)

            # 确保日志目录存在
            log_dir = os.path.dirname(log_file)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)

            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=max_size,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter(log_format, date_format)
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)

            # 设置特定模块的日志级别
            self._setup_module_levels()

            self._root_logger = root_logger
            self._root_logger.info(f"✅ 日志系统初始化完成 - 级别: {log_config.get('level', 'INFO')}")
            self._root_logger.info(f"📁 日志文件: {log_file}")

        except Exception as e:
            # 如果配置失败，使用基本日志配置
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            self._root_logger = logging.getLogger()
            self._root_logger.error(f"日志系统配置失败，使用默认配置: {str(e)}")

    def _setup_module_levels(self):
        """设置不同模块的日志级别"""
        # 可以根据需要调整不同模块的日志级别
        module_levels = {
            'requests': logging.WARNING,
            'urllib3': logging.WARNING,
            'sqlalchemy': logging.WARNING,
        }

        for module, level in module_levels.items():
            logging.getLogger(module).setLevel(level)

    def get_logger(self, name: str = None) -> logging.Logger:
        """
        获取指定名称的日志记录器

        Args:
            name: 日志记录器名称，如果为None则返回根记录器

        Returns:
            logging.Logger实例
        """
        if name is None:
            return self._root_logger

        if name not in self._loggers:
            self._loggers[name] = logging.getLogger(name)

        return self._loggers[name]

    def info(self, message: str):
        """输出INFO级别日志"""
        self._root_logger.info(message)

    def error(self, message: str):
        """输出ERROR级别日志"""
        self._root_logger.error(message)

    def warning(self, message: str):
        """输出WARNING级别日志"""
        self._root_logger.warning(message)

    def debug(self, message: str):
        """输出DEBUG级别日志"""
        self._root_logger.debug(message)

    def set_level(self, level: str, name: str = None):
        """
        设置日志级别

        Args:
            level: 日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL）
            name: 日志记录器名称，如果为None则设置根记录器
        """
        logger = self.get_logger(name)
        log_level = getattr(logging, level.upper())
        logger.setLevel(log_level)

        # 更新所有处理器的级别
        for handler in logger.handlers:
            handler.setLevel(log_level)

    def log_execution_start(self, script_name: str, **kwargs):
        """
        记录脚本执行开始

        Args:
            script_name: 脚本名称
            kwargs: 其他参数
        """
        logger = self.get_logger(script_name)
        logger.info(f"🚀 开始执行: {script_name}")

        if kwargs:
            params_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
            logger.info(f"📋 执行参数: {params_str}")

    def log_execution_end(self, script_name: str, success: bool, duration: float = None,
                          message: str = None):
        """
        记录脚本执行结束

        Args:
            script_name: 脚本名称
            success: 是否成功
            duration: 执行时长（秒）
            message: 附加消息
        """
        logger = self.get_logger(script_name)

        status = "✅ 成功" if success else "❌ 失败"
        duration_str = f" (耗时: {duration:.2f}秒)" if duration is not None else ""
        message_str = f" - {message}" if message else ""

        log_msg = f"{status}执行完成: {script_name}{duration_str}{message_str}"

        if success:
            logger.info(log_msg)
        else:
            logger.error(log_msg)

    def log_database_operation(self, operation: str, table: str,
                               records_affected: int = None, details: str = None):
        """
        记录数据库操作

        Args:
            operation: 操作类型（SELECT, INSERT, UPDATE, DELETE）
            table: 表名
            records_affected: 影响的记录数
            details: 详细信息
        """
        logger = self.get_logger('database')

        records_str = f", 影响记录: {records_affected}" if records_affected is not None else ""
        details_str = f" - {details}" if details else ""

        logger.debug(f"📊 数据库操作: {operation} {table}{records_str}{details_str}")

    def log_file_operation(self, operation: str, src: str = None,
                           dest: str = None, success: bool = True,
                           size: int = None):
        """
        记录文件操作

        Args:
            operation: 操作类型（COPY, MOVE, DELETE, UNZIP等）
            src: 源文件路径
            dest: 目标文件路径
            success: 是否成功
            size: 文件大小（字节）
        """
        logger = self.get_logger('file')

        src_str = f"源: {src}" if src else ""
        dest_str = f" -> 目标: {dest}" if dest else ""
        size_str = f" ({size:,} bytes)" if size else ""
        status = "✅" if success else "❌"

        logger.debug(f"{status} 文件操作: {operation}{size_str} {src_str}{dest_str}")

    def log_oms_operation(self, endpoint: str, method: str = 'GET',
                          status_code: int = None, response_time: float = None):
        """
        记录OMS API操作

        Args:
            endpoint: API端点
            method: HTTP方法
            status_code: 状态码
            response_time: 响应时间（秒）
        """
        logger = self.get_logger('oms')

        status_str = f", 状态: {status_code}" if status_code else ""
        time_str = f", 响应时间: {response_time:.2f}s" if response_time else ""

        logger.info(f"🌐 OMS API: {method} {endpoint}{status_str}{time_str}")


# 全局日志实例
_logger_instance = None


def get_pgm_logger() -> PGMLogger:
    """
    获取全局PGM日志实例

    Returns:
        PGMLogger实例
    """
    global _logger_instance

    if _logger_instance is None:
        _logger_instance = PGMLogger()

    return _logger_instance


def get_module_logger(module_name: str) -> logging.Logger:
    """
    获取指定模块的日志记录器

    Args:
        module_name: 模块名称

    Returns:
        logging.Logger实例
    """
    return get_pgm_logger().get_logger(module_name)