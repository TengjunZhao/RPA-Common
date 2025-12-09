"""
配置加载器 - 负责加载和管理应用程序配置
"""
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
import base64
from cryptography.fernet import Fernet


class ConfigLoader:
    """配置加载和管理类"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置加载器

        Args:
            config_path: 配置文件路径，如果为None则使用默认路径
        """
        self.config_path = config_path or self._get_default_config_path()
        self._config = None
        self._current_environment = None

    def _get_default_config_path(self) -> Path:
        """获取默认配置文件路径"""
        # 优先检查当前目录
        current_dir = Path.cwd()

        # 检查可能的配置文件位置
        possible_paths = [
            current_dir / "config.json",
            current_dir / "config" / "config.json",
            current_dir.parent / "config.json",
            Path.home() / ".pgm_config.json"
        ]

        for path in possible_paths:
            if path.exists():
                return path

        # 如果都没有，使用开发环境默认路径
        return Path(__file__).parent.parent / "config" / "config_template.json"

    def load_config(self, environment: Optional[str] = None) -> Dict[str, Any]:
        """
        加载配置文件

        Args:
            environment: 环境类型（test/production），如果为None则从配置文件读取

        Returns:
            配置字典
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)

            # 设置环境
            if environment:
                self._current_environment = environment
            else:
                self._current_environment = self._config.get('environment', 'test')

            print(f"✅ 配置加载成功: {self.config_path}")
            print(f"📋 当前环境: {self._current_environment}")

            return self._config

        except FileNotFoundError:
            raise FileNotFoundError(f"配置文件未找到: {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"配置文件JSON格式错误: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"加载配置文件失败: {str(e)}")

    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置"""
        if self._config is None:
            self.load_config()

        db_config = self._config.get('database', {})
        env_config = db_config.get(self._current_environment, {})

        if not env_config:
            # 回退到直接配置
            env_config = db_config

        return env_config

    def get_oms_config(self) -> Dict[str, Any]:
        """获取OMS配置"""
        if self._config is None:
            self.load_config()

        oms_config = self._config.get('oms', {})
        creds = oms_config.get('credentials', {}).get(self._current_environment, {})

        # 解密密码（如果有）
        if 'password' in creds:
            try:
                creds['password'] = self._decrypt_password(
                    creds['password'],
                    creds.get('decryption_key', '')
                )
            except:
                # 如果解密失败，保持原样（可能是明文）
                pass

        return {
            'api_base': oms_config.get('api_base', ''),
            'endpoints': oms_config.get('endpoints', {}),
            'credentials': creds,
            'headers': oms_config.get('headers', {}),
            'request_timeout': oms_config.get('request_timeout', {
                'login': 30,
                'list': 60,
                'detail': 60,
                'download': 300
            }),
            'retry_settings': oms_config.get('retry_settings', {
                'max_retries': 3,
                'retry_delay_seconds': 5,
                'retry_on_status_codes': [408, 429, 500, 502, 503, 504]
            })
        }

    def get_file_paths(self) -> Dict[str, str]:
        """获取文件路径配置"""
        if self._config is None:
            self.load_config()

        paths_config = self._config.get('file_paths', {})
        env_paths = paths_config.get(self._current_environment, {})

        # 确保目录存在
        for key, path in env_paths.items():
            if path:
                os.makedirs(path, exist_ok=True)

        return env_paths

    def get_ftp_config(self, pgm_type: str = None) -> Dict[str, Any]:
        """获取FTP配置"""
        if self._config is None:
            self.load_config()

        ftp_config = self._config.get('ftp', {})

        if pgm_type and pgm_type.upper() in ['AT', 'ET']:
            return ftp_config.get(pgm_type.lower(), {})

        return ftp_config

    def get_email_config(self) -> Dict[str, Any]:
        """获取邮件配置"""
        if self._config is None:
            self.load_config()

        email_config = self._config.get('email', {})
        creds = email_config.get('credentials', {}).get(self._current_environment, {})

        return {
            'smtp_server': email_config.get('smtp_server', ''),
            'smtp_port': email_config.get('smtp_port', 587),
            'use_tls': email_config.get('use_tls', True),
            'sender_email': email_config.get('sender_email', ''),
            'sender_name': email_config.get('sender_name', ''),
            'username': creds.get('username', ''),
            'password': creds.get('password', ''),
            'recipients': email_config.get('recipients', {}),
            'tat_thresholds': email_config.get('tat_thresholds', {})
        }

    def get_logging_config(self) -> Dict[str, Any]:
        """获取日志配置"""
        if self._config is None:
            self.load_config()

        logging_config = self._config.get('logging', {})

        # 确保日志目录存在
        log_file = logging_config.get('file_path', './logs/pgm_automation.log')
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        return logging_config

    def get_rpa_settings(self) -> Dict[str, Any]:
        """获取RPA设置"""
        if self._config is None:
            self.load_config()

        return self._config.get('rpa_settings', {})

    def get_hess_settings(self) -> Dict[str, Any]:
        """获取HESS设置"""
        if self._config is None:
            self.load_config()

        return self._config.get('hess_settings', {})

    def get_pgm_verification_settings(self) -> Dict[str, Any]:
        """获取PGM验证设置"""
        if self._config is None:
            self.load_config()

        return self._config.get('pgm_verification', {})

    def get_current_environment(self) -> str:
        """获取当前环境"""
        return self._current_environment

    def set_environment(self, environment: str):
        """设置当前环境"""
        valid_envs = ['test', 'production']
        if environment not in valid_envs:
            raise ValueError(f"环境必须是 {valid_envs} 之一")

        self._current_environment = environment

    def _decrypt_password(self, encrypted_password: str, key: str) -> str:
        """
        解密密码

        Args:
            encrypted_password: 加密的密码
            key: 解密密钥

        Returns:
            解密后的密码
        """
        if not encrypted_password or not key:
            return encrypted_password

        try:
            # 简单的Base64解码（实际项目中应使用更安全的加密方式）
            # 这里是一个简单示例
            decoded = base64.b64decode(encrypted_password).decode('utf-8')
            return decoded
        except:
            # 如果解码失败，返回原字符串
            return encrypted_password

    def save_config(self, new_config: Dict[str, Any], config_path: Optional[str] = None):
        """
        保存配置到文件

        Args:
            new_config: 新的配置字典
            config_path: 保存路径，如果为None则使用当前路径
        """
        save_path = config_path or self.config_path

        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(new_config, f, indent=2, ensure_ascii=False)

            print(f"✅ 配置保存成功: {save_path}")

        except Exception as e:
            raise RuntimeError(f"保存配置文件失败: {str(e)}")


# 全局配置实例
_config_instance = None


def get_config() -> ConfigLoader:
    """
    获取全局配置实例（单例模式）

    Returns:
        ConfigLoader实例
    """
    global _config_instance

    if _config_instance is None:
        _config_instance = ConfigLoader()
        _config_instance.load_config()

    return _config_instance


def reload_config(environment: Optional[str] = None) -> ConfigLoader:
    """
    重新加载配置

    Args:
        environment: 环境类型

    Returns:
        重新加载后的ConfigLoader实例
    """
    global _config_instance

    if _config_instance is None:
        _config_instance = ConfigLoader()

    _config_instance.load_config(environment)

    return _config_instance