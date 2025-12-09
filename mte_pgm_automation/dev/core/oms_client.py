"""
OMS客户端模块 - 负责与SK Hynix OMS系统交互
"""
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from urllib.parse import urljoin
import time
import hashlib
import sys
import os
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # scripts目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录

sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)
from utils.config_loader import get_config
from utils.logger import get_pgm_logger
from database.models import PGMOmsHistory
from database.repositories import PGMOmsHistoryRepository


class OMSClient:
    """OMS系统客户端"""

    def __init__(self):
        """初始化OMS客户端"""
        self.logger = get_pgm_logger().get_logger('oms')
        self.config = get_config().get_oms_config()

        # 获取端点配置
        self.endpoints = self.config['endpoints']
        self.api_base = self.config['api_base']

        # 认证信息
        config_loader = get_config()
        env = config_loader.get_current_environment()
        if env in self.config.get('credentials', {}):
            self.credentials = self.config['credentials'][env]
        else:
            # 回退到默认凭据结构（兼容旧配置）
            self.credentials = self.config.get('credentials', {})

        # Token管理
        self.token = None
        self.token_expiry = None
        self.token_refresh_buffer = timedelta(minutes=5)  # 提前5分钟刷新token

        # 请求超时配置
        self.timeouts = self.config.get('request_timeout', {
            'login': 30,
            'list': 60,
            'detail': 60,
            'download': 300
        })

        # 重试配置
        self.retry_settings = self.config.get('retry_settings', {
            'max_retries': 3,
            'retry_delay_seconds': 5,
            'retry_on_status_codes': [408, 429, 500, 502, 503, 504]
        })

        # 缓存配置
        self.cache = {}
        self.cache_ttl = 300  # 5分钟缓存

        # 创建带重试机制的Session
        self.session = self._create_retry_session()

        self.logger.info(f"🔧 OMS客户端初始化完成，环境: {env}")

        self.file_paths = get_config().get_file_paths()

    def _create_retry_session(self) -> requests.Session:
        """创建带重试机制的Session"""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.retry_settings['max_retries'],
            backoff_factor=self.retry_settings['retry_delay_seconds'],
            status_forcelist=self.retry_settings['retry_on_status_codes']
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def _get_headers(self, endpoint_name: str = None) -> Dict[str, str]:
        """
        获取请求头

        Args:
            endpoint_name: 端点名称，用于获取特定端点的headers

        Returns:
            请求头字典
        """
        headers = self.config['headers']['common'].copy()

        # 添加特定端点的headers
        if endpoint_name and 'endpoint_specific' in self.config['headers']:
            specific_headers = self.config['headers']['endpoint_specific'].get(endpoint_name, {})
            headers.update(specific_headers)

        # 添加Authorization头
        if self.token and self._is_token_valid():
            headers['Authorization'] = self.token

        return headers

    def login(self, force: bool = False) -> Tuple[bool, str]:
        """
        登录OMS系统获取token

        Args:
            force: 是否强制重新登录

        Returns:
            (是否成功, 错误信息)
        """
        try:
            # 检查token是否仍然有效
            if not force and self._is_token_valid():
                self.logger.debug("✅ 使用缓存的OMS token")
                return True, "使用缓存的token"

            login_url = urljoin(self.api_base, self.endpoints['auth'])

            # 准备登录数据
            login_data = {
                "id": self.credentials['user_id'],
                "password": self.credentials['password']
            }

            self.logger.info(f"🔑 正在登录OMS系统: {self.credentials['user_id']}")
            self.logger.debug(f"登录URL: {login_url}")

            # 发送登录请求
            response = self.session.post(
                login_url,
                json=login_data,
                headers=self._get_headers(),
                timeout=self.timeouts['login']
            )

            response.raise_for_status()

            result = response.json()

            # 检查响应结构
            if not isinstance(result, dict):
                return False, "登录响应格式错误"

            token = result.get("token")
            if not token:
                error_msg = result.get("message", "未知错误")
                self.logger.error(f"❌ OMS登录失败: {error_msg}")
                return False, f"响应中未找到token: {error_msg}"

            # 更新token
            self.token = f"Bearer {token}"

            # 解析token过期时间（从响应中获取或使用默认值）
            token_duration = result.get("expires_in", 3600)  # 默认1小时
            self.token_expiry = datetime.now() + timedelta(seconds=token_duration)

            self.logger.info(f"✅ OMS登录成功，token有效期至: {self.token_expiry}")
            return True, "登录成功"

        except requests.exceptions.Timeout:
            self.logger.error("❌ OMS登录失败: 请求超时")
            return False, "请求超时"
        except requests.exceptions.ConnectionError:
            self.logger.error("❌ OMS登录失败: 连接错误")
            return False, "连接错误"
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "未知"
            self.logger.error(f"❌ OMS登录失败 (HTTP {status_code}): {str(e)}")
            return False, f"HTTP错误: {status_code}"
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ OMS登录失败 (JSON解析错误): {str(e)}")
            return False, "JSON解析错误"
        except Exception as e:
            self.logger.error(f"❌ OMS登录失败 (未知错误): {str(e)}")
            return False, f"未知错误: {str(e)}"

    def _is_token_valid(self) -> bool:
        """检查token是否有效"""
        if not self.token or not self.token_expiry:
            return False

        # 考虑刷新缓冲时间
        return datetime.now() < (self.token_expiry - self.token_refresh_buffer)

    def _ensure_login(self) -> bool:
        """确保已登录，自动处理token刷新"""
        if not self._is_token_valid():
            success, message = self.login()
            if not success:
                self.logger.error(f"❌ 自动登录失败: {message}")
                return False
        return True

    def get_pgm_distribution_status(self,
                                    begin_date: Optional[str] = None,
                                    end_date: Optional[str] = None,
                                    factory_id: str = "OSMOD",
                                    company_id: str = "HITECH",
                                    force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        获取PGM分发状态

        Args:
            begin_date: 开始日期 (格式: "YYYY-MM-DD HH:MM:SS")
            end_date: 结束日期 (格式: "YYYY-MM-DD HH:MM:SS")
            factory_id: 工厂ID
            company_id: 公司ID
            force_refresh: 是否强制刷新缓存

        Returns:
            PGM分发状态列表
        """
        # 生成缓存键
        cache_key = hashlib.md5(
            f"distribution_{factory_id}_{company_id}_{begin_date}_{end_date}".encode()
        ).hexdigest()

        # 检查缓存
        if not force_refresh and cache_key in self.cache:
            cache_data, cache_time = self.cache[cache_key]
            if time.time() - cache_time < self.cache_ttl:
                self.logger.debug(f"📦 使用缓存的PGM分发状态数据")
                return cache_data

        try:
            # 确保已登录
            if not self._ensure_login():
                self.logger.error("❌ 获取PGM分发状态失败: 登录无效")
                return []

            # 设置默认日期范围
            if not begin_date or not end_date:
                today = datetime.now()
                begin_date = (today - timedelta(days=11)).strftime("%Y-%m-%d 07:00:00")
                end_date = (today + timedelta(days=1)).strftime("%Y-%m-%d 07:00:00")

            # 构建URL和参数
            url = urljoin(self.api_base, self.endpoints['distribute_status'])

            params = {
                "factoryId": factory_id,
                "companyId": company_id,
                "beginDate": begin_date,
                "endDate": end_date
            }

            self.logger.info(f"📋 获取PGM分发状态: {begin_date} 到 {end_date}")
            self.logger.debug(f"请求URL: {url}")
            self.logger.debug(f"请求参数: {params}")

            # 发送请求
            start_time = time.time()
            response = self.session.get(
                url,
                params=params,
                headers=self._get_headers('distribute_status'),
                timeout=self.timeouts['list']
            )
            response_time = time.time() - start_time

            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                self.logger.error(f"❌ 响应格式错误，期望列表，得到: {type(data)}")
                return []

            self.logger.info(f"✅ 成功获取PGM分发状态，共{len(data)}条记录")
            self.logger.debug(f"响应时间: {response_time:.2f}秒")

            # 缓存结果
            self.cache[cache_key] = (data, time.time())

            return data

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "未知"

            if status_code in [401, 403]:
                self.logger.info("🔄 Token过期或无效，尝试重新登录...")
                self.token = None
                self.token_expiry = None

                # 重新登录并重试
                if self._ensure_login():
                    return self.get_pgm_distribution_status(
                        begin_date, end_date, factory_id, company_id, True
                    )

            self.logger.error(f"❌ 获取PGM分发状态失败 (HTTP {status_code}): {str(e)}")
            return []
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 获取PGM分发状态失败 (网络错误): {str(e)}")
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ 获取PGM分发状态失败 (JSON解析错误): {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"❌ 获取PGM分发状态失败 (未知错误): {str(e)}")
            return []

    def get_new_pgms(self, last_check_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        获取新的PGM（自上次检查后新增的）

        Args:
            last_check_time: 上次检查时间

        Returns:
            新的PGM列表
        """
        try:
            # 如果没有指定上次检查时间，默认检查过去1天的数据
            if not last_check_time:
                last_check_time = datetime.now() - timedelta(days=1)

            begin_date = last_check_time.strftime("%Y-%m-%d %H:%M:%S")
            end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            all_pgms = self.get_pgm_distribution_status(begin_date, end_date)

            # 过滤出新的PGM（这里可以根据业务逻辑进一步过滤）
            new_pgms = []
            for pgm in all_pgms:
                # 示例过滤逻辑：只获取特定状态或类型的PGM
                work_type_desc = pgm.get('workTypeDesc', '')
                complete_yn = pgm.get('completeYn', '')

                # 可以根据实际需求调整过滤条件
                if 'PGM' in str(pgm.get('processName', '')).upper():
                    new_pgms.append(pgm)

            self.logger.info(f"📊 发现 {len(new_pgms)} 个新的PGM")

            return new_pgms

        except Exception as e:
            self.logger.error(f"❌ 获取新PGM失败: {str(e)}")
            return []

    def get_file_info_from_pgm(self, pgm_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        从PGM记录中提取文件信息

        Args:
            pgm_record: PGM分发状态记录

        Returns:
            文件信息字典
        """
        file_info = {
            'draft_id': pgm_record.get('draftId'),
            'process_id': pgm_record.get('processId'),
            'process_name': pgm_record.get('processName'),
            'work_sequence': pgm_record.get('workSequence'),
            'user_name': pgm_record.get('userName'),
            'work_start_tm': pgm_record.get('workStartTm'),
            'complete_yn': pgm_record.get('completeYn'),
            'pgm_type': self._determine_pgm_type(pgm_record)
        }

        # 猜测可能的文件类型
        process_name = str(pgm_record.get('processName', '')).upper()
        if 'ET' in process_name:
            file_info['expected_extensions'] = ['.obj', '.zip']
        elif 'AT' in process_name:
            file_info['expected_extensions'] = ['.xml', '.zip']
        else:
            file_info['expected_extensions'] = ['.zip', '.xml', '.obj']

        return file_info

    def _determine_pgm_type(self, pgm_record: Dict[str, Any]) -> str:
        """确定PGM类型"""
        process_name = str(pgm_record.get('processName', '')).upper()

        if 'ET' in process_name:
            return 'ET'
        elif 'AT' in process_name:
            return 'AT'
        elif 'HESS' in process_name:
            return 'HESS'
        else:
            return 'UNKNOWN'

    def get_et_pgm_details(self,
                           process_id: str,
                           work_sequence: int) -> Optional[Dict[str, Any]]:
        """
        获取ET PGM详细信息

        Args:
            process_id: 流程ID (UUID格式)
            work_sequence: 工作序列号

        Returns:
            ET PGM详细信息
        """
        return self._get_pgm_details('et', process_id, work_sequence)

    def get_at_pgm_details(self,
                           process_id: str,
                           work_sequence: int) -> Optional[Dict[str, Any]]:
        """
        获取AT PGM详细信息

        Args:
            process_id: 流程ID (UUID格式)
            work_sequence: 工作序列号

        Returns:
            AT PGM详细信息
        """
        return self._get_pgm_details('at', process_id, work_sequence)

    def _get_pgm_details(self,
                         pgm_type: str,
                         process_id: str,
                         work_sequence: int) -> Optional[Dict[str, Any]]:
        """
        通用方法获取PGM详情

        Args:
            pgm_type: 'et' 或 'at'
            process_id: 流程ID
            work_sequence: 工作序列号

        Returns:
            PGM详细信息
        """
        try:
            if not self._ensure_login():
                self.logger.error(f"❌ 获取{pgm_type.upper()}详情失败: 登录无效")
                return None

            # 构建URL
            endpoint_key = f"{pgm_type}_detail"
            url = urljoin(self.api_base, self.endpoints[endpoint_key])

            params = {
                "processId": process_id,
                "workSequence": work_sequence
            }

            self.logger.info(
                f"📋 获取{pgm_type.upper()} PGM详情: process_id={process_id}, work_sequence={work_sequence}")

            # 发送请求
            start_time = time.time()
            response = self.session.get(
                url,
                params=params,
                headers=self._get_headers(endpoint_key),
                timeout=self.timeouts['detail']
            )
            response_time = time.time() - start_time

            response.raise_for_status()

            data = response.json()

            self.logger.info(f"✅ 成功获取{pgm_type.upper()} PGM详情，响应时间: {response_time:.2f}秒")

            # 提取关键信息
            if pgm_type == 'et':
                extracted_data = self._extract_et_info(data, process_id, work_sequence)
            else:
                extracted_data = self._extract_at_info(data, process_id, work_sequence)

            return extracted_data

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "未知"
            self.logger.error(f"❌ 获取{pgm_type.upper()} PGM详情失败 (HTTP {status_code}): {str(e)}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 获取{pgm_type.upper()} PGM详情失败 (网络错误): {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ 获取{pgm_type.upper()} PGM详情失败 (JSON解析错误): {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"❌ 获取{pgm_type.upper()} PGM详情失败 (未知错误): {str(e)}")
            return None

    def _extract_et_info(self, data: Dict[str, Any], process_id: str, work_sequence: int) -> Dict[str, Any]:
        """提取ET信息"""
        try:
            extracted = {
                'process_id': process_id,
                'work_sequence': work_sequence,
                'type': 'ET',
                'extracted_at': datetime.now().isoformat(),
                'pgm_records': [],
                'file_info': [],
                'work_info': {}
            }

            # 提取工作详情
            if 'workDetailViews' in data and data['workDetailViews']:
                work_detail = data['workDetailViews'][0]
                extracted['work_info'] = {
                    'process_name': work_detail.get('processName'),
                    'work_name': work_detail.get('workName'),
                    'status': work_detail.get('status'),
                    'process_start_time': work_detail.get('processStartTime'),
                    'process_end_time': work_detail.get('processEndTime'),
                    'work_start_time': work_detail.get('workStartTime'),
                    'work_end_time': work_detail.get('workEndTime'),
                    'factory': work_detail.get('factory'),
                    'specified_work_user': work_detail.get('specifiedWorkUserName')
                }

                # 提取文件信息
                if 'file' in work_detail and work_detail['file']:
                    for file_item in work_detail['file']:
                        file_info = {
                            'file_download_id': file_item.get('fileDownloadId'),
                            'file_name': file_item.get('fileName'),
                            'size': file_item.get('size')
                        }
                        extracted['file_info'].append(file_info)

            # 提取PGM记录
            if 'testProgramModuleDramEtViews' in data:
                for pgm_record in data['testProgramModuleDramEtViews']:
                    pgm_info = {
                        'draft_id': pgm_record.get('draftId'),
                        'pgm_id': pgm_record.get('pgmId'),
                        'pgm_rev_ver': pgm_record.get('pgmRevVer'),
                        'pgm_dir': pgm_record.get('pgmDir'),
                        'pgm_dir2': pgm_record.get('pgmDir2'),
                        'pgm_dir3': pgm_record.get('pgmDir3'),
                        'pgm_dir4': pgm_record.get('pgmDir4'),
                        'equipment_model_code': pgm_record.get('equipmentModelCode'),
                        'operation_id': pgm_record.get('operationId'),
                        'module_type': pgm_record.get('moduleType'),
                        'product_type': pgm_record.get('productType'),
                        'tech_nm': pgm_record.get('techNm'),
                        'pkg_den_typ': pgm_record.get('pkgDenTyp'),
                        'organiz_cd': pgm_record.get('organizCd'),
                        'den_typ': pgm_record.get('denTyp'),
                        'change_date_time': pgm_record.get('changeDateTime'),
                        'factory_id': pgm_record.get('factoryId')
                    }
                    extracted['pgm_records'].append(pgm_info)

            self.logger.info(f"📊 提取ET信息: {len(extracted['pgm_records'])}条PGM记录")
            return extracted

        except Exception as e:
            self.logger.error(f"❌ 提取ET信息失败: {str(e)}")
            return {}

    def _extract_at_info(self, data: Dict[str, Any], process_id: str, work_sequence: int) -> Dict[str, Any]:
        """提取AT信息"""
        try:
            extracted = {
                'process_id': process_id,
                'work_sequence': work_sequence,
                'type': 'AT',
                'extracted_at': datetime.now().isoformat(),
                'pgm_records': [],
                'file_info': [],
                'work_info': {}
            }

            # 提取工作详情
            if 'workDetailViews' in data and data['workDetailViews']:
                work_detail = data['workDetailViews'][0]
                extracted['work_info'] = {
                    'process_name': work_detail.get('processName'),
                    'work_name': work_detail.get('workName'),
                    'status': work_detail.get('status'),
                    'process_start_time': work_detail.get('processStartTime'),
                    'process_end_time': work_detail.get('processEndTime'),
                    'work_start_time': work_detail.get('workStartTime'),
                    'work_end_time': work_detail.get('workEndTime'),
                    'factory': work_detail.get('factory'),
                    'specified_work_user': work_detail.get('specifiedWorkUserName')
                }

                # 提取文件信息
                if 'file' in work_detail and work_detail['file']:
                    for file_item in work_detail['file']:
                        file_info = {
                            'file_download_id': file_item.get('fileDownloadId'),
                            'file_name': file_item.get('fileName'),
                            'size': file_item.get('size')
                        }
                        extracted['file_info'].append(file_info)

            # 提取PGM记录
            if 'testProgramModuleDramAtViews' in data:
                for pgm_record in data['testProgramModuleDramAtViews']:
                    pgm_info = {
                        'draft_id': pgm_record.get('draftId'),
                        'pgm_id': pgm_record.get('pgmId'),
                        'pgm_rev_ver': pgm_record.get('pgmRevVer'),
                        'pgm_dir': pgm_record.get('pgmDir'),
                        'hdiag_dir': pgm_record.get('hdiagDir'),
                        'test_board_id': pgm_record.get('testBoardId'),
                        'operation_id': pgm_record.get('operationId'),
                        'module_type': pgm_record.get('moduleType'),
                        'product_type': pgm_record.get('productType'),
                        'tech_nm': pgm_record.get('techNm'),
                        'pkg_den_typ': pgm_record.get('pkgDenTyp'),
                        'sap_history_code': pgm_record.get('sapHistoryCode'),
                        'temper_val': pgm_record.get('temperVal'),
                        'change_date_time': pgm_record.get('changeDateTime'),
                        'factory_id': pgm_record.get('factoryId')
                    }
                    extracted['pgm_records'].append(pgm_info)

            self.logger.info(f"📊 提取AT信息: {len(extracted['pgm_records'])}条PGM记录")
            return extracted

        except Exception as e:
            self.logger.error(f"❌ 提取AT信息失败: {str(e)}")
            return {}

    def get_pgm_details_with_files(self,
                                   process_id: str,
                                   work_sequence: int,
                                   pgm_type: str = None) -> Optional[Dict[str, Any]]:
        """
        获取PGM详情（包含文件信息）

        Args:
            process_id: 流程ID
            work_sequence: 工作序列
            pgm_type: PGM类型（ET/AT），如果为None则自动判断

        Returns:
            PGM详情（包含文件信息）
        """
        try:
            if not self._ensure_login():
                self.logger.error("❌ 获取PGM详情失败: 登录无效")
                return None

            # 根据类型选择端点
            if pgm_type == 'ET' or pgm_type == 'et':
                endpoint = 'et_detail'
            elif pgm_type == 'AT' or pgm_type == 'at':
                endpoint = 'at_detail'
            else:
                # 默认使用ET
                endpoint = 'et_detail'
                self.logger.warning("⚠️ 未指定PGM类型，默认使用ET")

            url = urljoin(self.api_base, self.endpoints[endpoint])

            params = {
                "processId": process_id,
                "workSequence": work_sequence
            }

            self.logger.info(f"📋 获取PGM详情: {process_id}, 类型: {pgm_type or '自动'}")

            response = self.session.get(
                url,
                params=params,
                headers=self._get_headers(endpoint),
                timeout=self.timeouts['detail']
            )

            response.raise_for_status()
            data = response.json()

            # 提取文件信息
            file_list = []

            # 从workDetailViews中提取
            if 'workDetailViews' in data and data['workDetailViews']:
                work_detail = data['workDetailViews'][0]
                if 'file' in work_detail and work_detail['file']:
                    for file_item in work_detail['file']:
                        file_info = {
                            'file_download_id': file_item.get('fileDownloadId'),
                            'file_name': file_item.get('fileName'),
                            'size': file_item.get('size'),
                            'type': 'attachment'  # 附件文件
                        }
                        file_list.append(file_info)

            # 从PGM记录中提取可能的文件信息
            pgm_records = []
            if 'testProgramModuleDramEtViews' in data:
                pgm_records.extend(data['testProgramModuleDramEtViews'])
            if 'testProgramModuleDramAtViews' in data:
                pgm_records.extend(data['testProgramModuleDramAtViews'])

            for pgm_record in pgm_records:
                # 提取可能的文件路径信息
                pgm_dir = pgm_record.get('pgmDir')
                if pgm_dir:
                    file_info = {
                        'file_download_id': None,  # 需要单独获取
                        'file_name': os.path.basename(pgm_dir) if pgm_dir else None,
                        'pgm_dir': pgm_dir,
                        'type': 'pgm_file'
                    }
                    file_list.append(file_info)

            result = {
                'process_id': process_id,
                'work_sequence': work_sequence,
                'pgm_type': pgm_type,
                'files': file_list,
                'raw_data': data  # 保存原始数据用于调试
            }

            self.logger.info(f"✅ 获取到 {len(file_list)} 个文件信息")
            return result

        except Exception as e:
            self.logger.error(f"❌ 获取PGM详情失败: {str(e)}")
            return None

    def download_file(self,
                      file_download_id: str,
                      file_name: str,
                      process_id: str,
                      work_sequence: int,
                      save_dir: str = None,
                      custom_filename: str = None) -> Tuple[bool, str]:
        """
        下载单个文件

        Args:
            file_download_id: 文件下载ID
            file_name: 原始文件名
            process_id: 流程ID
            work_sequence: 工作序列
            save_dir: 保存目录（如果为None则使用配置路径）
            custom_filename: 自定义文件名

        Returns:
            (是否成功, 保存的文件路径或错误信息)
        """
        try:
            if not self._ensure_login():
                return False, "下载文件失败: 登录无效"

            # 确定保存目录
            if save_dir is None:
                save_dir = self.file_paths['local_verify']

            # 创建目录（如果不存在）
            save_path_obj = Path(save_dir)
            save_path_obj.mkdir(parents=True, exist_ok=True)

            # 确定文件名
            if custom_filename:
                filename = custom_filename
            else:
                # 清理文件名，移除非法字符
                filename = self._sanitize_filename(file_name)

            # 构建完整保存路径
            save_path = save_path_obj / filename

            # 构建下载URL和参数
            download_url = urljoin(self.api_base, self.endpoints['file_download'])

            params = {
                "id": file_download_id,
                "userId": self.credentials.get('user_id', ''),
                "bpmsProcessId": process_id,
                "bpmsWorkSequence": work_sequence,
                "uiId": "ModuleTestPgmDistributeStatus"
            }

            self.logger.info(f"📥 开始下载文件: {filename}")
            self.logger.debug(f"下载URL: {download_url}")
            self.logger.debug(f"参数: {params}")

            # 发送下载请求
            response = self.session.get(
                download_url,
                params=params,
                headers=self._get_headers('file_download'),
                stream=True,
                timeout=self.timeouts['download']
            )

            response.raise_for_status()

            # 尝试从Content-Disposition获取文件名
            content_disposition = response.headers.get('content-disposition', '')
            if 'filename=' in content_disposition:
                match = re.search(r'filename="?([^"]+)"?', content_disposition)
                if match:
                    server_filename = match.group(1)
                    filename = self._sanitize_filename(server_filename)
                    save_path = save_path_obj / filename
                    self.logger.info(f"📝 使用服务器提供的文件名: {filename}")

            # 获取文件大小
            total_size = int(response.headers.get('content-length', 0))
            content_type = response.headers.get('content-type', 'application/octet-stream')

            self.logger.info(f"📄 文件类型: {content_type}, 大小: {self._format_size(total_size)}")

            # 下载文件
            downloaded = 0
            start_time = time.time()

            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        # 显示下载进度
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            current_time = time.time()
                            elapsed = current_time - start_time

                            # 每下载5MB或5秒显示一次进度
                            if downloaded % (5 * 1024 * 1024) == 0 or elapsed > 5:
                                speed = downloaded / elapsed / 1024 if elapsed > 0 else 0
                                self.logger.info(
                                    f"📥 下载进度: {percent:.1f}% ({self._format_size(downloaded)}/{self._format_size(total_size)}) "
                                    f"速度: {speed:.1f} KB/s"
                                )

            # 验证文件
            actual_size = os.path.getsize(save_path)
            download_time = time.time() - start_time

            if total_size > 0 and actual_size != total_size:
                self.logger.warning(
                    f"⚠️ 文件大小不匹配: 期望={self._format_size(total_size)}, 实际={self._format_size(actual_size)}")
            else:
                self.logger.info(f"✅ 文件大小验证通过")

            # 计算下载速度
            speed = actual_size / download_time / 1024 if download_time > 0 else 0

            self.logger.info(f"✅ 文件下载完成: {save_path}")
            self.logger.info(f"📊 下载统计 - 大小: {self._format_size(actual_size)}, "
                             f"时间: {download_time:.2f}秒, 速度: {speed:.1f} KB/s")

            # 计算文件哈希（可选）
            file_hash = self._calculate_file_hash(str(save_path))
            if file_hash:
                self.logger.debug(f"🔢 文件MD5: {file_hash}")

            return True, str(save_path)

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "未知"
            error_msg = f"下载失败 (HTTP {status_code}): {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            return False, error_msg
        except Exception as e:
            error_msg = f"下载失败: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            return False, error_msg

    def download_all_pgm_files(self,
                               pgm_record: Dict[str, Any],
                               target_dir: str = None) -> Dict[str, Any]:
        """
        下载PGM所有相关文件

        Args:
            pgm_record: PGM记录（从get_pgm_distribution_status获取）
            target_dir: 目标目录（如果为None则使用配置路径）

        Returns:
            下载结果汇总
        """
        results = {
            'success': False,
            'pgm_info': {},
            'downloaded_files': [],
            'failed_files': [],
            'total_size': 0,
            'total_time': 0,
            'save_directory': ''
        }

        try:
            process_id = pgm_record.get('processId')
            work_sequence = pgm_record.get('workSequence')

            if not process_id or not work_sequence:
                results['error'] = "缺少process_id或work_sequence"
                return results

            # 提取PGM信息
            pgm_info = self.get_file_info_from_pgm(pgm_record)
            results['pgm_info'] = pgm_info

            # 确定保存目录
            if target_dir is None:
                # 使用配置路径 + PGM类型 + draft_id
                base_dir = self.file_paths['local_verify']
                pgm_type = pgm_info.get('pgm_type', 'UNKNOWN')
                draft_id = pgm_info.get('draft_id', 'unknown')
                date_str = datetime.now().strftime("%Y%m%d")

                target_dir = os.path.join(base_dir, date_str, f"{pgm_type}_{draft_id}")

            # 确保目录存在
            Path(target_dir).mkdir(parents=True, exist_ok=True)
            results['save_directory'] = target_dir

            self.logger.info(f"📂 保存到目录: {target_dir}")

            # 获取PGM详情（包含文件信息）
            pgm_type = pgm_info.get('pgm_type')
            details = self.get_pgm_details_with_files(process_id, work_sequence, pgm_type)

            if not details or 'files' not in details:
                results['error'] = "获取PGM文件信息失败"
                return results

            # 下载所有文件
            start_time = time.time()
            downloaded_count = 0
            failed_count = 0

            for file_info in details['files']:
                file_download_id = file_info.get('file_download_id')
                file_name = file_info.get('file_name', f"file_{file_download_id or 'unknown'}")

                if not file_download_id:
                    self.logger.warning(f"⚠️ 文件 {file_name} 没有download_id，跳过")
                    continue

                self.logger.info(f"⬇️  下载文件: {file_name}")

                success, file_path = self.download_file(
                    file_download_id=file_download_id,
                    file_name=file_name,
                    process_id=process_id,
                    work_sequence=work_sequence,
                    save_dir=target_dir
                )

                if success:
                    downloaded_count += 1
                    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0

                    results['downloaded_files'].append({
                        'file_name': file_name,
                        'file_path': file_path,
                        'size': file_size,
                        'type': file_info.get('type', 'unknown')
                    })
                    results['total_size'] += file_size
                else:
                    failed_count += 1
                    results['failed_files'].append({
                        'file_name': file_name,
                        'error': file_path  # 这里file_path实际上是错误信息
                    })

            # 计算总时间
            total_time = time.time() - start_time
            results['total_time'] = total_time

            # 判断是否成功
            if downloaded_count > 0:
                results['success'] = True

            self.logger.info(f"📊 下载完成 - 成功: {downloaded_count}, 失败: {failed_count}, "
                             f"总大小: {self._format_size(results['total_size'])}, "
                             f"总时间: {total_time:.2f}秒")

            # 如果下载了文件，创建一个摘要文件
            if results['downloaded_files']:
                self._create_download_summary(results, target_dir)

            return results

        except Exception as e:
            error_msg = f"下载PGM文件失败: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            results['error'] = error_msg
            return results

    def _sanitize_filename(self, filename: str) -> str:
        """清理文件名，移除非法字符"""
        if not filename:
            return "unknown_file"

        # 移除非法字符
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')

        # 限制长度
        if len(filename) > 255:
            name, ext = os.path.splitext(filename)
            filename = name[:250 - len(ext)] + ext

        return filename

    def _format_size(self, size_bytes: int) -> str:
        """格式化文件大小"""
        if size_bytes == 0:
            return "0B"

        units = ['B', 'KB', 'MB', 'GB']
        i = 0
        size = float(size_bytes)

        while size >= 1024 and i < len(units) - 1:
            size /= 1024
            i += 1

        return f"{size:.2f} {units[i]}"

    def _calculate_file_hash(self, file_path: str, algorithm: str = 'md5') -> str:
        """计算文件哈希值"""
        if not os.path.exists(file_path):
            return ""

        hash_func = hashlib.new(algorithm)

        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_func.update(chunk)
            return hash_func.hexdigest()
        except:
            return ""

    def _create_download_summary(self, results: Dict[str, Any], target_dir: str):
        """创建下载摘要文件"""
        try:
            summary_path = Path(target_dir) / "download_summary.json"

            summary = {
                'timestamp': datetime.now().isoformat(),
                'pgm_info': results.get('pgm_info', {}),
                'download_stats': {
                    'successful': len(results.get('downloaded_files', [])),
                    'failed': len(results.get('failed_files', [])),
                    'total_size': results.get('total_size', 0),
                    'total_time': results.get('total_time', 0)
                },
                'downloaded_files': results.get('downloaded_files', []),
                'failed_files': results.get('failed_files', [])
            }

            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)

            self.logger.info(f"📝 创建下载摘要: {summary_path}")

        except Exception as e:
            self.logger.warning(f"⚠️ 创建下载摘要失败: {str(e)}")


class FileDownloadManager:
    """文件下载管理器"""

    def __init__(self):
        """初始化下载管理器"""
        self.logger = get_pgm_logger().get_logger('download_manager')
        self.oms_client = OMSClient()
        self.config = get_config()

    def download_new_pgm_files(self, days: int = 1) -> List[Dict[str, Any]]:
        """
        下载最近几天的新PGM文件

        Args:
            days: 检查最近几天的数据

        Returns:
            下载结果列表
        """
        all_results = []

        try:
            # 1. 登录
            self.logger.info(f"🔍 开始下载最近 {days} 天的新PGM文件")

            success, message = self.oms_client.login()
            if not success:
                self.logger.error(f"❌ 登录失败: {message}")
                return all_results

            # 2. 获取PGM分发状态
            today = datetime.now()
            begin_date = (today - timedelta(days=days)).strftime("%Y-%m-%d 00:00:00")
            end_date = today.strftime("%Y-%m-%d 23:59:59")

            self.logger.info(f"📋 获取PGM分发状态: {begin_date} 到 {end_date}")

            # 这里需要调用原始的OMS客户端获取数据
            # 由于模块依赖，我们先简单实现
            # 实际使用时应该使用原始的OMSClient

            # 3. 模拟下载测试文件
            self.logger.info("⚠️  注意：这是模拟下载，需要真实的file_download_id才能下载实际文件")

            # 创建测试目录结构
            self._create_test_directory_structure()

            # 下载示例文件
            test_result = self._download_example_file()
            if test_result:
                all_results.append(test_result)

            return all_results

        except Exception as e:
            self.logger.error(f"❌ 下载PGM文件失败: {str(e)}")
            return all_results

    def _create_test_directory_structure(self):
        """创建测试目录结构"""
        try:
            file_paths = self.config.get_file_paths()
            verify_dir = file_paths['local_verify']

            # 创建主目录
            Path(verify_dir).mkdir(parents=True, exist_ok=True)

            # 创建子目录示例
            subdirs = ['ET', 'AT', 'HESS', 'ARCHIVE']
            for subdir in subdirs:
                Path(verify_dir).joinpath(subdir).mkdir(exist_ok=True)

            self.logger.info(f"📁 创建目录结构: {verify_dir}")

        except Exception as e:
            self.logger.warning(f"⚠️ 创建目录结构失败: {str(e)}")

    def _download_example_file(self) -> Dict[str, Any]:
        """下载示例文件（用于测试）"""
        try:
            # 创建测试文件
            test_dir = self.config.get_file_paths()['local_verify']
            test_file_path = Path(test_dir) / "test_example.txt"

            with open(test_file_path, 'w') as f:
                f.write("这是一个测试文件，用于验证文件下载功能。\n")
                f.write(f"生成时间: {datetime.now().isoformat()}\n")
                f.write("实际文件下载需要有效的file_download_id。\n")

            file_size = os.path.getsize(test_file_path)

            result = {
                'success': True,
                'file_path': str(test_file_path),
                'size': file_size,
                'type': 'test',
                'message': '示例文件已创建，实际下载需要有效的file_download_id'
            }

            self.logger.info(f"📝 创建示例文件: {test_file_path}")
            return result

        except Exception as e:
            self.logger.error(f"❌ 创建示例文件失败: {str(e)}")
            return None


def real_download_test(file_download_id: str, process_id: str, work_sequence: int):
    """真实文件下载测试（需要有效的参数）"""
    print("=" * 60)
    print("真实文件下载测试")
    print("=" * 60)

    from utils.logger import get_pgm_logger
    logger = get_pgm_logger()
    logger.log_execution_start("真实文件下载测试")

    try:
        # 初始化客户端
        oms_client = OMSClient()

        # 登录
        success, message = oms_client.login()
        if not success:
            print(f"❌ 登录失败: {message}")
            return False

        print(f"✅ 登录成功")
        # 获取PGM List
        pgm_list = oms_client.get_new_pgms()
        print(f"📋 获取到的PGM列表: {pgm_list}")
        # 下载文件
        print(f"📥 开始下载文件...")
        print(f"   文件ID: {file_download_id}")
        print(f"   流程ID: {process_id}")
        print(f"   工作序列: {work_sequence}")

        success, result = oms_client.download_file(
            file_download_id=file_download_id,
            file_name=f"downloaded_file_{file_download_id[:8]}",
            process_id=process_id,
            work_sequence=work_sequence
        )

        if success:
            print(f"✅ 文件下载成功: {result}")

            # 显示文件信息
            if os.path.exists(result):
                file_size = os.path.getsize(result)
                print(f"📄 文件大小: {oms_client._format_size(file_size)}")

                # 检查文件类型
                import mimetypes
                file_type, _ = mimetypes.guess_type(result)
                print(f"📋 文件类型: {file_type or '未知'}")
        else:
            print(f"❌ 文件下载失败: {result}")

        logger.log_execution_end("真实文件下载测试", success=success)
        return success

    except Exception as e:
        logger.log_execution_end("真实文件下载测试", success=False, message=str(e))
        print(f"❌ 真实下载测试失败: {str(e)}")
        return False


class OMSDataProcessor:
    """OMS数据处理器"""

    def __init__(self):
        """初始化增强版数据处理器"""
        super().__init__()
        self.oms_client = OMSClient()  # 使用优化后的客户端

    def download_pgm_files(self,
                           pgm_data: Dict[str, Any],
                           save_base_dir: str = None) -> Dict[str, Any]:
        """
        下载PGM相关文件（HESS文件和PGM文件）

        Args:
            pgm_data: PGM数据，包含process_id和work_sequence
            save_base_dir: 保存基础目录

        Returns:
            下载结果字典
        """
        if not save_base_dir:
            config = get_config()
            save_base_dir = config.get_file_paths()['local_verify']

        results = {
            'hess_downloaded': False,
            'pgm_downloaded': False,
            'hess_path': None,
            'pgm_path': None,
            'errors': []
        }

        try:
            process_id = pgm_data.get('process_id')
            work_sequence = pgm_data.get('work_sequence')

            if not process_id or not work_sequence:
                results['errors'].append("缺少process_id或work_sequence")
                return results

            # 1. 先获取PGM详情（包含文件信息）
            pgm_type = 'et' if pgm_data.get('type') == 'ET' else 'at'

            if pgm_type == 'et':
                details = self.oms_client.get_et_pgm_details(process_id, work_sequence)
            else:
                details = self.oms_client.get_at_pgm_details(process_id, work_sequence)

            if not details:
                results['errors'].append("获取PGM详情失败")
                return results

            # 2. 创建保存目录
            save_dir = Path(save_base_dir) / f"{pgm_type.upper()}_{process_id}"
            save_dir.mkdir(parents=True, exist_ok=True)

            # 3. 下载文件
            file_infos = []

            # HESS文件
            for file_info in details.get('file_info', []):
                if 'hess' in file_info.get('fileName', '').lower() or '.xlsx' in file_info.get('fileName', ''):
                    file_infos.append({
                        **file_info,
                        'bpmsProcessId': process_id,
                        'bpmsWorkSequence': work_sequence
                    })

            # PGM文件（从pgm_records中获取）
            for pgm_record in details.get('pgm_records', []):
                # 这里可以根据需要提取PGM文件信息
                # 实际实现需要根据API返回的具体结构调整
                pass

            # 批量下载
            if file_infos:
                download_results = self.oms_client.batch_download_files(
                    file_infos, str(save_dir), max_concurrent=2
                )

                for result in download_results:
                    if result['success']:
                        if 'hess' in result['file_info'].get('fileName', '').lower():
                            results['hess_downloaded'] = True
                            results['hess_path'] = result['file_path']
                        else:
                            results['pgm_downloaded'] = True
                            results['pgm_path'] = result['file_path']
                    else:
                        results['errors'].append(f"文件下载失败: {result['error']}")

            return results

        except Exception as e:
            self.logger.error(f"❌ 下载PGM文件失败: {str(e)}")
            results['errors'].append(str(e))
            return results


if __name__ == "__main__":

    print("\n" + "=" * 60)
    real_download_test(
        file_download_id="7186051",
        process_id="85c7d1b3-aabb-4aef-88e1-b3a0e3de6d19",
        work_sequence=1
    )
