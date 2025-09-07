import json
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Config:
    """配置类，存储数据库连接和文件路径信息"""

    def __init__(self):
        self.host = ""
        self.username = "remoteuser"
        self.password = "password"
        self.source_dir = ""
        self.db_name = "cmsalpha"  # 数据库名


class DatabaseHandler:
    """数据库操作类，负责数据导入和查询（兼容SQLAlchemy 2.0+）"""

    def __init__(self, config):
        self.config = config
        self.engine = self._create_engine()

    def _create_engine(self):
        """创建MySQL数据库连接引擎"""
        try:
            # MySQL连接字符串（使用pymysql驱动）
            conn_str = f"mysql+pymysql://{self.config.username}:{self.config.password}@{self.config.host}/{self.config.db_name}?charset=utf8mb4"
            return create_engine(conn_str)
        except Exception as e:
            logger.error(f"创建MySQL数据库连接失败: {str(e)}")
            raise

    def import_process_setting(self, file_path):
        """导入Process Setting Table到cmsalpha.flw_modtst"""
        try:
            # 读取Excel，表头在第3行(索引2)
            df = pd.read_excel(file_path, header=2)
            df.columns = df.columns.str.strip()
            # 数据映射
            column_mapping = {
                "Device": "device",
                "Fab": "fab",
                "Grade": "grade",
                "Owner": "owner_code",
                "Hist": "hist_code",
                "Mask": "mask",
                "Customer": "customer",
                "Process": "default_flow",
                "S/C": "special_code",
                "Flow": "flow",
                "BD Type": "bd_type",
                "Update User": "update_user",
                "Update Time": "update_time"
            }

            # 重命名列并筛选需要的列
            df = df.rename(columns=column_mapping)
            df = df[column_mapping.values()]

            # 添加更新时间（如果不存在）
            if "update_time" not in df.columns or df["update_time"].isna().all():
                df["update_time"] = datetime.now()

            # 使用engine.begin()自动管理事务（执行+提交）
            with self.engine.begin() as conn:
                # 清空表
                conn.execute(text("TRUNCATE TABLE cmsalpha.flw_modtst"))
                # 插入数据（使用当前事务连接）
                df.to_sql("flw_modtst", conn, schema="cmsalpha", if_exists="append", index=False)

            logger.info(f"成功导入Process Setting Table: {file_path}")
            return True
        except Exception as e:
            logger.error(f"导入Process Setting Table失败: {str(e)}")
            return False

    def import_special_process_setting(self, file_path):
        """导入Special Process Setting Table到cmsalpha.spc_flw_modtst"""
        try:
            # 读取Excel，表头在第3行(索引2)
            df = pd.read_excel(file_path, header=2)
            df.columns = df.columns.str.strip()
            # 数据映射
            column_mapping = {
                "FAB": "fab",
                "Device": "device",
                "Owner": "owner_code",
                "Grade": "grade",
                "Hist": "hist_code",
                "Mask": "mask",
                "Customer": "customer",
                "Run No": "run_no",
                "Sub No": "sub_no",
                "Process": "process",
                "S/C": "special_code",
                "Flow": "flow",
                "BD_Type": "bd_type",
                "Case Name": "case_name",
                "Update User": "update_user",
                "Update Time": "update_time",
                "Seq No": "seq_no",
            }

            # 重命名列
            df = df.rename(columns=column_mapping)

            # 添加默认值列
            df["discard_flag"] = "N"
            df["discard_time"] = "19000101000000"

            # 筛选需要的列
            required_columns = list(column_mapping.values()) + ["discard_flag", "discard_time"]
            df = df[required_columns]

            # 添加更新时间（如果不存在）
            if "update_time" not in df.columns or df["update_time"].isna().all():
                df["update_time"] = datetime.now()

            # 自动事务管理
            with self.engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE cmsalpha.spc_flw_modtst"))
                df.to_sql("spc_flw_modtst", conn, schema="cmsalpha", if_exists="append", index=False)

            logger.info(f"成功导入Special Process Setting Table: {file_path}")
            return True
        except Exception as e:
            logger.error(f"导入Special Process Setting Table失败: {str(e)}")
            return False

    def import_wip_table(self, file_path):
        """导入WIP Table到cmsalpha.db_wip"""
        try:
            # 读取Excel，表头在第7行(索引6)
            df = pd.read_excel(file_path, header=6)
            df.columns = df.columns.str.strip()
            # 数据映射
            column_mapping = {
                "Oper": "oper",
                "Desc": "description",
                "Fab": "fab",
                "Owner": "owner",
                "Grade": "grade",
                "Package Density": "package_density",
                "Tech": "tech",
                "Module Density": "module_density",
                "New Device": "new_device",
                "Lot ID": "lot_id",
                "Sub": "sub",
                "Qty": "qty",
                "EQ Qty": "eq_qty",
                "Flow": "flow",
                "Cap No": "cap_no",
                "Line": "line",
                "In TAT": "in_tat",
                "Date Code": "date_code",
                "Wafer": "wafer",
                "SAP Code": "sap_code",
                "From Oper": "from_oper",
                "Batch No": "batch_no",
                "S/C": "sc",
                "SFK": "sfk",
                "Temp": "temp",
                "Oper In Date": "oper_in_date",
                "Create Date": "create_date",
                "Hold Code": "hold_code",
                "Hold Flag": "hold_flag",
                "Last Comment": "last_comment",
                "Hot Flag": "hot_flag",
                "Rework Flag": "rework_flag",
                "ERN": "ern",
                "ERS": "ers",
                "ERD": "erd",
                "ARN": "arn",
                "ARS": "ars",
                "AHN": "ahn",
                "Family": "family",
                "Mode": "mode",
                "Module Config": "module_config",
                "Organization": "organization",
                "Module Type": "module_type",
                "Speed": "speed",
                "Device_Group": "device_group",
                "Lot Status": "lot_status",
                "Lot Group": "lot_group",
                "Position": "position",
                "Equip ID": "equip_id",
                "PGET Time": "pget_time",
                "IMVOU Time": "imvou_time",
                "Start Time": "start_time",
                "End Time": "end_time",
                "Affect S/N": "affect_sn",
                "MASK Rev": "mask_rev",
                "MTEST TAT": "mtest_tat",
                "WLPKG Site Cd": "wlpkg_site_cd",
                "New Marking": "new_marking",
                "RANK": "rank_code",
            }

            # 重命名列并筛选需要的列
            df = df.rename(columns=column_mapping)
            df = df[column_mapping.values()]

            # 自动事务管理
            with self.engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE cmsalpha.db_wip"))
                df.to_sql("db_wip", conn, schema="cmsalpha", if_exists="append", index=False)

            logger.info(f"成功导入WIP Table: {file_path}")
            return True
        except Exception as e:
            logger.error(f"导入WIP Table失败: {str(e)}")
            return False

    def get_wip_data(self):
        """获取WIP数据（兼容2.0+）"""
        try:
            query = text("SELECT * FROM cmsalpha.db_wip")
            # 显式获取连接并执行
            with self.engine.connect() as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            logger.error(f"获取WIP数据失败: {str(e)}")
            return pd.DataFrame()

    def check_lot_in_special(self, lot_id, sub_no):
        """检查lot+sub是否在db_spc_flw_modtst中（兼容2.0+）"""
        try:
            # 使用text()和参数化查询避免注入
            query = text("""
                SELECT COUNT(*) as count FROM cmsalpha.spc_flw_modtst 
                WHERE run_no = :lot_id AND sub_no = :sub_no
            """)
            # 显式获取连接
            with self.engine.connect() as conn:
                result = conn.execute(query, {"lot_id": lot_id, "sub_no": sub_no})
                return result.scalar() > 0  # 直接获取计数结果
        except Exception as e:
            logger.error(f"检查lot是否在特殊流程中失败: {str(e)}")
            return False

    def get_device_info(self, new_device):
        """查询modulemte.db_deviceinfo表的设备属性（兼容2.0+）"""
        if not new_device:
            return None
        try:
            query = text("""
                SELECT * FROM modulemte.db_deviceinfo 
                WHERE Device = :new_device
                LIMIT 1
            """)
            # 显式获取连接执行查询
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={"new_device": new_device})

            # 转换为字典返回
            if not df.empty:
                return df.iloc[0].to_dict()
            return None
        except Exception as e:
            logger.error(f"查询设备信息失败（new_device={new_device}）: {str(e)}")
            return None


class ConfigHandler:
    """配置文件处理类，负责读取和验证config.json"""

    def __init__(self, config_path):
        self.config_path = config_path
        self.config_data = None

    def load_and_validate(self):
        """加载并验证配置文件"""
        if not os.path.exists(self.config_path):
            logger.error(f"配置文件不存在: {self.config_path}")
            return False

        try:
            with open(self.config_path, 'r') as f:
                self.config_data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"解析config.json失败: {str(e)}")
            return False

        if not isinstance(self.config_data, list):
            logger.error("config.json应该是一个数组")
            return False

        required_fields = ["id", "material_properties", "distinguishing_conditions", "process_settings"]
        for rule in self.config_data:
            if not isinstance(rule, dict):
                logger.error(f"每个规则应该是一个JSON对象，无效规则: {rule}")
                return False

            for field in required_fields:
                if field not in rule:
                    logger.error(f"规则 {rule.get('id', 'unknown')} 缺少必要字段 '{field}'")
                    return False

            process_settings = rule.get("process_settings", {})
            if not all(key in process_settings for key in ["operList", "BDList", "SCList"]):
                logger.error(
                    f"规则 {rule.get('id', 'unknown')} 的process_settings必须包含 'operList', 'BDList' 和 'SCList'")
                return False

            if len(process_settings["operList"]) != len(process_settings["BDList"]) or len(
                    process_settings["operList"]) != len(process_settings["SCList"]):
                logger.error(f"规则 {rule.get('id', 'unknown')} 的operList, BDList和SCList长度必须一致")
                return False

        logger.info("配置文件加载并验证成功")
        return True

    def get_rules(self):
        """获取配置规则"""
        return self.config_data


class ProcessGenerator:
    """Process生成类，根据规则生成Process Excel"""

    def __init__(self, db_handler, config_handler):
        self.db_handler = db_handler
        self.config_handler = config_handler
        self.rules = config_handler.get_rules()

    def generate(self, output_path):
        """生成Process Excel"""
        try:
            # 获取WIP数据
            wip_data = self.db_handler.get_wip_data()
            if wip_data.empty:
                logger.warning("没有找到WIP数据")
                return False

            process_data = []

            for _, wip_row in wip_data.iterrows():
                lot_id = wip_row.get("lot_id")
                sub_no = wip_row.get("sub")
                if not lot_id or not sub_no:
                    logger.warning(f"Lot ID或Sub No为空，跳过记录: {lot_id}-{sub_no}")
                    continue

                # 处理Sub No偶数判断
                try:
                    sub_num = int(''.join(filter(str.isdigit, str(sub_no))))
                    is_sub_even = (sub_num % 2 == 0)
                except (ValueError, TypeError):
                    logger.warning(f"Sub No格式异常，无法判断奇偶: {sub_no}，跳过")
                    continue

                # 遍历规则
                for rule in self.rules:
                    if rule["distinguishing_conditions"] != "Sub_Even":
                        continue

                    if not is_sub_even:
                        continue

                    if self.db_handler.check_lot_in_special(lot_id, sub_no):
                        continue

                    # 检查device属性匹配
                    dd_properties = rule["material_properties"].get("dd", {})
                    if dd_properties:
                        device_info = self.db_handler.get_device_info(wip_row.get("new_device"))
                        if not device_info:
                            logger.debug(f"未找到设备信息: {wip_row.get('new_device')}，跳过")
                            continue

                        dd_match = True
                        for prop, target_values in dd_properties.items():
                            if not target_values:
                                continue

                            device_field_map = {
                                "mode": "Product_Mode",
                                "speed": "Speed_Code",
                                "product_mode": "Product_Mode",
                                "speed_code": "Speed_Code"
                            }
                            db_field = device_field_map.get(prop.lower(), prop)
                            actual_value = device_info.get(db_field)

                            if actual_value not in target_values:
                                dd_match = False
                                break
                        if not dd_match:
                            continue

                    # 检查dy属性匹配
                    dy_properties = rule["material_properties"].get("dy", {})
                    if dy_properties:
                        dy_match = True
                        for prop, target_values in dy_properties.items():
                            if not target_values:
                                continue

                            actual_value = wip_row.get(prop.lower())
                            if actual_value not in target_values:
                                dy_match = False
                                break
                        if not dy_match:
                            continue

                    # 生成Process数据
                    process_settings = rule["process_settings"]
                    process_str = ";".join(process_settings["operList"]) + ";"
                    bd_str = ";".join(process_settings["BDList"]) + ";"
                    sc_str = ";".join(process_settings["SCList"]) + ";"
                    full_lot_id = f"{lot_id}{sub_no}"

                    process_record = {
                        "Lot ID": full_lot_id,
                        "Process": process_str,
                        "S/C": sc_str,
                        "BD_Type": bd_str,
                        "Case Name": rule["id"]
                    }
                    process_data.append(process_record)

            # 保存为Excel
            if process_data:
                df = pd.DataFrame(process_data)
                for col in df.columns:
                    df[col] = df[col].astype(str).str.replace(" ", "")
                df.to_excel(output_path, index=False)
                logger.info(f"Process生成成功: {output_path}，共{len(process_data)}条记录")
                return True
            else:
                logger.warning("没有生成任何Process数据")
                return False

        except Exception as e:
            logger.error(f"生成Process失败: {str(e)}", exc_info=True)
            return False


def main(mode):
    """主函数"""
    config = Config()
    if mode == "test":
        config.host = "localhost"
        config.source_dir = r"D:\Python\25. At Speed Flexible"
    else:
        config.host = "172.27.154.57"
        config.source_dir = r"\\172.27.7.188\Mod_TestE\25. At Speed Flexible"

    if not os.path.exists(config.source_dir):
        logger.error(f"源目录不存在: {config.source_dir}")
        return

    config_path = os.path.join(config.source_dir, "config.json")
    config_handler = ConfigHandler(config_path)
    if not config_handler.load_and_validate():
        return

    db_handler = DatabaseHandler(config)

    process_setting_path = os.path.join(config.source_dir, "Normal", "source.xlsx")
    special_process_path = os.path.join(config.source_dir, "Special", "source.xlsx")
    wip_path = os.path.join(config.source_dir, "WIP", "source.xlsx")

    if not os.path.exists(process_setting_path):
        logger.error(f"Process Setting文件不存在: {process_setting_path}")
        return

    if not os.path.exists(special_process_path):
        logger.error(f"Special Process Setting文件不存在: {special_process_path}")
        return

    if not os.path.exists(wip_path):
        logger.error(f"WIP文件不存在: {wip_path}")
        return

    if not db_handler.import_process_setting(process_setting_path):
        return

    if not db_handler.import_special_process_setting(special_process_path):
        return

    if not db_handler.import_wip_table(wip_path):
        return

    # 删除源文件（注释仅为测试，实际使用时取消注释）
    try:
        # os.remove(process_setting_path)
        # os.remove(special_process_path)
        # os.remove(wip_path)
        logger.info("源文件删除成功")
    except Exception as e:
        logger.error(f"删除源文件失败: {str(e)}")
        return

    output_path = os.path.join(config.source_dir, "process_output.xlsx")
    process_generator = ProcessGenerator(db_handler, config_handler)
    if not process_generator.generate(output_path):
        return

    logger.info("所有操作完成成功")


if __name__ == "__main__":
    main("test")