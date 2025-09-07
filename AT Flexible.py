import json
import os
import pandas as pd
from sqlalchemy import create_engine,text
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
    """数据库操作类，负责数据导入和查询"""

    def __init__(self, config):
        self.config = config
        self.engine = self._create_engine()

    def _create_engine(self):
        """创建MySQL数据库连接引擎"""
        try:
            # MySQL正确的连接字符串格式，使用pymysql驱动
            conn_str = f"mysql+pymysql://{self.config.username}:{self.config.password}@{self.config.host}/{self.config.db_name}?charset=utf8mb4"
            return create_engine(conn_str)
        except Exception as e:
            logger.error(f"创建MySQL数据库连接失败: {str(e)}")
            raise

    def import_process_setting(self, file_path):
        """导入Process Setting Table到cmsalpha.flw_modtst"""
        try:
            # 读取Excel，表头在第3行(索引2)，数据从第4行开始
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

            # 先清空表，再插入数据
            with self.engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE cmsalpha.flw_modtst"))
                conn.commit()
                df.to_sql("flw_modtst", self.engine, schema="cmsalpha", if_exists="append", index=False)

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

            # 先清空表，再插入数据
            with self.engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE cmsalpha.spc_flw_modtst"))
                conn.commit()
                df.to_sql("spc_flw_modtst", self.engine, schema="cmsalpha", if_exists="append", index=False)

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

            # 先清空表，再插入数据
            with self.engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE cmsalpha.db_wip"))
                conn.commit()
                df.to_sql("db_wip", self.engine, schema="cmsalpha", if_exists="append", index=False)

            logger.info(f"成功导入WIP Table: {file_path}")
            return True
        except Exception as e:
            logger.error(f"导入WIP Table失败: {str(e)}")
            return False

    def get_wip_data(self):
        """获取WIP数据"""
        try:
            query = "SELECT * FROM cmsalpha.db_wip"
            return pd.read_sql(query, self.engine)
        except Exception as e:
            logger.error(f"获取WIP数据失败: {str(e)}")
            return pd.DataFrame()

    def check_lot_in_special(self, lot_id, sub_no):
        """检查lot+sub是否在db_spc_flw_modtst中"""
        try:
            query = f"SELECT COUNT(*) as count FROM cmsalpha.spc_flw_modtst WHERE run_no = '{lot_id}' AND sub_no = '{sub_no}'"
            result = pd.read_sql(query, self.engine)
            return result.iloc[0]['count'] > 0
        except Exception as e:
            logger.error(f"检查lot是否在特殊流程中失败: {str(e)}")
            return False

    def get_device_info(self, new_device):
        """
        根据new_device查询modulemte.db_deviceinfo表的设备属性
        :param new_device: db_wip表中的new_device字段值
        :return: 设备属性字典（None表示未找到）
        """
        if not new_device:
            return None
        try:
            # 查询db_deviceinfo中与new_device匹配的记录（Device字段对应）
            query = text("""
                SELECT * FROM modulemte.db_deviceinfo 
                WHERE Device = :new_device
                LIMIT 1
            """)
            with self.engine.connect() as conn:
                result = conn.execute(query, {"new_device": new_device})
                row = result.fetchone()
                if row:
                    # 将查询结果转换为字典（字段名: 值）
                    return dict(zip(result.keys(), row))
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
        # 检查文件是否存在
        if not os.path.exists(self.config_path):
            logger.error(f"配置文件不存在: {self.config_path}")
            return False

        # 读取配置文件
        try:
            with open(self.config_path, 'r') as f:
                self.config_data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"解析config.json失败: {str(e)}")
            return False

        # 验证配置文件格式
        if not isinstance(self.config_data, list):
            logger.error("config.json应该是一个数组")
            return False

        required_fields = ["id", "material_properties", "distinguishing_conditions", "process_settings"]
        for rule in self.config_data:
            if not isinstance(rule, dict):
                logger.error(f"每个规则应该是一个JSON对象，无效规则: {rule}")
                return False

            # 检查必要字段
            for field in required_fields:
                if field not in rule:
                    logger.error(f"规则 {rule.get('id', 'unknown')} 缺少必要字段 '{field}'")
                    return False

            # 验证process_settings
            process_settings = rule.get("process_settings", {})
            if not all(key in process_settings for key in ["operList", "BDList", "SCList"]):
                logger.error(
                    f"规则 {rule.get('id', 'unknown')} 的process_settings必须包含 'operList', 'BDList' 和 'SCList'")
                return False

            # 验证数组长度是否一致
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

            # 存储生成的Process数据
            process_data = []

            # 遍历每个WIP记录
            for _, wip_row in wip_data.iterrows():
                lot_id = wip_row.get("lot_id")
                sub_no = wip_row.get("sub")
                if not lot_id or not sub_no:
                    logger.warning(f"Lot ID或Sub No为空，跳过记录: {lot_id}-{sub_no}")
                    continue

                # 处理Sub No偶数判断（支持带T等后缀格式，如02T视为偶数）
                try:
                    # 提取数字部分（移除非数字字符）
                    sub_num = int(''.join(filter(str.isdigit, str(sub_no))))
                    is_sub_even = (sub_num % 2 == 0)
                except (ValueError, TypeError):
                    logger.warning(f"Sub No格式异常，无法判断奇偶: {sub_no}，跳过")
                    continue

                # 遍历每条规则
                for rule in self.rules:
                    # 仅处理区分条件为Sub_Even的规则
                    if rule["distinguishing_conditions"] != "Sub_Even":
                        continue

                    # 非偶数Sub跳过
                    if not is_sub_even:
                        continue

                    # 检查lot+sub是否在Special Process表中，存在则跳过
                    if self.db_handler.check_lot_in_special(lot_id, sub_no):
                        continue

                    # 1. 检查device属性匹配（关联db_deviceinfo）
                    dd_properties = rule["material_properties"].get("dd", {})
                    if dd_properties:
                        # 根据db_wip的new_device查询db_deviceinfo获取属性
                        device_info = self.db_handler.get_device_info(wip_row.get("new_device"))
                        if not device_info:
                            logger.debug(f"未找到设备信息: {wip_row.get('new_device')}，跳过")
                            continue

                        # 验证dd属性匹配
                        dd_match = True
                        for prop, target_values in dd_properties.items():
                            if not target_values:  # 空配置跳过检查
                                continue

                            # 映射db_deviceinfo字段（参考字段映射表）
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

                    # 2. 检查dy属性匹配（Lot相关属性）
                    dy_properties = rule["material_properties"].get("dy", {})
                    if dy_properties:
                        dy_match = True
                        for prop, target_values in dy_properties.items():
                            if not target_values:  # 空配置跳过检查
                                continue

                            # 从WIP数据中获取属性值
                            actual_value = wip_row.get(prop.lower())
                            if actual_value not in target_values:
                                dy_match = False
                                break
                        if not dy_match:
                            continue

                    # 3. 生成Process数据（按规则拼接列表为字符串）
                    process_settings = rule["process_settings"]
                    # 拼接operList为Process字段（分号分隔，结尾分号）
                    process_str = ";".join(process_settings["operList"]) + ";"
                    # 拼接BDList（空值用分号占位，结尾分号）
                    bd_str = ";".join(process_settings["BDList"]) + ";"
                    # 拼接SCList（空值用分号占位，结尾分号）
                    sc_str = ";".join(process_settings["SCList"]) + ";"
                    # 生成Lot ID（原Lot+Sub组合）
                    full_lot_id = f"{lot_id}{sub_no}"

                    # 构建输出记录
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
                # 确保无空格（替换可能的空格）
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
    # 初始化配置
    config = Config()
    if mode == "test":
        config.host = "localhost"
        config.source_dir = r"D:\Python\25. At Speed Flexible"
    else:
        config.host = "172.27.154.57"
        config.source_dir = r"\\172.27.7.188\Mod_TestE\25. At Speed Flexible"

    # 检查源目录是否存在
    if not os.path.exists(config.source_dir):
        logger.error(f"源目录不存在: {config.source_dir}")
        return

    # 读取配置文件
    config_path = os.path.join(config.source_dir, "config.json")
    config_handler = ConfigHandler(config_path)
    if not config_handler.load_and_validate():
        return

    # 初始化数据库处理器
    db_handler = DatabaseHandler(config)

    # 定义数据源路径
    process_setting_path = os.path.join(config.source_dir, "Normal", "source.xlsx")
    special_process_path = os.path.join(config.source_dir, "Special", "source.xlsx")
    wip_path = os.path.join(config.source_dir, "WIP", "source.xlsx")

    # 检查源文件是否存在
    if not os.path.exists(process_setting_path):
        logger.error(f"Process Setting文件不存在: {process_setting_path}")
        return

    if not os.path.exists(special_process_path):
        logger.error(f"Special Process Setting文件不存在: {special_process_path}")
        return

    if not os.path.exists(wip_path):
        logger.error(f"WIP文件不存在: {wip_path}")
        return

    # 导入数据到数据库
    if not db_handler.import_process_setting(process_setting_path):
        return

    if not db_handler.import_special_process_setting(special_process_path):
        return

    if not db_handler.import_wip_table(wip_path):
        return

    # 删除源文件
    try:
        # os.remove(process_setting_path)
        # os.remove(special_process_path)
        # os.remove(wip_path)
        logger.info("源文件删除成功")
    except Exception as e:
        logger.error(f"删除源文件失败: {str(e)}")
        return

    # 生成Process
    output_path = os.path.join(config.source_dir, "process_output.xlsx")
    process_generator = ProcessGenerator(db_handler, config_handler)
    if not process_generator.generate(output_path):
        return

    logger.info("所有操作完成成功")


if __name__ == "__main__":
    # 可以根据需要切换"test"和"production"模式
    main("test")