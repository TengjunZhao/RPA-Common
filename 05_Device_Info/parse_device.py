# 增加log记录
import logging
import pymysql

# 原始文本Key → 数据库字段映射（空格替换下划线，对齐MySQL字段）
key_mapping = {
    "Product Density": "Product_Density",
    "Organization": "Organization",
    "Generation": "Generation",
    "Speed Code": "Speed_Code",
    "Module Type": "Module_Type",
    "Sales Type": "Sales_Type",
    "Die Density": "Die_Density",
    "PKG Type": "PKG_Type",
    "Number Of Die": "Number_Of_Die",
    "Power Supply": "Power_Supply",
    "Module Height": "Module_Height",
    "RCD Y/N": "RCD_Y/N",
    "RCD Vendor": "RCD_Vendor",
    "RCD Revision": "RCD_Revision",
    "Data Buffer Y/N": "Data_Buffer_Y/N",
    "Data Buffer Vendor": "Data_Buffer_Vendor",
    "Data Buffer Revision": "Data_Buffer_Revision",
    "PMIC Vendor": "PMIC_Vendor",
    "PMIC Revision": "PMIC_Revision",
    "PMIC Current": "PMIC_Current",
    "Thermal Sensor Y/N": "Thermal_Sensor_Y/N",
    "Heat Spreader Y/N": "Heat_Spreader_Y/N",
    "Tech Name": "Tech_Name",
    "Product Family": "Product_Family",
    "Module Config": "Module_Config",
    "Product Group ID": "Product_Group_ID",
    "Module Density": "Module_Density",
    "Product Mode": "Product_Mode",
    "EQ Factory": "EQ_Factory",
    "Marking Event Work Week": "Marking_Event_Work_Week",
    "PKG Material": "PKG_Material",
    "P&T Cost Info": "P&T_Cost_Info",
    "PKG Density": "PKG_Density",
    "Memory Depth": "Memory_Depth",
    "Module Data Width": "Module_Data_Width",
    "DRAM Tech Name": "DRAM_Tech_Name",
    "DRAM Tech": "DRAM_Tech",
    "DRAM Organization": "DRAM_Organization",
    "Module Special Info": "Module_Special_Info",
    "Net Die 200": "Net_Die_200",
    "Manufactoring Event Date": "Manufactoring_Event_Date",
    "Product Group": "Product_Group",
    "Manufacturing Event": "Manufacturing_Event",
    "New Product ID": "New_Product_ID",
    "Planning Product": "Planning_Product",
    "Division": "Division",
    "Product Type": "Product_Type",
    "Marking Event": "Marking_Event",
    "Product Tech": "Product_Tech",
    "Module Pin": "Module_Pin",
    "Die Type": "Die_Type",
    "Module Customer Specification": "Module_Customer_Specification",
    "Module Type2": "Module_Type2",
    "Customer Info": "Customer_Info",
    "FAB Process Reduc": "FAB_Process_Reduc",
    "Back End Process": "Back_End_Process",
    "Product Special Ha": "Product_Special_Ha",
    "Product Site Trans": "Product_Site_Trans",
    "FAB Process Reduc": "FAB_Process_Reduc",
    "Back End Process": "Back_End_Process",
    "Product Special Ha": "Product_Special_Ha",
    "Product Site Trans": "Product_Site_Trans",
}

def parse_device_text(path: str, mapping: dict) -> dict:
    result_dict = {}
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            strip_line = line.strip()
            # 过滤空行、表头行
            if not strip_line or strip_line.startswith("Property Name"):
                continue
            # 按2个及以上空格分割key/value
            parts = [p.strip() for p in split_multi_space(strip_line)]
            if len(parts) < 2:
                continue
            raw_key, raw_val = parts[0], parts[1]
            # 只保留映射表里存在的key
            if raw_key in mapping:
                db_field = mapping[raw_key]
                # 重复key不覆盖，只存第一次
                if db_field not in result_dict:
                    result_dict[db_field] = raw_val
            else:
                logging.info(f"未映射的key: {raw_key}")

    return result_dict


def split_multi_space(s: str):
    import re
    return re.split(r"\s{2,}", s)


def insert_device_info_to_db(config: dict, info: dict) -> None:
    """
    将info的内容写入config配置的数据库
    :param config: 数据库配置字典，包含host, user, passwd等
    :param info: 设备信息字典，包含要插入的字段和值
    :return: None
    """
    db_name = "modulemte"
    
    try:
        conn = pymysql.connect(
            host=config.get('host'),
            user=config.get('user'),
            passwd=config.get('passwd'),
            database=db_name,
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        
        if not info:
            logging.info("设备信息为空，无需插入")
            return
        
        columns = ", ".join([f"`{k}`" for k in info.keys()])
        placeholders = ", ".join(["%s"] * len(info))
        values = list(info.values())
        
        sql = f"INSERT INTO db_deviceinfo ({columns}) VALUES ({placeholders})"
        
        cursor.execute(sql, values)
        conn.commit()
        
        logging.info(f"成功插入设备信息，影响行数: {cursor.rowcount}")
        
    except pymysql.MySQLError as e:
        logging.error(f"数据库操作失败: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()



def main(mode: str)->None:
    """
    主函数：解析device info文件，并导入到数据库
    :param mode: 模式test或prod
    :return: None
    """
    # 1. 读取device info文件
    if mode == 'test':
        db_config = {
            "host": "localhost",
            "user": "remoteuser",
            "passwd": "password",
        }
        path = r"D:\Sync\临时存放\device.txt"
        # 指定log文件路径
        log_file = r"D:\Sync\临时存放\device.log"
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file, encoding="utf-8")
    else:
        db_config = {
            "host": "172.27.154.57",
            "user": "remoteuser",
            "passwd": "password",
        }
        path = r"\\172.277.188\Mod_TestE\05.Device value Check\device.txt"
        # 指定log文件路径
        log_file = r"\\172.277.188\Mod_TestE\05.Device value Check\device.log"
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file, encoding="utf-8")

    # 2. 解析device info文件
    device_info = parse_device_text(path, key_mapping)
    device_info['Device']= device_info['Planning_Product']

    # 3. 将device_info写入数据库
    insert_device_info_to_db(db_config, device_info)




if __name__ == '__main__':
    main('test')