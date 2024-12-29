import pymysql
import os
import glob
import pandas as pd


class DatabaseImporter:
    def __init__(self, db_config, table_name):
        self.db_config = db_config
        self.table_name = table_name
        self.connection = None

    def connect(self):
        """Establish a connection to the database."""
        self.connection = pymysql.connect(**self.db_config)
        print("Database connection established.")

    def disconnect(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def create_table_if_not_exists(self):
        """Create the table if it does not already exist."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            lot_id VARCHAR(12) NOT NULL,
            device VARCHAR(50),
            property VARCHAR(5),
            status VARCHAR(5),
            PRIMARY KEY (lot_id)
        );
        """
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            self.connection.commit()
            print("Table ensured to exist.")

    def insert_data(self, file_path):
        """Insert data from a file into the database."""
        property_value = "ET" if "et" in file_path.lower() else "AT"
        property_value = property_value.upper()

        category_value = "RT" if "rt" in file_path.lower() else "FGS"
        category_value = category_value.upper()

        with open(file_path, 'r') as file:
            lines = file.readlines()
            with self.connection.cursor() as cursor:
                for line in lines[1:]:  # Skip header
                    parts = line.strip().split('\t')
                    if len(parts) < 3:
                        continue

                    device = parts[0]
                    lot_id = parts[1] + parts[2]
                    status = '0'

                    insert_sql = f"""
                    INSERT INTO {self.table_name} (lot_id, device, property, status, category)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    device = VALUES(device), 
                    property = VALUES(property), 
                    status = VALUES(status),
                    category = VALUES(category);
                    """
                    cursor.execute(insert_sql, (lot_id, device, property_value, status, category_value))
                self.connection.commit()
                print(f"Data from {file_path} inserted successfully.")


def process_all_txt_files(directory, importer):
    """Process all .txt files in the given directory."""
    txt_files = glob.glob(os.path.join(directory, '*.txt'))
    for txt_file in txt_files:
        try:
            print(f"Processing file: {txt_file}")
            importer.insert_data(txt_file)
            # os.remove(txt_file)  # Delete the file after processing
            print(f"File {txt_file} processed and deleted.")
        except Exception as e:
            print(f"Error processing file {txt_file}: {e}")


class ProcessImporter:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None

    def connect(self):
        """Establish a connection to the database."""
        self.connection = pymysql.connect(**self.db_config)
        print("Database connection established.")

    def disconnect(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def getList(self):
        sql = f"""
                    SELECT lot_id, device, property, category FROM db_fgs_request WHERE status = '0'
                    """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchall()
        return res

    def getDeviceInfo(self, device):
        sql = f"""
                    SELECT Product_Mode, Tech_Name, Die_Density FROM db_deviceinfo WHERE device = '{device}'
                    """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchall()
        return res

    def getProcess(self, family, tech, density, category, property):
        sql = f"""
                    SELECT process, sc FROM db_fgs_process WHERE 
                    device_cmf7 = '{family}' AND
                    tech = '{tech}' AND
                    pkg_density = '{density}'AND
                    category = '{category}' AND
                    property = '{property}'
                    """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchall()
        return res

    def updateProcess(self, lot_id, ps):
        with self.connection.cursor() as cursor:
            # 更新表格信息
            update_sql = f"""
                        UPDATE db_fgs_request SET 
                        process = '{ps[0][0]}',
                        sc = '{ps[0][1]}'
                        WHERE lot_id = '{lot_id}'
                        """
            cursor.execute(update_sql)
        self.connection.commit()
        print(f"Data {lot_id} update successfully.")

    def getExportList(self):
        sql = f"""
                    SELECT lot_id, process, sc, category FROM db_fgs_request WHERE status = '0'
                    """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            res = cursor.fetchall()
        return res


def ProcessGenerate(config):
    pr = ProcessImporter(config)
    pr.connect()
    lots = pr.getList()
    for lot in lots:
        lot_id = lot[0]
        device = lot[1]
        property = lot[2]
        category = lot[3]
        # 获取Lot属性
        info = pr.getDeviceInfo(device)
        # 如过没有相关制品信息则跳过
        if not info:
            print(f'{lot[0]}, {device} Device infomation missing.')
            continue
        family = info[0][0]
        tech = info[0][1]
        density = info[0][2]
        ps = pr.getProcess(family, tech, density, category, property)
        if not ps:
            print(f'{lot[0]}, {device}, {family}, {tech}, {density}, {category}, {property} Process infomation missing.')
            continue
        pr.updateProcess(lot_id, ps)


def Importer(config, table_name, file_path):
    importer = DatabaseImporter(config, table_name)
    try:
        importer.connect()
        importer.create_table_if_not_exists()
        process_all_txt_files(file_path, importer)
    finally:
        importer.disconnect()

def Exporter(config, directory):
    ex = ProcessImporter(config)
    ex.connect()
    try:
        export_data = ex.getExportList()

        # Prepare data for export
        processed_data = []
        for row in export_data:
            lot_id = row[0]
            process = row[1]
            sc = row[2]
            category = row[3]
            bd_type = process_bd_type(process)
            flow_name = ""  # Empty field

            processed_data.append({
                "Lot ID": lot_id,
                "Process": process,
                "S/C": sc,
                "BD_Type": bd_type,
                "Case Name": category,
                "Flow Name": flow_name
            })

        # Convert to DataFrame
        df = pd.DataFrame(processed_data)

        # Export to Excel
        output_file = os.path.join(directory, "FGSOST.xlsx")
        df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"Data exported successfully to {output_file}.")

    finally:
        ex.disconnect()

def process_bd_type(process):
    """Extract BD_Type by retaining only spaces and semicolons."""
    return ''.join(c if c in {' ', ';'} else '' for c in process)


def main():
    db_config_local = {
        'host': 'localhost',
        'user': 'remoteuser',
        'password': 'password',
        'database': 'modulemte',
        'charset': 'utf8mb4'
    }
    db_config_apply = {
        'host': '172.27.154.57',
        'user': 'remoteuser',
        'password': 'password',
        'database': 'modulemte',
        'charset': 'utf8mb4'
    }
    table_name = 'db_fgs_request'
    directory = r'C:\Users\Tengjun Zhao\Desktop'  # Replace with the directory path containing the .txt files

    Importer(db_config_local, table_name, directory)
    ProcessGenerate(db_config_local)
    Exporter(db_config_local, directory)



if __name__ == '__main__':
    main()
