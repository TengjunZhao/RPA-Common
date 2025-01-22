import os
import shutil
from datetime import datetime


def copy_and_save_report(src_path, target_dir, report_month):
    # Create the new target directory
    new_dir = os.path.join(target_dir, report_month)  # Combine base directory with YYYYMM
    os.makedirs(new_dir, exist_ok=True)  # Create the directory if it doesn't exist

    # Construct the new file path
    new_file_path = os.path.join(new_dir, os.path.basename(src_path))

    # Copy the file to the new location (overwrite if exists)
    # shutil.copy2(src_path, new_file_path)

    print(f"Report successfully copied to: {new_file_path}")


if __name__ == '__main__':
    reportDir = r'D:\Sync\业务报告\1on1\不良Status.pptx'
    report_month = datetime.now().strftime('%Y%m')
    target_base_dir = r'D:\Sync\业务报告\1on1'
    copy_and_save_report(reportDir, target_base_dir, report_month)