"""
修复数据库字符集排序规则问题
"""
import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # scripts目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录

sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)

from utils.db_connection import execute_query


def check_collation():
    """检查数据库和表的排序规则"""
    print("检查数据库排序规则...")

    # 检查数据库排序规则
    db_result = execute_query(
        "SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME "
        "FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'cmsalpha'"
    )

    if db_result:
        print(f"数据库排序规则: {db_result[0]['DEFAULT_COLLATION_NAME']}")

    # 检查pgm_main表的排序规则
    table_result = execute_query(
        "SELECT TABLE_COLLATION FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = 'cmsalpha' AND TABLE_NAME = 'pgm_main'"
    )

    if table_result:
        print(f"pgm_main表排序规则: {table_result[0]['TABLE_COLLATION']}")

    # 检查列的排序规则
    column_result = execute_query(
        "SELECT COLUMN_NAME, COLLATION_NAME "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = 'cmsalpha' AND TABLE_NAME = 'pgm_main' "
        "AND COLLATION_NAME IS NOT NULL"
    )

    print(f"\n列排序规则检查:")
    for col in column_result:
        print(f"  - {col['COLUMN_NAME']}: {col['COLLATION_NAME']}")


def fix_pgm_main_collation():
    """修复pgm_main表的排序规则"""
    print("\n修复pgm_main表排序规则...")

    try:
        # 1. 将表转换为正确的排序规则
        sql = """
              ALTER TABLE pgm_main
                  CONVERT TO CHARACTER SET utf8mb4
                  COLLATE utf8mb4_0900_ai_ci \
              """

        execute_query(sql, fetch=False)
        print("✅ pgm_main表排序规则已修复")

        # 2. 检查修复结果
        check_collation()

        return True

    except Exception as e:
        print(f"❌ 修复排序规则失败: {str(e)}")
        return False


def check_other_tables():
    """检查其他相关表"""
    print("\n检查其他相关表...")

    tables = ['pgm_oms_history', 'pgm_alarm_history']

    for table in tables:
        result = execute_query(
            "SELECT TABLE_COLLATION FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = 'cmsalpha' AND TABLE_NAME = %s",
            (table,)
        )

        if result:
            print(f"{table}表排序规则: {result[0]['TABLE_COLLATION']}")
        else:
            print(f"⚠️  表 {table} 不存在")


def main():
    """主函数"""
    print("=" * 60)
    print("数据库字符集排序规则修复工具")
    print("=" * 60)

    try:
        # 1. 检查当前状态
        check_collation()

        # 2. 检查其他表
        check_other_tables()

        # 3. 询问是否修复
        print("\n" + "=" * 60)
        response = input("是否修复pgm_main表的排序规则？(y/N): ").strip().lower()

        if response == 'y':
            if fix_pgm_main_collation():
                print("\n✅ 修复完成！请重新运行示例程序测试。")
            else:
                print("\n❌ 修复失败")
        else:
            print("\n⚠️  跳过修复，请手动处理排序规则问题")

        print("\n建议解决方案:")
        print("1. 使用方案A: 修改数据库连接使用 utf8mb4 和 utf8mb4_0900_ai_ci")
        print("2. 使用方案B: 修改SQLAlchemy引擎配置")
        print("3. 使用本工具修复表排序规则")

        return 0

    except Exception as e:
        print(f"\n❌ 执行过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())