"""
项目启动脚本 - 用于设置Python路径和初始化环境
"""
import sys
import os
from pathlib import Path


def setup_environment():
    """
    设置Python环境路径
    """
    # 获取项目根目录
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent  # MTE_PGM_Automation目录

    # 将项目根目录添加到Python路径
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    # 将dev目录添加到Python路径
    dev_dir = project_root / "dev"
    if str(dev_dir) not in sys.path:
        sys.path.insert(0, str(dev_dir))

    print(f"📁 项目根目录: {project_root}")
    print(f"📁 开发目录: {dev_dir}")
    print(f"📋 Python路径已设置")

    return project_root, dev_dir


def test_imports():
    """
    测试所有模块导入
    """
    print("\n🧪 测试模块导入...")

    test_modules = [
        ("database.models", "PGMMain"),
        ("database.repositories", "PGMMainRepository"),
        ("utils.config_loader", "get_config"),
        ("utils.logger", "get_pgm_logger"),
        ("utils.db_connection", "get_db_pool"),
    ]

    all_success = True
    for module_name, attr_name in test_modules:
        try:
            exec(f"from {module_name} import {attr_name}")
            print(f"✅ {module_name}.{attr_name}")
        except ImportError as e:
            print(f"❌ {module_name}.{attr_name}: {e}")
            all_success = False

    return all_success


def main():
    """主函数"""
    print("=" * 60)
    print("MTE PGM Automation 环境设置")
    print("=" * 60)

    # 设置环境
    project_root, dev_dir = setup_environment()

    # 测试导入
    if test_imports():
        print("\n🎉 环境设置成功!")

        # 显示示例运行命令
        print("\n📝 运行示例程序:")
        print(f"  cd {dev_dir}")
        print(f"  python scripts/example_main.py")

        # 显示配置文件位置
        config_file = dev_dir / "config" / "config_template.json"
        if config_file.exists():
            print(f"\n📄 配置文件位置: {config_file}")
            print("⚠️  请确保已配置正确的数据库连接信息")
    else:
        print("\n❌ 环境设置失败，请检查目录结构")

        # 显示目录结构
        print("\n📁 预期目录结构:")
        print(f"  {project_root}/")
        print(f"  ├── dev/")
        print(f"  │   ├── __init__.py")
        print(f"  │   ├── database/")
        print(f"  │   │   ├── __init__.py")
        print(f"  │   │   ├── models.py")
        print(f"  │   │   └── repositories.py")
        print(f"  │   ├── utils/")
        print(f"  │   │   ├── __init__.py")
        print(f"  │   │   ├── config_loader.py")
        print(f"  │   │   ├── logger.py")
        print(f"  │   │   └── db_connection.py")
        print(f"  │   ├── scripts/")
        print(f"  │   │   └── example_main.py")
        print(f"  │   └── config/")
        print(f"  │       └── config_template.json")
        print(f"  └── prod/")


if __name__ == "__main__":
    main()