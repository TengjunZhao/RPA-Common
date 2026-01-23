import os
import sys

# -------------------------- 配置项 --------------------------
# 你的PST文件路径（用原始字符串避免转义）
PST_FILE_PATH = r"D:\sync\临时存放\个人文件夹20260122.pst"
# 控制正文预览长度（避免打印过长内容）
BODY_PREVIEW_LENGTH = 200

def read_pst_alternative():
    """使用替代方法读取PST文件"""
    print("🔄 尝试使用替代方法读取PST文件...")
    print("💡 提示：当前环境中缺少Outlook，正在寻找其他解决方案...")

    # 检查是否安装了pypff
    try:
        import pypff
        print("✅ 检测到pypff库，尝试使用它来读取PST文件...")

        # 打开PST文件
        pst_file = pypff.file()
        pst_file.open(PST_FILE_PATH, 'r')

        # 获取根文件夹
        root = pst_file.get_root_folder()
        print(f"📁 PST根文件夹名称: {root.name}")

        # 递归遍历文件夹和邮件
        def process_folder(folder, level=0):
            indent = "  " * level
            print(f"\n{indent}📂 处理文件夹: {folder.name}")

            # 获取子文件夹
            for i in range(folder.number_of_sub_folders):
                sub_folder = folder.get_sub_folder(i)
                process_folder(sub_folder, level + 1)

            # 获取邮件
            for i in range(folder.number_of_sub_messages):
                try:
                    message = folder.get_sub_message(i)
                    subject = message.subject or '(无主题)'
                    # 确保主题是字符串
                    if isinstance(subject, bytes):
                        subject = subject.decode('utf-8', errors='ignore')

                    sender = message.sender_name or message.sender_email_address or '(未知)'
                    # 确保发件人是字符串
                    if isinstance(sender, bytes):
                        sender = sender.decode('utf-8', errors='ignore')

                    print(f"{indent}├─ 📧 邮件: {subject}")
                    print(f"{indent}   发件人: {sender}")

                    # 处理时间
                    delivery_time = str(message.delivery_time) if hasattr(message, 'delivery_time') else '(未知时间)'
                    if isinstance(delivery_time, bytes):
                        delivery_time = delivery_time.decode('utf-8', errors='ignore')
                    print(f"{indent}   时间: {delivery_time}")

                    # 显示邮件正文预览
                    body = None
                    if hasattr(message, 'plain_text_body') and message.plain_text_body:
                        body = message.plain_text_body
                    elif hasattr(message, 'html_body') and message.html_body:
                        body = message.html_body

                    if body:
                        # 确保正文是字符串
                        if isinstance(body, bytes):
                            body = body.decode('utf-8', errors='ignore')

                        preview = body[:BODY_PREVIEW_LENGTH]
                        if len(body) > BODY_PREVIEW_LENGTH:
                            preview += "..."
                        print(f"{indent}   正文预览: {preview}")

                    print(f"{indent}└──────────────────────────────────")
                except UnicodeDecodeError as e:
                    print(f"{indent}❌ 读取邮件时编码错误: {e}")
                except Exception as e:
                    print(f"{indent}❌ 读取邮件失败: {e}")

        process_folder(root)
        pst_file.close()
        print("\n🎉 PST文件读取完成！")

    except ImportError:
        print("❌ 无法使用替代方法读取PST文件")
        print("💡 安装方法：")
        print("   1. 安装Visual Studio C++ Build Tools")
        print("   2. 运行: pip install libpff-python 或 pypff")
        print("   3. 或者安装Microsoft Outlook")
        print("\n💡 或者您可以使用以下工具:")
        print("   - SysTools PST Viewer (免费)")
        print("   - Bitrecover PST Viewer (免费)")
        print("   - 在Windows上安装完整版Microsoft Outlook")
    except Exception as e:
        print(f"❌ 使用替代方法读取PST失败: {e}")
        import traceback
        traceback.print_exc()

# -------------------------- 主函数 --------------------------
if __name__ == "__main__":
    read_pst_alternative()