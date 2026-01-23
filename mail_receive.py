import win32com.client
import os


def read_pst_emails(pst_path):
    # 连接 Outlook 应用
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")

    # 添加 PST 文件到 Outlook 会话（临时挂载，不影响本地配置）
    pst = outlook.AddStore(pst_path)

    # 获取 PST 文件的根文件夹（默认是 "Outlook Data File"）
    root_folder = outlook.Folders.Item(pst.DisplayName)

    # 遍历根文件夹下的所有邮件（可根据需要指定子文件夹，如 "收件箱"）
    for folder in root_folder.Folders:
        if folder.Name == "收件箱":  # 只读取收件箱
            print(f"=== 收件箱邮件总数：{folder.Items.Count} ===")
            for item in folder.Items:
                if item.Class == 43:  # 43 表示邮件类型（避免读取日历/联系人等）
                    print(f"主题：{item.Subject}")
                    print(f"发件人：{item.SenderName} <{item.SenderEmailAddress}>")
                    print(f"接收时间：{item.ReceivedTime}")
                    print(f"正文：{item.Body[:100]}...")  # 截取前100字符
                    print("-" * 50)

    # 移除 PST 文件挂载（可选，避免残留）
    outlook.RemoveStore(pst)


# 调用函数（替换为你的 PST 文件路径）
if __name__ == "__main__":
    pst_file = r"D:\Sync\临时存放\个人文件夹20260122.pst"
    if os.path.exists(pst_file):
        read_pst_emails(pst_file)
    else:
        print("PST 文件不存在！")