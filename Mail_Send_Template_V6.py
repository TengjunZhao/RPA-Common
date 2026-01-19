# -*- coding : utf-8 -*-
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from typing import List
from email.utils import parseaddr, formataddr
from email.header import Header
import re
'''
1. Mail_Send_Template_V4 增加邮箱地址是否有效的验证： 无效地址，不放入收件人、抄送人地址中  2024-03
2. Mail_Send_Template_V4 检查附件是否存在: 不存在，不放入附件中   2024-03
3. Mail_Send_Template_V4 检查outlook是否启动： 如果未启动，自动启动   2024-05
4. Mail_Send_Template_V5 更换邮件发送方式， 不再依赖outlook启动，已知问题：收件人、抄送人 不能是群组   2024-08
5. Mail_Send_Template_V6 支持发送群组 2024-09
'''
#---------hitech mail config----------
smtp_addr = "mail.hitechsemi.com"
smtp_addr_port = 587

mail_from = "RpaADM@hitechsemi.com"
mail_id = "RpaADM"
mail_pwd = "Ht@123"

def smtp_connect():
    # 创建SMTP连接并登录
    server = smtplib.SMTP(smtp_addr, smtp_addr_port)
    # server.set_debuglevel(1)
    server.starttls()
    server.login(mail_id, mail_pwd)
    return server

def smtp_close(server):
    server.quit()

def is_valid_email(email)->bool:
    # 定义一个正则表达式来匹配电子邮件地址
    # 这个正则表达式可以捕获大多数常见的电子邮件地址格式
    email_regex = r"[^@]+@[^@]+\.[^@]+"
    # 使用正则表达式的 match 方法来检查电子邮件地址
    if re.match(email_regex, email):
        return True
    else:
        return False

def get_image_file(filedir: str)->list:
    arr_file = []
    # root是指当前目录路径(文件夹的绝对路径)
    # dirs是指路径下所有的子目录(文件夹里的文件夹)
    # files是指路径下所有的文件(文件夹里所有的文件)
    for root, dirs, files in os.walk(filedir):
        for file in files:
            if os.path.splitext(file)[1].upper() == '.PNG':
                arr_file.append(os.path.join(root, file))
    return arr_file

def _format_from_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))

def mail_send(mail_to: List[str], mail_subject: str, mail_body: str, str_image_dir: str, mail_cc: List[str], list_attach: List[str]) -> bool:
    server = smtp_connect()

    try:
        # 1.开启SMTP发信服务，并登录邮箱
        # 2. 获取图片列表
        list_images = []
        if len(str_image_dir) > 0:
            list_images = get_image_file(str_image_dir)


        # 3.检查 mail_to  mail_cc 是否有效， 无效过滤
        mail_cc = list(mail_cc)
        mail_cc.append("RpaADM@hitechsemi.com")
        valid_mail_to = [element for element in mail_to if is_valid_email(element)]
        valid_mail_cc = [element for element in mail_cc if is_valid_email(element)]
        mail_to = valid_mail_to
        mail_cc = valid_mail_cc

        # 4.构建邮件信息，采用related定义内嵌资源的邮件体
        msg = MIMEMultipart('related')
        # msg['From'] = mail_from
        msg['From'] = _format_from_addr('RpaADM <%s>' % mail_from)
        msg['To'] = ', '.join(mail_to)
        msg['Cc'] = ', '.join(mail_cc)  # To和Cc会在发送时进行拼接一起发送，这里的Cc仅仅是收见人看到的展示效果
        msg['Subject'] = mail_subject
        msg_content = MIMEMultipart('alternative')

        mail_body = mail_body.replace("\\r\\n", "<br>")

        # 如果正文中插入了图片
        # "这是正文<br>这是内容<br><img src='cid:1'><br><img src='cid:2'>",
        #  正文中如果有图片，需要在正文中插入位置， 在msg中插入图片
        if list_images:
            for index, image_file_path in enumerate(list_images):
                mail_body = mail_body + f"<br><img src='cid:{index+1}'><br>"

        # 邮箱正文内容，第一个参数为内容，第二个参数为格式(plain为纯文本；html为富文本)，第三个参数为编码
        msg_content.attach(MIMEText(mail_body, 'html', 'utf-8'))
        msg.attach(msg_content)

        # 如果正文中插入了图片
        #  正文中如果有图片，需要在正文中插入位置， 在msg中插入图片

        if list_images:
            for index, image_file_path in enumerate(list_images):
                image_file_name = os.path.basename(image_file_path)
                with open(image_file_path, 'rb') as f:
                    img = MIMEImage(f.read(), name=image_file_name)
                img.add_header('Content-ID', str(index+1))
                msg.attach(img)

        # 如果有附件，以二进制形式将文件的数据读出，再使用MIMEText进行规范化
        if list_attach:  # 如果附件列表有值
            for attach in list_attach:
                if os.path.exists(attach):
                   # 添加判断附件是否存在
                   attach_file_name = os.path.basename(attach)
                   attachment = MIMEText(open(attach, 'rb').read(), 'base64', 'utf-8')
                   # 告知浏览器或邮件服务器这是字节流，浏览器处理字节流的默认方式为下载
                   attachment['Content-Type'] = 'application/octet-stream'
                   # 此部分主要是告知浏览器或邮件服务器这是一个附件
                   attachment.add_header('Content-Disposition', 'attachment', filename=attach_file_name)
                   msg.attach(attachment)

        # 5.发送邮件，邮件发给：收件人+抄送人
        server.sendmail(mail_from, list(mail_to) + list(mail_cc), msg.as_string())
        return True   # 发送邮件成功
    except Exception as e_msg:
        print(e_msg)
        return False    # 发送邮件失败
    finally:
     # 6.关闭服务器
     smtp_close(server)

def main():
    # Hitech Mail Address
    hitech_member = ["tengjun.zhao","xin.liu","mei.zhang", "cheng2.chen", "yan.cai", "hongshan.tan", "dandan.hua",
                     "qisheng.tang", "zongming.tao"]
    sys_member = ["RpaADM"]
    sk_member = [""]
    # 为hiteh member添加邮件后缀“hitechsemi.com”
    hitech_member = [member + "@hitechsemi.com" for member in hitech_member]
    sys_member = [member + "@hitechsemi.com" for member in sys_member]
    # 合并邮件收件人
    mail_to = hitech_member + sys_member + sk_member

    # 如果在正文中插入图片，写法为<img src='cid:1'><img src='cid:2'><img src='cid:3'>......
    # cid后面的数字从1开始依次增加，并且与【正文插入的图片路径列表】参数的顺序和数量对得上,图片名称无所谓

    # result = mail_send(["RpaADM@hitechsemi.com", "RpaADM@hitechsemi.com"],
    #               "测试7",
    #               "这是正文<br><img src='cid:1'><br><img src='cid:2'><br>这是内容",
    #               r'\\172.27.7.188\Mod_TestP\08_QMV Daliy Report\data_mid',
    #               ["RpaADM@hitechsemi.com", "RpaADM@hitechsemi.com"],
    #               ["D:/test/test另存为.docx", "D:/test/boston.xlsx"])

    # image_dir = r'\\172.27.7.188\Mod_TestP\08_QMV Daliy Report\data_mid'
    image_dir = " "

    # mail_body1 = ('hello1' + "\\r\\n" + "\\r\\n" +
    #               r'<a href="\\172.27.7.188\fin\21发票校验\流程业务文件\单份投递\20240829\20240828163057\0.png">\\172.27.7.188\fin\21发票校验\流程业务文件\单份投递\20240829\20240828163057\0.png</a>'
    #               + "\\r\\n" + "hello3")
    task = 'PGM ALARM'
    mail_body1 = (f"各位好，我是MTE RPA机器人："+ "\\r\\n" + "\\r\\n" +f"  以下是{task}检查结果")

    result = mail_send(mail_to,
                       "RPA邮件群发测试",
                       mail_body1,
                       image_dir,
                       [],
                       [])

    print(result)


if __name__ == '__main__':
    main()
