import os
import sys
import win32com.client as win32
import pythoncom


def get_image_file(fileDir: str):
    # 获取目录下所有xls文件
    arr_file = []
    # root是指当前目录路径(文件夹的绝对路径)
    # dirs是指路径下所有的子目录(文件夹里的文件夹)
    # files是指路径下所有的文件(文件夹里所有的文件)
    for root, dirs, files in os.walk(fileDir):
        for file in files:
            if os.path.splitext(file)[1] == '.png' or os.path.splitext(file)[1] == '.PNG':
                arr_file.append(os.path.join(root, file))
    return arr_file

def mail_send(recipients: list, mail_subject: str, mail_body: str, str_image_dir: str, copies: list, list_app_path: list)-> bool:
    arr_image=[]
    str_content = ''

    if len(str_image_dir)>0:
       arr_image = get_image_file(str_image_dir)

    if len(recipients) > 0:
        # 调用Outlook application
        try:
            outlook_app = win32.Dispatch('Outlook.Application')
            mail_item = outlook_app.CreateItem(0)  # 0: olMailItem
        except pythoncom.com_error as com_error:
            print(com_error)
            print(vars(com_error))
            print(com_error.args)
        # 收件人
        str_to = ""
        for recipient in recipients:
            str_to += recipient + ";"
        mail_item.To = str_to

        # 抄送人
        if copies is not None:
            str_cc = ""
            for cc in copies:
                str_cc += cc + ";"
            mail_item.CC = str_cc
        # 收件主题
        mail_item.Subject = mail_subject

        # 收件内容
        mail_item.BodyFormat = 2  # 2: Html format

        # 邮件文字
        str_content = "<div>{text}</div>".format(text=mail_body)

        # print(arr_image)
        # 邮件附件
        for appendix in list_app_path:
            mail_item.Attachments.Add(appendix)

        # 邮件图片
        for i in range(len(arr_image)):
            mail_item.Attachments.Add(arr_image[i])
            str_image = os.path.basename(arr_image[i])
            str_content += "<div><img src='{name}' /></div>".format(name=str_image)

        # 邮件body
        str_content = str_content.replace("\\r\\n", "<br>")
        mail_item.HTMLBody = str_content

        # mail_item.Display()   # 图片如果不显示，需求调用display
        mail_item.Send()
        return True

    else:
        return False
        print('Recipient email address - NOT FOUND')

if __name__ == "__main__":
    recipients = ['172549118@qq.com',]
    list_app_path = []


    copies = []

    str_image_dir = r'D:\Python\RPA Common'

    mail_body='大家好， 我是'


    mail_subject = 'test'

    mail_send(recipients, mail_subject,  mail_body,
              copies, str_image_dir, list_app_path)

    sys.exit()
