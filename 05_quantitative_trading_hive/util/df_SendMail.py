import os
import smtplib
import sys
import time
from datetime import date
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr
import akshare as ak
import pandas as pd

# 在linux会识别不了包 所以要加临时搜索目录
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

my_sender = '2855227591@qq.com'    # 发件人邮箱账号
my_pass = 'sogvwwkvkmkqdghi'   # 发件人邮箱密码
Subject = '量化投资操作'
sender_show = '蔡广源'
# recipient_shows = ['蔡广源','秦晋隆']
# to_addrs = ['2855227591@qq.com','1620157829@qq.com'] # 收件人邮箱账号(这里发自己) list 可放批量
recipient_shows = ['蔡广源','xx']
to_addrs = ['2855227591@qq.com','2855227591@qq.com'] # 收件人邮箱账号(这里发自己) list 可放批量

data_dic = {'机型': ['小米12', '华为P40', 'IQOO8', 'iphone13'], '价格': [3999, 5000, 3899, 5999],
            '颜色': ['白色', '紫色', '金色', '白色']}
df = pd.DataFrame(data_dic)

def mail(message,Subject,sender_show,recipient_shows,to_addrs,cc_show=''):
    '''
    :param message: str 邮件内容
    :param Subject: str 邮件主题描述
    :param sender_show: str 发件人显示，不起实际作用如："xxx"
    :param recipient_shows: str 收件人显示，不起实际作用 多个收件人用数组
    :param to_addrs: str 实际收件人
    :param cc_show: str 抄送人显示，不起实际作用，多个抄送人用','隔开如："xxx,xxxx"
    '''
    ret = True
    try:
        current_dt = date.today()
        # 新浪财经的股票交易日历数据
        df = ak.tool_trade_date_hist_sina()
        df = df[df['trade_date'] > current_dt].reset_index(drop=True)
        current_dt = df.iat[0,0] # 下一个交易日
        title = str(current_dt) + Subject

        server = smtplib.SMTP_SSL("smtp.qq.com", 465)       # 发件人邮箱中的SMTP服务器，端口是465
        server.login(my_sender, my_pass)  # 发件人邮箱账号、邮箱密码

        for to_addr,recipient_show in zip(to_addrs,recipient_shows):
            msg = MIMEText(message, 'html', 'utf-8')
            msg['From'] = formataddr([sender_show, my_sender])  # 发件人昵称
            msg['Subject'] = title  # 邮件的主题
            # 抄送人显示，不起实际作用
            msg["Cc"] = cc_show
            msg['To'] = formataddr([recipient_show, to_addr])     # 接收人昵称

            server.sendmail(my_sender, to_addr, msg.as_string())  # 发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
    except Exception as e:  # 如果 try 中的语句没有执行，则会执行下面的 ret = False
        ret = False
        print(e)
    return ret



# 后期把邮件网页改成bigquant那个看一下F12
def send_mail(df):
    # 转化成 HTML 格式
    df_html = df.to_html(index=False)
    mail_html =  '''
        <!DOCTYPE html>
        <html lang="en">
            <head>
            <meta charset="utf-8">
            <STYLE TYPE="text/css" MEDIA=screen>

                table.dataframe {
                    border-collapse: collapse;
                    border: 2px solid #a19da2;
                }

                table.dataframe thead {
                    border: 2px solid #91c6e1;
                    background: #5B9BD5;
                    padding: 10px 10px 10px 10px;
                    color: #333333;
                }

                table.dataframe tbody {
                    border: 2px solid #91c6e1;
                    padding: 10px 10px 10px 10px;
                }

                table.dataframe tr {

                }

                table.dataframe th {
                    vertical-align: top;
                    font-size: 14px;
                    padding: 10px 10px 10px 10px;
                    color: #FFFFFF;
                    font-family: arial;
                    text-align: center;
                }

                table.dataframe td {
                    text-align: center;
                    padding: 10px 10px 10px 10px;
                }

                body {
                    font-family: 宋体;
                }

                h1 {
                    color: #5db446
                }

                div.header h2 {
                    color: #FF3366;
                    font-family: 黑体;
                }

                div.content h2 {
                    text-align: center;
                    font-size: 28px;
                    text-shadow: 2px 2px 1px #de4040;
                    color: #fff;
                    font-weight: bold;
                    background-color: #008eb7;
                    line-height: 1.5;
                    margin: 20px 0;
                    box-shadow: 10px 10px 5px #888888;
                    border-radius: 5px;
                }

                h3 {
                    font-size: 22px;
                    background-color: rgba(61, 160, 20, 0.71);
                    text-shadow: 2px 2px 1px #de4040;
                    color: rgba(239, 241, 234, 0.99);
                    line-height: 1.5;
                }

                h4 {
                    color: #e10092;
                    font-family: 楷体;
                    font-size: 20px;
                    text-align: center;
                }

                td img {
                    max-width: 300px;
                    max-height: 300px;
                }

            </STYLE>
        </head>
        <body>

        <h2><b>股市有风险，投资需谨慎！！！</b></h2>
        <hr size="1px" noshade=true />
        <p><font size="3" color="#525252"><b>股票推荐短线策略：大部分是持仓两天，合理设置权重、轮动。在9点15到9点25以涨停价挂单，这样能以开盘价成交；2点57挂跌停价，这样能以收盘价成交。方便事后统计。
        </b></font></p>
        '''+ df_html +'''
        </body>
        </html>'''

    ret = mail(mail_html,Subject,sender_show,recipient_shows,to_addrs)
    if ret:
        print("邮件发送成功！！！")
    else:
        print("邮件发送失败！！！")

if __name__ =='__main__':
    send_mail(df)