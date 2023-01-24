import os

from PyQt5.QtCore import QDate

# ui文件夹的路径
ui_dir_path = os.path.split(os.path.abspath(os.path.realpath(__file__)))[0] + "/.."


def str_amount(x):
    return '%.2f亿' % (float(x) / 100000000)


def str_value(x):
    return '%.2f万手' % (float(x) / 10000)


def str_change(x):
    change_str = '%.2f%%' % ((x['close'] - x['open']) / x['open'] * 100)
    return change_str


def trade_date(date, config, is_Forward=True):
    """
    返回一个合法的交易日期QDate
    :param is_Forward: 是否向前移动
    :param date:
    :return:
    """
    date_str = date.toString('yyyy-MM-dd')
    date_str = config.legal_trade_date(date_str, is_Forward)
    return QDate.fromString(date_str, 'yyyy-MM-dd')
