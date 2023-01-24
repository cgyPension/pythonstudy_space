
import os
import time

import akshare as ak
import pandas as pd

from config import stockconstant as sc
from core.utils import timeutils
from database import stockdatabase as sdb


class StockConfig:
    time_format = '%Y-%m-%d'

    # 配置dataframe
    config_df = pd.DataFrame()
    trade_date_df = pd.DataFrame()
    trade_date_list = []
    # 股票池的一些配置
    pool_config_df = pd.DataFrame()
    all_board_df = pd.DataFrame()

    def __init__(self):
        self.read_config()
        self.__get_trade_date()

    def __create_config(self):
        """
        配置文件参数
        code：股票代码 ，name: 股票名称
        last_update_time: 最后一次更新的日期
        error_update_count：请求更新错误次数统计，定期清理多次更新错误的股票代码
        :return: DataFrame
        """
        columns = ['code', 'name', 'daily_update_time', 'error_daily_update_count', 'ind_update_time']
        self.config_df = pd.DataFrame(columns=columns)
        self.save_config()

    def __create_pool_config(self):
        """
        股票池的配置
        :return:
        """
        self.pool_config_df = pd.DataFrame({'key': ['rps_update_time', 'board_ind_update_time'],
                                            'update_time': [sc.start_date, sc.start_date]}, dtype=str)
        self.pool_config_df.set_index('key', inplace=True)
        self.save_pool_config()

    def read_config(self, is_new=False):
        """
        is_new if true del config else if part update
        如果是新则删除配置信息，重置，重新更新，否则增量更新
        读取配置信息，并对返回的dataFrame 数据类型进行预处理
        :return:
        """
        if is_new:
            os.remove(sc.config_path)
            os.remove(sc.pool_config_path)
            print('删除配置成功，重置配置...')
            sdb.drop_all_table()
            print('删除数据表...')

        # 创建所有的表
        sdb.create_all_table()

        try:
            self.config_df = pd.read_csv(sc.config_path, dtype={
                'code': str, 'name': str, 'daily_update_time': str,
                'error_daily_update_count': int, 'ind_update_time': str,
            })
        except Exception as error:
            print("创建配置信息：", error)
            self.__create_config()
        # code 强制转str类型，并补全股票代码为6位
        # self.config_df['code'] = self.config_df['code'].astype(str)
        self.config_df['code'] = self.config_df['code'].str.zfill(6)

        # 股票池的配置
        try:
            self.pool_config_df = pd.read_csv(sc.pool_config_path, index_col=['key'])
        except:
            self.__create_pool_config()

        # 板块配置
        self.__get_industry_board()

    def save_config(self):
        """
        保存配置信息，忽略列
        :return:
        """
        self.config_df.to_csv(sc.config_path, encoding='utf-8', index=False)

    def save_pool_config(self, key=None, end_time=None):
        """
        保存股票池相关配置信息
        :return:
        """
        if not end_time is None:
            self.pool_config_df.loc[key, 'update_time'] = end_time

        self.pool_config_df.to_csv(sc.pool_config_path, encoding='utf-8')

    def save_industry_board_config(self):
        self.all_board_df.to_csv(sc.industry_board_path, index=False)

    def __get_industry_board(self, is_update=False):
        """
        获取板块代码并保存到 csv 中
        :return:
        """
        try:
            self.all_board_df = pd.read_csv(sc.industry_board_path)
        except Exception:
            self.all_board_df = pd.DataFrame(
                columns=['name', 'code', 'amount', 'number',
                         'daily_update_time', 'is_pull_constituent', 'is_industry'])
            # 如果本地没有或者强制更新，网络拉取
            if self.all_board_df.empty or is_update:
                board_df = ak.stock_board_industry_name_em()
                board_df['is_industry'] = True
                board_concept_df = ak.stock_board_concept_name_em()
                board_concept_df['is_industry'] = False
                tmp_board_df = pd.concat((board_df, board_concept_df), ignore_index=True)
                self.all_board_df[['name', 'code', 'amount', 'is_industry']] = tmp_board_df[
                    ['板块名称', '板块代码', '总市值', 'is_industry']]
                self.all_board_df['number'] = tmp_board_df['上涨家数'] + tmp_board_df['下跌家数']
                df = self.all_board_df
                # 过滤不需要的板块
                df.drop(df[df['number'] < 10].index, inplace=True)
                df.drop(df[(df['name'].isin(self.__exclude_board()))].index, inplace=True)
                df['daily_update_time'] = sc.start_date
                df['is_pull_constituent'] = False
                self.all_board_df = df
                self.save_industry_board_config()
                print('更新板块完毕...')

    def __get_trade_date(self):
        try:
            tmp_df = ak.tool_trade_date_hist_sina()
            if not tmp_df.empty:
                tmp_df.set_index(['trade_date'], inplace=True)
                self.trade_date_df = tmp_df
                self.trade_date_df.to_csv(sc.trade_date_path)
            else:
                self.trade_date_df = pd.read_csv(sc.trade_date_path)
                self.trade_date_df.set_index(['trade_date'], inplace=True)
        except:
            print('交易日拉取新数据失败')
            self.trade_date_df = pd.read_csv(sc.trade_date_path)
            self.trade_date_df.set_index(['trade_date'], inplace=True)
        self._trade_date_list()

        print('交易日期更新完成')

    def _trade_date_list(self):
        tmp_list = self.trade_date_df.index.tolist()
        self.trade_date_list = [str(x) for x in tmp_list]

    def daily_start_time(self, code):
        """
        日行情更新的开始时间
        :param code:  股票代码
        :return:
        """
        df = self.config_df
        if 'daily_update_time' not in df.columns:
            df['daily_update_time'] = sc.start_date
        s = df.loc[df['code'] == code, 'daily_update_time']
        if s.empty:
            return self.legal_trade_date(sc.start_date)
        else:
            return self.legal_trade_date(str(s.iloc[0]))

    def daily_board_start_time(self, code):
        """
        日行情更新的开始时间
        :param code:  股票代码
        :return:
        """
        df = self.all_board_df
        s = df.loc[df['code'] == code, 'daily_update_time']
        if s.empty:
            return self.legal_trade_date(sc.start_date)
        else:
            return self.legal_trade_date(str(s.iloc[0]))

    def ind_start_time(self, code):
        """
        指标的开始时间
        :param code: 股票代码
        :return:
        """
        df = self.config_df
        if 'ind_update_time' not in df.columns:
            df['ind_update_time'] = sc.start_date
        s = df.loc[df['code'] == code, 'ind_update_time']
        if s.empty:
            return self.legal_trade_date(sc.start_date)
        else:
            return self.legal_trade_date(str(s.iloc[0]))

    def rps_stock_start_time(self):
        """
        计算个股rps的开始时间
        :return:
        """
        start_time = self.pool_config_df.loc['rps_update_time', 'update_time']

        return self.legal_trade_date(start_time)

    def block_ind_start_time(self):
        """
        计算个股rps的开始时间
        :return:
        """
        start_time = self.pool_config_df.loc['board_ind_update_time', 'update_time']

        return self.legal_trade_date(start_time)

    def update_end_time(self):
        """
        更新的结束时间
        取交易日期 df 中距离今天最近的日期
        如果当前运行的时间不是当天收盘后的时间，更新时间往前推一天
        :return:
        """
        localtime = time.localtime(time.time())
        end_time_str = time.strftime(self.time_format, localtime)
        # 时间要在下午3点之后，否则向前移动一天
        if localtime.tm_hour < 15:
            end_time_str = timeutils.time_str_delta(end_time_str, self.time_format, days=-1)
        # 寻找离现在时间最近的交易日期
        while True:
            if end_time_str in self.trade_date_list:
                return end_time_str
            else:
                end_time_str = timeutils.time_str_delta(end_time_str, self.time_format, days=-1)

    def legal_trade_date(self, trade_date, is_Forward=True):
        """
        format : %Y-%m-%d
        合法化为交易日期，如果不是则往前移动，直到找到为止
        @:param is_Forward: 是否是往前移动
        :return:
        """
        localtime = time.localtime(time.time())
        localtime_str = time.strftime(self.time_format, localtime)
        localtime_stamp = timeutils.str_to_stamp(localtime_str, self.time_format)

        while True:
            if trade_date in self.trade_date_list:
                trade_stamp = timeutils.str_to_stamp(trade_date, self.time_format)
                # 如果超过今天，取最新更新日期
                if trade_stamp > localtime_stamp:
                    return self.update_end_time()
                else:
                    return trade_date
            else:
                date = -1 if is_Forward else 1
                trade_date = timeutils.time_str_delta(trade_date, self.time_format, days=date)

    def legal_trade_date_delta(self, trade_date, offset):
        """
        合法的交易日期偏移
        :param trade_date: 交易日期
        :param offset: 交易日期列表的偏移
        :return:
        """
        trade_date = self.legal_trade_date(trade_date)
        delta_index = self.trade_date_list.index(trade_date) + offset
        if delta_index < 0:
            delta_index = 0
        if delta_index >= len(self.trade_date_list):
            delta_index = len(self.trade_date_list) - 1
        return self.trade_date_list[delta_index]

    def __exclude_board(self):
        list = ['昨日连板', '昨日连板_含一字', '昨日涨停', '昨日涨停_含一字', '专业服务', '工程咨询服务', '包装材料', '医药商业', '能源金属', '房地产服务', '公用事业',
                '珠宝首饰', '采掘行业', '化肥行业', '非金属材料', '贸易行业', '贵金属', '数据确权', '职业教育', '快手概念', '知识产权', '虚拟数字人', '职业教育',
                'NFT概念', 'DRG/DIP', '抗原检测', '数字哨兵', '电子身份证', 'VPN', 'AIGC概念', '新冠检测', '广电', '商汤概念', '字节概念', '手游概念',
                '数字阅读', '痘病毒防治', 'IPv6',
                '网络游戏', '人脑工程', '电子竞技', '数据中心', '华为昇腾', '蚂蚁概念', '数字孪生', '鸿蒙概念', 'RCS概念', '在线旅游', 'UWB概念', '基因测序',
                'UWB概念', 'eSIM', '智慧城市',
                '注射器概念', '华为欧拉', '重组蛋白', '边缘计算', '跨境支付', '智慧灯杆', '免疫治疗', 'CAR-T细胞疗法', '百度概念', '核污染防治', '盲盒经济', '彩票概念',
                '养老概念', '核酸采样亭', '单抗概念',
                '幽门螺杆菌概念', '万达概念', '京东金融', '阿里概念', '租售同权', '粤港自贸', '肝炎概念', '生物疫苗', '健康中国', '杭州亚运会', '新型城镇化', '医废处理',
                '超清视频', '股权转让', '病毒防治', '抖音小店',
                '超超临界发电', '肝素概念', '网红直播', '托育服务', '京津冀', '超级品牌', '流感', '青蒿素', '独角兽', '华为概念', '雄安新区', '创投', '参股保险',
                '辅助生殖', '分拆预期', 'PPP模式', 'GDR', '体育产业', '地热能',
                '土壤修复', 'ETC', '拼多多概念', '电子纸概念', '世界杯', '6G概念', '科创板做市商', '低价股', '创业板综', '央视50_', '参股期货', 'MicroLED',
                '智能电视', '参股券商', '生物质能发电', '北京冬奥', '参股新三板', '草甘膦',
                '滨海新区', '2025规划', '地下管网', '熊去氧胆酸', '汽车拆解', '破净股', 'REITs概念', '茅指数', '纾困概念', '深圳特区', '沪企改革', '富时罗素',
                '债转股', '成渝特区', 'AH股', '国企改革', '证金持股', '中证500', '上证50_',
                '超级真菌', '超级真菌', '深股通', '全息技术', '海洋经济', '人造太阳', '深成500', '预亏预减', '标准普尔', 'HS300_', '净水概念', 'EDR概念',
                '婴童概念', '股权激励', '上证180_', '深证100R', 'AB股', '创业成份', '海绵城市', '转债标的',
                '建筑节能', '融资融券', 'PVDF概念', '宁组合', '节能环保', '独家药品', '纳米银', 'RCEP概念', '维生素', '机构重仓', '核能核电', '北交所概念',
                '参股银行', '垃圾分类', '注册制次新股', 'IPO受益', '长江三角', '气溶胶检测', 'MSCI中国', '智能机器',
                '沪股通', '阿兹海默', '口罩', '东北振兴', '天基互联', '进口博览', '一带一路', '代糖概念', '社区团购', '上证380', '新冠药物', '预盈预增', 'QFII重仓',
                '基本金属', '湖北自贸', '养老金', '尾气治理', '乡村振兴', '小米概念', '苹果概念', '基金重仓',
                '生态农业', '上海自贸', '社保重仓', '航母概念', '油价相关', 'WiFi', '虚拟电厂', 'C2M概念', '黄金概念', '消毒剂', '可燃冰', '高送转', '3D玻璃',
                '无线充电', '壳资源', '发电机概念', 'OLED', '内贸流通', '科创板做市股', '贬值受益', '举牌', '共享经济',
                '毛发医疗', '富士康', '户外露营', '特斯拉', '跨境电商', '蝗虫防治', '百元股', '复合集流体', '昨日触板', '送转预期', 'MLCC', '氦气概念', '快递概念',
                '氦气概念', '环氧丙烷', '转基因', '胎压监测', '中俄贸易概念', '刀片电池', '蓝宝石', '长寿药', '华为汽车',
                '退税商店', '超导概念', 'B股', '统一大市场', '民爆概念', '汽车一体化压铸', '抗菌面料', '蒙脱石散', '地塞米松']
        return list


if __name__ == '__main__':
    config = StockConfig()
    time_str = config.update_end_time()
    print(time_str)
    index = config.trade_date_list.index(time_str)
    print(config.trade_date_list[index - 1])
