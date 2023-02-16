create database stock;
use stock;

-- TODO =========================================================  xx  =====================================================================
-- 字段不能为空 在表设置默认值好像有其他限制 可以在插入时候设置nvl
drop table if exists stock.ods_trade_date_hist_sina_df;
create table if not exists stock.ods_trade_date_hist_sina_df
(
    date_id     Int32 comment '日期id',
    trade_date  Date comment '交易日期',
    update_time DateTime64(3, 'Asia/Shanghai') comment '更新时间'
)
    engine = MergeTree ORDER BY date_id
        comment '新浪财经的股票交易日历数据';

drop table if exists stock.dim_plate_df;
create table if not exists stock.dim_plate_df
(
    trade_date      Date comment '交易日期',
    plate_code      String comment '板块代码',
    plate_name      String comment '板块名称',
    open_price      Decimal(20, 4) comment '开盘价',
    close_price     Decimal(20, 4) comment '收盘价',
    high_price      Decimal(20, 4) comment '最高价',
    low_price       Decimal(20, 4) comment '最低价',
    change_percent  Decimal(20, 4) comment '涨跌幅',
    change_amount   Decimal(20, 4) comment '涨跌额',
    volume          Int64 comment '成交量',
    turnover        Decimal(20, 4) comment '成交额',
    amplitude       Decimal(20, 4) comment '振幅',
    turnover_rate   Decimal(20, 4) comment '换手率',
    rps_5d          Decimal(20, 15) comment '欧奈尔rps_5d',
    rps_10d         Decimal(20, 15) comment '欧奈尔rps_10d',
    rps_15d         Decimal(20, 15) comment '欧奈尔rps_15d',
    rps_20d         Decimal(20, 15) comment '欧奈尔rps_20d',
    rps_50d         Decimal(20, 15) comment '欧奈尔rps_50d',
    is_rps_red      Int32 comment '20日内rps首次三线翻红',
    ma_5d           Decimal(20, 4) comment '5日均线',
    ma_10d          Decimal(20, 4) comment '10日均线',
    ma_20d          Decimal(20, 4) comment '20日均线',
    ma_50d          Decimal(20, 4) comment '50日均线',
    ma_120d         Decimal(20, 4) comment '120日均线',
    ma_150d         Decimal(20, 4) comment '150日均线',
    ma_200d         Decimal(20, 4) comment '200日均线',
    ma_250d         Decimal(20, 4) comment '250日均线',
    high_price_250d Decimal(20, 4) comment '250日最高价',
    low_price_250d  Decimal(20, 4) comment '250日最低价',
    update_time     DateTime64(3, 'Asia/Shanghai') comment '更新时间'
) ENGINE = MergeTree()
    partition by (trade_date)
    order by (plate_code)
    comment '东方财富-沪深板块-行业概念板块汇总';

drop table if exists stock.dwd_stock_quotes_stand_di;
create table if not exists stock.dwd_stock_quotes_stand_di
(
    trade_date                Date comment '交易日期',
    stock_code                String comment '股票代码',
    stock_name                String comment '股票名称',
    open_price                Decimal(20, 4) comment '开盘价',
    close_price               Decimal(20, 4) comment '收盘价',
    high_price                Decimal(20, 4) comment '最高价',
    low_price                 Decimal(20, 4) comment '最低价',
    volume                    Decimal(20, 4) comment '成交量',
    volume_ratio_1d           Decimal(20, 4) comment '量比_1d 与昨日对比',
    volume_ratio_5d           Decimal(20, 4) comment '量比：过去5个交易日',
    turnover                  Decimal(20, 4) comment '成交额',
    amplitude                 Decimal(20, 4) comment '振幅',
    change_percent            Decimal(20, 4) comment '涨跌幅',
    change_amount             Decimal(20, 4) comment '涨跌额',
    turnover_rate             Decimal(20, 4) comment '换手率',
    turnover_rate_5d          Decimal(20, 4) comment '5日平均换手率',
    turnover_rate_10d         Decimal(20, 4) comment '10日平均换手率',
    total_market_value        Decimal(20, 4) comment '总市值',
    industry_plate            String comment '行业板块',
    concept_plates            String comment '概念板块 ,拼接',
    rps_5d                    Decimal(20, 15) comment '欧奈尔rps_5d',
    rps_10d                   Decimal(20, 15) comment '欧奈尔rps_10d',
    rps_20d                   Decimal(20, 15) comment '欧奈尔rps_20d',
    rps_50d                   Decimal(20, 15) comment '欧奈尔rps_50d',
    rps_120d                  Decimal(20, 15) comment '欧奈尔rps_120d',
    rps_250d                  Decimal(20, 15) comment '欧奈尔rps_250d',
    rs                        Decimal(20, 4) comment '欧奈尔RS',
    rsi_6d                    Decimal(20, 6) comment 'rsi_6d',
    rsi_12d                   Decimal(20, 6) comment 'rsi_12d',
    ma_5d                     Decimal(20, 4) comment '5日均线',
    ma_10d                    Decimal(20, 4) comment '10日均线',
    ma_20d                    Decimal(20, 4) comment '20日均线',
    ma_50d                    Decimal(20, 4) comment '50日均线',
    ma_120d                   Decimal(20, 4) comment '120日均线',
    ma_150d                   Decimal(20, 4) comment '150日均线',
    ma_200d                   Decimal(20, 4) comment '200日均线',
    ma_250d                   Decimal(20, 4) comment '250日均线',
    high_price_250d           Decimal(20, 4) comment '250日最高价',
    low_price_250d            Decimal(20, 4) comment '250日最低价',
    stock_label_names         String comment '股票标签名称 负面标签后缀用- ,拼接',
    stock_label_num           Int32 comment '股票标签数量',
    sub_factor_names          String comment '主观因子标签名称 负面因子后缀用- ,拼接',
    sub_factor_score          Int32 comment '主观因子分数 负面因子后缀用-',
    holding_yield_2d          Decimal(20, 4) comment '持股2日后收益率(%)',
    holding_yield_5d          Decimal(20, 4) comment '持股5日后收益率(%)',
    hot_rank                  Int32 comment '个股热度排名',
    interprets                String comment '解读 ;拼接',
    reason_for_lhbs           String comment '上榜原因 ;拼接',
    lhb_num_5d                Int32 comment '最近5天_龙虎榜上榜次数',
    lhb_num_10d               Int32 comment '最近10天_龙虎榜上榜次数',
    lhb_num_30d               Int32 comment '最近30天_龙虎榜上榜次数',
    lhb_num_60d               Int32 comment '最近60天_龙虎榜上榜次数',
    pe                        Decimal(20, 4) comment '市盈率',
    pe_ttm                    Decimal(20, 4) comment '市盈率TTM',
    pb                        Decimal(20, 4) comment '市净率',
    ps                        Decimal(20, 4) comment '市销率',
    ps_ttm                    Decimal(20, 4) comment '市销率TTM',
    dv_ratio                  Decimal(20, 4) comment '股息率',
    dv_ttm                    Decimal(20, 4) comment '股息率TTM',
    net_profit                Decimal(20, 4) comment '净利润',
    net_profit_yr             Decimal(20, 4) comment '净利润同比',
    total_business_income     Decimal(20, 4) comment '营业总收入',
    total_business_income_yr  Decimal(20, 4) comment '营业总收入同比',
    business_fee              Decimal(20, 4) comment '营业总支出-营业支出',
    sales_fee                 Decimal(20, 4) comment '营业总支出-销售费用',
    management_fee            Decimal(20, 4) comment '营业总支出-管理费用',
    finance_fee               Decimal(20, 4) comment '营业总支出-财务费用',
    total_business_fee        Decimal(20, 4) comment '营业总支出-营业总支出',
    business_profit           Decimal(20, 4) comment '营业利润',
    total_profit              Decimal(20, 4) comment '利润总额',
    ps_business_cash_flow     Decimal(20, 4) comment '每股经营性现金流(元)',
    return_on_equity          Decimal(20, 4) comment '净资产收益率(%)',
    npadnrgal                 Decimal(20, 4) comment '扣除非经常性损益后的净利润(元)',
    net_profit_growth_rate    Decimal(20, 4) comment '净利润增长率(%)',
    suspension_time           String comment '停牌时间',
    suspension_deadline       String comment '停牌截止时间',
    suspension_period         String comment '停牌期限',
    suspension_reason         String comment '停牌原因',
    belongs_market            String comment '所属市场',
    estimated_resumption_time String comment '预计复牌时间',
    f_volume                  Decimal(20, 10) comment '极值标准中性化_成交量',
    f_volume_ratio_1d         Decimal(20, 10) comment '极值标准中性化_量比_1d 与昨日对比',
    f_volume_ratio_5d         Decimal(20, 10) comment '极值标准中性化_量比：过去5个交易日',
    f_turnover                Decimal(20, 10) comment '极值标准中性化_成交额',
    f_turnover_rate           Decimal(20, 10) comment '极值标准中性化_换手率',
    f_turnover_rate_5d        Decimal(20, 10) comment '极值标准中性化_5日平均换手率',
    f_turnover_rate_10d       Decimal(20, 10) comment '极值标准中性化_10日平均换手率',
    f_total_market_value      Decimal(20, 10) comment '极值标准中性化_总市值',
    f_pe                      Decimal(20, 10) comment '极值标准中性化_市盈率',
    f_pe_ttm                  Decimal(20, 10) comment '极值标准中性化_市盈率TTM',
    update_time               DateTime64(3, 'Asia/Shanghai') comment '更新时间'
) ENGINE = MergeTree()
    partition by (trade_date)
    order by (stock_code)
    comment '沪深A股行情表-部分字段去极值标准化行业市值中性化';

