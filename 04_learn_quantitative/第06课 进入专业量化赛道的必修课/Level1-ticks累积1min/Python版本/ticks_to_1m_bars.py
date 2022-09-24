#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
普量学院量化投资课程系列案例源码包
普量学院版权所有
仅用于教学目的，严禁转发和用于盈利目的，违者必究
（C) Plouto-Quants All Rights Reserved

普量学院助教微信：niuxiaomi3
"""

""" Ticks到1min线的转换，该脚本每天运行一次 """

# 所有累积完成的1min线放到一个dict中，以股票代码作为key
bars_by_codes = dict()

def ticks_to_1m_bars(one_tick):
	code = one_tick["code"]        # 该tick所属的股票代码
	ptr = bars_by_codes.get(code)  # 该代码对应的1min线计算用的数据结构
	if ptr is None:
		ptr = bars_by_codes[code] = \
			{
				"bars_queue":     list(),  # 已完成累积的1min线的队列
				"last_bar_time":  None,    # 上次tick对应的K线时间戳
				"day_high":       None,    # 截至当前时刻，当日的最高价
				"day_low":        None,    # 截至当前时刻，当日的最低价
				"current_bar":    dict()   # 当前累积中的1min线数据结构
			}

	# 临时取出当前工作的数据结构，便于后面的读写
	bars_queue = ptr["bars_queue"]
	last_bar_time = ptr["last_bar_time"]
	day_high = ptr["day_high"]
	day_low = ptr["day_low"]
	current_bar = ptr["current_bar"]

	# 接收到的每个tick中的时间戳格式举例："2018-03-12 09:41:27"
	current_bar_time = one_tick["time"][:-3]

	if current_bar_time != last_bar_time:  # 进入下一分钟的第一个tick
		if last_bar_time is not None:  # 不是当天的第一个tick
			# 把前面刚累计好的1min线输出
			current_bar["time"] += ":00"  # 把时间戳中的“秒”字段补成零
			bars_queue.append(current_bar)  # 添加到1min线队列的末尾

		# 把新的一根1min线的工作数据结构初始化
		current_bar["time"] = current_bar_time
		if last_bar_time is None:  # 当日第一个tick（也是第一个1min线）
			current_bar["open"] = one_tick["open"]  # 当日开盘价就是第一根1min线的开盘价
			day_high = one_tick["high"]  # 以截至到第一个tick时的当日最高价初始化
			day_low = one_tick["low"]    # 以截至到第一个tick时的当日最低价初始化
		else:  # 非当日第一个tick
			current_bar["open"] = one_tick["latest"]  # 以tick最新价初始化该1min线的开盘价
			if one_tick["high"] > day_high:  # 本分钟内见到了当日的新的最高价
				current_bar["high"] = one_tick["high"]  # 更新本根1min线的最高价
				day_high = one_tick["high"]  # 记录到目前为止当日的最高价
			elif one_tick["latest"] > current_bar["high"]:  # tick最新价创该分钟内新高
				current_bar["high"] = one_tick["latest"]  # 更新本根1min线的最高价

			if one_tick["low"] < day_low:  # 本分钟内见到了当日的新的最低价
				current_bar["low"] = one_tick["low"]  # 更新本根1min线的最低价
				day_low = one_tick["low"]  # 记录到目前为止当日的最低价
			elif one_tick["latest"] < current_bar["low"]:  # tick最新价创该分钟内新低
				current_bar["low"] = one_tick["latest"]  # 更新本根1min线的最低价

		current_bar["close"] = one_tick["latest"]  # 以tick最新价更新该1min线的收盘价

		ptr["last_bar_time"] = current_bar_time  # 记录当前1min线的时间戳

	else:  # 还在同一个分钟内的tick
		if one_tick["high"] > day_high:  # 本分钟内见到了当日的新的最高价
			current_bar["high"] = one_tick["high"]  # 更新本根1min线的最高价
			day_high = one_tick["high"]  # 记录到目前为止当日的最高价
		elif one_tick["latest"] > current_bar["high"]:  # tick最新价创该分钟内新高
			current_bar["high"] = one_tick["latest"]  # 更新本根1min线的最高价

		if one_tick["low"] < day_low:  # 本分钟内见到了当日的新的最低价
			current_bar["low"] = one_tick["low"]  # 更新本根1min线的最低价
			day_low = one_tick["low"]  # 记录到目前为止当日的最低价
		elif one_tick["latest"] < current_bar["low"]:  # tick最新价创该分钟内新低
			current_bar["low"] = one_tick["latest"]  # 更新本根1min线的最低价

		current_bar["close"] = one_tick["latest"]  # 以tick最新价更新该1min线的收盘价

	# 更新保存当日的最高最低价，其它数据结构在上面也已更新
	ptr["day_high"] = day_high
	ptr["day_low"] = day_low
