import time
from datetime import datetime, timedelta

import pytz
from ib_insync import *

EST = pytz.timezone('US/Eastern')


class TradingLogic:
    def __init__(self, cooldown_days, trade_quantity, total_money, atr_multiple, exit_pt, redis_client):
        self.cooldown_days = cooldown_days
        self.trade_quantity = trade_quantity
        self.total_money = total_money
        self.atr_multiple = atr_multiple
        self.exit_pt = exit_pt
        self.local_last_trade_time = None
        self.redis_client = redis_client

    @staticmethod
    def get_last_trade_info(ib, symbol, side=None):
        """
        "BUY"：表示买入。
        "SELL"：表示卖出。
        "SSHORT"：表示卖空（即对某只证券进行做空）。
        "SLONG"：表示对冲空头部位（这更多出现在期货、市商、机构交易中）。
        """
        # 创建一个ExecutionFilter对象
        exec_filter = ExecutionFilter()
        exec_filter.symbol = symbol
        if side:
            exec_filter.side = side
        # exec_filter.side = 'SELL'
        # 请求执行记录
        executions = ib.reqExecutions(exec_filter)

        if not executions:
            print(f"No executions found for {symbol}")
            return None, None

        # 找到最新的执行记录
        last_execution = max(executions, key=lambda x: x.time)
        last_trade_time = last_execution.time.astimezone(EST)
        last_trade_price = last_execution.execution.price
        return last_trade_time, last_trade_price

    @staticmethod
    def get_specified_stock_positions(ib, symbols):
        positions = ib.positions()
        long_quantity = 0
        short_quantity = 0
        avgCost = 0
        avgCost_short = 0
        for position in positions:
            if position.contract.secType == 'STK' and position.contract.symbol in symbols:
                if position.position > 0:
                    avgCost = position.avgCost
                    long_quantity = position.position
                elif position.position < 0:
                    avgCost_short = position.avgCost
                    short_quantity = abs(position.position)
                break
        return avgCost, long_quantity, avgCost_short, short_quantity

    @staticmethod
    def place_limit_order(ib, contract, action, quantity, limit_price):
        order = LimitOrder(action, quantity, limit_price)
        trade = ib.placeOrder(contract, order)
        return trade

    @staticmethod
    def wait_for_fill(ib, trade, timeout=30):
        start_time = time.time()
        while time.time() - start_time < timeout:
            ib.sleep(1)
            if trade.orderStatus.status == 'Filled':
                return True
        return False

    def get_last_long_price(self, contract_symbol):
        last_trade = self.redis_client.get(contract_symbol)
        if last_trade and last_trade.get('tradePrice', 0) > 0:
            return last_trade['tradePrice']
        else:
            return None

    def get_last_short_price(self, contract_symbol):
        last_trade = self.redis_client.get(contract_symbol)
        if last_trade and last_trade.get('shortPrice', 0) > 0:
            return last_trade['shortPrice']
        else:
            return None

    def is_in_cooldown(self, ib, contract_symbol):

        last_trade_time, _ = TradingLogic.get_last_trade_info(ib, contract_symbol)
        if last_trade_time is None:
            last_trade_info = self.redis_client.get(contract_symbol)
            if last_trade_info:
                last_trade_time = datetime.fromisoformat(last_trade_info['time']).astimezone(EST)

        current_time = datetime.now(EST)
        cooldown_delta = timedelta(days=self.cooldown_days)
        if current_time - last_trade_time < cooldown_delta:
            return True
        return False


    def should_short(self, latest_bar):
        if not all(key in latest_bar for key in ['close', 'lowest_low', 'ema']):
            raise KeyError("Missing required keys in latest_bar")
        close_price = latest_bar['close']
        if close_price == 0:
            return False
        condition1 = close_price < latest_bar['lowest_low']
        condition2 = close_price < latest_bar['ema']
        return condition1 and condition2


    def should_exit_short(self, latest_bar, contract_symbol):
        if not all(key in latest_bar for key in ['close', 'exit_high', 'ema', 'atr']):
            raise KeyError("Missing required keys in latest_bar")
        close_price = latest_bar['close']
        last_short_price = self.get_last_short_price(contract_symbol)
        if close_price == 0:
            return False
        condition1 = close_price > latest_bar['exit_high']
        condition2 = close_price > latest_bar['ema']
        if last_short_price is not None and latest_bar['atr'] > 0:
            condition3 = close_price > last_short_price and (close_price - last_short_price) > self.atr_multiple * \
                         latest_bar['atr']
            return condition1 or condition2 or condition3
        else:
            return condition1 or condition2


    def should_long(self, latest_bar, avg_cost, total_quantity):
        if not all(key in latest_bar for key in ['close', 'highest_high', 'ema']):
            raise KeyError("Missing required keys in latest_bar")
        close_price = latest_bar['close']
        if close_price == 0:
            return False
        condition1 = close_price > latest_bar['highest_high']
        condition2 = close_price > latest_bar['ema']
        condition3 = True
        if avg_cost > 0 and total_quantity > 0 and self.total_money > 0 and self.trade_quantity > 0:
            condition3 = self.total_money > avg_cost * total_quantity + self.trade_quantity * close_price
        return condition1 and condition2 and condition3


    def should_exit_long(self, latest_bar, avg_cost, contract_symbol):
        if not all(key in latest_bar for key in ['close', 'exit_low', 'ema', 'atr']):
            raise KeyError("Missing required keys in latest_bar")
        close_price = latest_bar['close']
        last_trade_price = self.get_last_long_price(contract_symbol)
        if close_price == 0 or avg_cost == 0:
            return False
        condition1 = close_price < latest_bar['exit_low']
        condition2 = close_price < latest_bar['ema']
        condition3 = (close_price - avg_cost) / avg_cost > self.exit_pt
        if last_trade_price is not None and latest_bar['atr'] > 0:
            condition4 = close_price < last_trade_price and (last_trade_price - close_price) > self.atr_multiple * \
                         latest_bar['atr']
            return condition1 or condition2 or condition3 or condition4
        else:
            return condition1 or condition2 or condition3


if __name__ == '__main__':
    ib = IB()
    ib.connect('127.0.0.1', 4002, 1)
    trade_time, trade_price = TradingLogic.get_last_trade_info(ib, "IBIT", 'SELL')

    print(trade_time, trade_price)
