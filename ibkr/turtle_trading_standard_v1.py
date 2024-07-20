import time
from datetime import datetime
import pytz
import pandas as pd
from market_data_module import MarketDataProcessor
from trading_logic import TradingLogic
from logger_module import setup_logger
from redis_module import RedisClient
from ib_insync import *

class TurtleTradingTask:

    def __init__(self, client_id, cooldown_days, trade_quantity, mode, contract, exitPt=0.3, totalMoney=10000.0,
                 atr_multiple=2.0):
        self.logger = setup_logger(__name__, 'turtle_trading.log')
        self.cooldown_days = cooldown_days
        self.trade_quantity = trade_quantity
        self.mode = mode
        self.contract = contract
        self.exitPt = exitPt
        self.totalMoney = totalMoney
        self.atr_multiple = atr_multiple
        self.client_id = client_id
        self.redis_client = RedisClient()
        self.trading_logic = TradingLogic(cooldown_days, trade_quantity, totalMoney, atr_multiple, exitPt, self.redis_client)

    def record_trade_to_redis(self, contract_symbol, tradePrice=0, shortPrice=0):
        trade_time = datetime.now(pytz.timezone('US/Eastern')).isoformat()
        value = {
            'time': trade_time,
            'tradePrice': tradePrice,
            'shortPrice': shortPrice,
        }
        self.redis_client.set(contract_symbol, value)

    def on_mkt_data_update(self, ticker, ib, contract, data, is_long, last_long_signal, average_entry_price,
                           total_position_quantity, total_pnl, is_short, average_entry_price_short,
                           total_position_quantity_short):
        new_bar = pd.DataFrame([{
            'date': datetime.now(pytz.timezone('US/Eastern')),
            'open': ticker.open,
            'high': ticker.high,
            'low': ticker.low,
            'close': ticker.last,
            'volume': ticker.volume
        }])
        if new_bar.isna().any().any():
            self.logger.info(f"New bar is missing data for {contract.symbol}")
            return
        new_bar.set_index('date', inplace=True)
        if new_bar.index[-1] > data.index[-1]:
            data = pd.concat([data, new_bar])
        data = MarketDataProcessor.calculate_indicators(data)
        latest_bar = data.iloc[-1]
        avgCost, long_quantity, avgCost_short, short_quantity = self.trading_logic.get_specified_stock_positions(ib,
                                                                                                                 [contract.symbol])
        if self.mode in ['long', 'both']:
            long_condition = self.trading_logic.should_long(latest_bar, avgCost, long_quantity)
            exit_long = self.trading_logic.should_exit_long(latest_bar, avgCost, contract.symbol)
            if long_condition and not self.trading_logic.is_in_cooldown(ib, contract.symbol):
                limit_price = latest_bar['highest_high']
                self.logger.info(f"start long sign trade {contract.symbol}, sign_price {limit_price}")
                trade = self.trading_logic.place_limit_order(ib, contract, 'BUY', self.trade_quantity, limit_price)
                if self.trading_logic.wait_for_fill(ib, trade):
                    avgCost, long_quantity, _, _ = self.trading_logic.get_specified_stock_positions(ib, [contract.symbol])
                    is_long = True
                    # self.trading_logic.local_last_trade_time = datetime.now(pytz.timezone('US/Eastern'))
                    self.record_trade_to_redis(contract.symbol, tradePrice=limit_price)
                else:
                    ib.cancelOrder(trade.order)
                    market_order = MarketOrder('BUY', self.trade_quantity)
                    market_trade = ib.placeOrder(contract, market_order)
                    if self.trading_logic.wait_for_fill(ib, market_trade):
                        avgCost, long_quantity, _, _ = self.trading_logic.get_specified_stock_positions(ib, [contract.symbol])
                        is_long = True
                        # self.trading_logic.local_last_trade_time = datetime.now(pytz.timezone('US/Eastern'))
                        self.record_trade_to_redis(contract.symbol, tradePrice=latest_bar['close'])
                    else:
                        self.logger.error(f"long sign buy order not filled for {contract.symbol}")
            elif exit_long and long_quantity > 0:
                limit_price = latest_bar['close'] * 0.999
                trade = self.trading_logic.place_limit_order(ib, contract, 'SELL', long_quantity, limit_price)
                self.logger.info(f"start exit long sign trade {contract.symbol}, sign_price {limit_price}")
                if self.trading_logic.wait_for_fill(ib, trade):
                    last_trade_pnl, total_pnl, is_long = self.trading_logic.close_position(trade.fillPrice, total_pnl,
                                                                                          avgCost, long_quantity)
                    avgCost, long_quantity, _, _ = self.trading_logic.get_specified_stock_positions(ib, [contract.symbol])
                    # self.trading_logic.local_last_trade_time = datetime.now(pytz.timezone('US/Eastern'))
                    self.record_trade_to_redis(contract.symbol)
                else:
                    ib.cancelOrder(trade.order)
                    market_order = MarketOrder('SELL', long_quantity)
                    market_trade = ib.placeOrder(contract, market_order)
                    ib.sleep(3)
                    if market_trade.orderStatus.status == 'Filled':
                        last_trade_pnl, total_pnl, is_long = self.trading_logic.close_position(market_trade.fillPrice,
                                                                                              total_pnl, avgCost,
                                                                                              long_quantity)
                        avgCost, long_quantity, _, _ = self.trading_logic.get_specified_stock_positions(ib, [contract.symbol])
                        # self.trading_logic.local_last_trade_time = datetime.now(pytz.timezone('US/Eastern'))
                        self.record_trade_to_redis(contract.symbol)
                    else:
                        self.logger.error(f"exit long buy order not filled for {contract.symbol}")

        if self.mode in ['short', 'both']:
            short_condition = self.trading_logic.should_short(latest_bar)
            exit_short = self.trading_logic.should_exit_short(latest_bar, contract.symbol)
            if short_condition and not self.trading_logic.is_in_cooldown(ib, contract.symbol):
                limit_price = latest_bar['lowest_low']
                self.logger.info(f"start short sign trade {contract.symbol}, sign_price {limit_price}")
                trade = self.trading_logic.place_limit_order(ib, contract, 'SELL', self.trade_quantity, limit_price)
                if self.trading_logic.wait_for_fill(ib, trade):
                    _, _, avgCost_short, short_quantity = self.trading_logic.get_specified_stock_positions(ib, [contract.symbol])
                    is_short = True
                    # self.trading_logic.local_last_trade_time = datetime.now(pytz.timezone('US/Eastern'))
                    self.record_trade_to_redis(contract.symbol, shortPrice=limit_price)
                else:
                    ib.cancelOrder(trade.order)
                    market_order = MarketOrder('SELL', self.trade_quantity)
                    market_trade = ib.placeOrder(contract, market_order)
                    ib.sleep(1)
                    if market_trade.orderStatus.status == 'Filled':
                        _, _, avgCost_short, short_quantity = self.trading_logic.get_specified_stock_positions(ib, [contract.symbol])
                        is_short = True
                        # self.trading_logic.local_last_trade_time = datetime.now(pytz.timezone('US/Eastern'))
                        self.record_trade_to_redis(contract.symbol, shortPrice=latest_bar['close'])
                    else:
                        self.logger.error(f"short sell order not filled for {contract.symbol}")
            elif exit_short and short_quantity > 0:
                limit_price = latest_bar['close'] * 1.001
                trade = self.trading_logic.place_limit_order(ib, contract, 'BUY', short_quantity, limit_price)
                self.logger.info(f"start exit short sign trade {contract.symbol}, sign_price {limit_price}")
                if self.trading_logic.wait_for_fill(ib, trade):
                    last_trade_pnl, total_pnl, is_short = self.trading_logic.close_position(trade.fillPrice, total_pnl,
                                                                                           avgCost_short, short_quantity)
                    _, _, avg_cost_short, short_quantity = self.trading_logic.get_specified_stock_positions(ib, [contract.symbol])
                    # self.trading_logic.local_last_trade_time = datetime.now(pytz.timezone('US/Eastern'))
                    self.record_trade_to_redis(contract.symbol)
                else:
                    ib.cancelOrder(trade.order)
                    market_order = MarketOrder('BUY', short_quantity)
                    market_trade = ib.placeOrder(contract, market_order)
                    if self.trading_logic.wait_for_fill(ib, market_trade):
                        last_trade_pnl, total_pnl, is_short = self.trading_logic.close_position(market_trade.fillPrice,
                                                                                              total_pnl, avgCost_short,
                                                                                              short_quantity)
                        _, _, avg_cost_short, short_quantity = self.trading_logic.get_specified_stock_positions(ib,
                                                                                                               [contract.symbol])
                        # self.trading_logic.local_last_trade_time = datetime.now(pytz.timezone('US/Eastern'))
                        self.record_trade_to_redis(contract.symbol)
                    else:
                        self.logger.error(f"exit short sell order not filled for {contract.symbol}")

        if is_long:
            current_pnl = (latest_bar['close'] - avgCost) * long_quantity
            self.logger.info(f"Current unrealized PnL for {contract.symbol} (long): ${current_pnl:.2f}")
        if is_short:
            current_pnl_short = (avgCost_short - latest_bar['close']) * short_quantity
            self.logger.info(f"Current unrealized PnL for {contract.symbol} (short): ${current_pnl_short:.2f}")
        print(f"Last update for {contract.symbol}: {datetime.now(pytz.timezone('US/Eastern'))}, Close: {latest_bar['close']:.2f}, EMA: {latest_bar['ema']:.2f}")
        print(f"Total realized PnL for {contract.symbol}: ${total_pnl:.2f}")

    def turtle_trading_system(self, ib):
        is_long = False
        last_long_signal = 0
        average_entry_price = 0.0
        total_position_quantity = 0
        total_pnl = 0.0
        is_short = False
        average_entry_price_short = 0.0
        total_position_quantity_short = 0
        contract = self.contract
        # No need to recover state from Redis for positions and prices
        # Fetch historical data
        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='4 M',
            barSizeSetting='1 day', whatToShow='TRADES', useRTH=True
        )
        data = util.df(bars)
        if hasattr(data, 'set_index'):
            data = data.set_index('date')
            print(f"History data is ready for {contract.symbol}")
        else:
            return
        data.index = pd.to_datetime(data.index, utc=True).tz_convert(pytz.timezone('US/Eastern'))
        # Calculate initial indicators
        data = MarketDataProcessor.calculate_indicators(data)
        # Subscribe to real-time market data
        ticker = ib.reqMktData(contract, '', False, False)

        def on_mkt_data(ticker):
            self.on_mkt_data_update(
                ticker, ib, contract, data.copy(), is_long, last_long_signal,
                average_entry_price, total_position_quantity, total_pnl,
                is_short, average_entry_price_short, total_position_quantity_short
            )
        # Register the callback for market data
        ticker.updateEvent += on_mkt_data
        # Start event loop
        ib.run()

    def start_trading_system(self):
        ib = IB()
        ib.connect('127.0.0.1', 4002, self.client_id)
        self.turtle_trading_system(ib)
        ib.disconnect()

if __name__ == "__main__":
    cooldown_days = 1
    trade_quantity = 100
    mode = 'both'
    contract = Stock('IBIT', 'SMART', 'USD')
    trade = TurtleTradingTask(1,cooldown_days, trade_quantity, mode, contract, 0.3, 10000, 2)
    trade.start_trading_system()