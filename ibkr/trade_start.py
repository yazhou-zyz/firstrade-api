import multiprocessing
from datetime import datetime

import pytz
from ib_insync import *

from ibkr.turtle_trading_standard_v1 import TurtleTradingTask

EST = pytz.timezone('US/Eastern')  # 美东时区

def start_trading_system(client_id,contract, cooldown_days, trade_quantity, mode,exitPt, totalMoney, atr_multiple):
    turtle_trading = TurtleTradingTask(client_id,cooldown_days, trade_quantity, mode, contract,exitPt, totalMoney, atr_multiple)
    turtle_trading.start_trading_system()

if __name__ == "__main__":
    # cooldown_days = 1
    # trade_quantity = 100
    # mode = 'both'

    contract_process_map = {
        'TQQQ': {
            'contract': Stock('TQQQ', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 100,
            'mode': 'both',
            'exitPt': 0.3,
            'totalMoney': 10000,
            'atr_multiple': 2,
            'client_id': 1

        },
        'TSLA': {
            'contract': Stock('TSLA', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 10,
            'mode': 'both',
            'exitPt': 0.3,
            'totalMoney': 20000,
            'atr_multiple': 2,
            'client_id': 2

        },
        'AAPL': {
            'contract': Stock('AAPL', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 10,
            'mode': 'both',
            'exitPt': 0.3,
            'totalMoney': 20000,
            'atr_multiple': 2,
            'client_id': 3
        },

        'NVDA': {
            'contract': Stock('NVDA', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 20,
            'mode': 'both',
            'exitPt': 0.3,
            'totalMoney': 20000,
            'atr_multiple': 2,
            'client_id': 4

        },
        'MSFT': {
            'contract': Stock('MSFT', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 5,
            'mode': 'both',
            'exitPt': 0.3,
            'totalMoney': 20000,
            'atr_multiple': 2,
            'client_id': 5
        },
        'SOXL': {
            'contract': Stock('SOXL', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 100,
            'mode': 'both',
            'exitPt': 0.4,
            'totalMoney': 20000,
            'atr_multiple': 3,
            'client_id': 6
        },
        'SPY': {
            'contract': Stock('SPY', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 5,
            'mode': 'long',
            'exitPt': 0.4,
            'totalMoney': 20000,
            'atr_multiple': 2,
            'client_id': 7
        },
        'TNA': {
                'contract': Stock('TNA', 'SMART', 'USD'),
                'cooldown_days': 1,
                'trade_quantity': 100,
                'mode': 'long',
                'exitPt': 0.4,
                'totalMoney': 20000,
                'atr_multiple': 2,
                'client_id': 8
            },
        'QQQ': {
            'contract': Stock('QQQ', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 5,
            'mode': 'both',
            'exitPt': 0.3,
            'totalMoney': 20000,
            'atr_multiple': 2,
            'client_id': 9
        },
        'IBIT': {
            'contract': Stock('IBIT', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 100,
            'mode': 'both',
            'exitPt': 0.4,
            'totalMoney': 20000,
            'atr_multiple': 3,
            'client_id': 10
        },
        'AMD': {
            'contract': Stock('AMD', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 20,
            'mode': 'both',
            'exitPt': 0.4,
            'totalMoney': 20000,
            'atr_multiple': 3,
            'client_id': 11
        },
        'META': {
            'contract': Stock('META', 'SMART', 'USD'),
            'cooldown_days': 1,
            'trade_quantity': 5,
            'mode': 'both',
            'exitPt': 0.3,
            'totalMoney': 20000,
            'atr_multiple': 2,
            'client_id': 12
        },
    }


    while True:
        current_time = datetime.now(EST)
        # Check if it's trading hours (assumes US market hours from 9:30 AM to 4 PM Eastern time)
        # if current_time.weekday() >= 5 or current_time.hour < 9 or (
        #         current_time.hour == 9 and current_time.minute < 30) or current_time.hour >= 16:
        #     print("Market is closed. Waiting for next trading day.")
        #     time.sleep(3600)  # Wait for one hour
        # else:
        # Run trading systems concurrently
        # with multiprocessing.Pool(processes=len(contracts)) as pool:
        #     pool.starmap(start_trading_system,
        #                  [(contract, cooldown_days, trade_quantity, mode) for contract in contracts])
        # # Exit the loop once trading systems have run
        processes = []
        for process_name, contract_info in contract_process_map.items():
            process = multiprocessing.Process(
                name=process_name,
                target=start_trading_system,
                args=(
                    contract_info['client_id'],
                    contract_info['contract'],
                    contract_info['cooldown_days'],
                    contract_info['trade_quantity'],
                    contract_info['mode'],
                    contract_info['exitPt'],
                    contract_info['totalMoney'],
                    contract_info['atr_multiple']

                )
            )
            processes.append(process)
            process.start()

            # Wait for all processes to complete
        for process in processes:
            process.join()

        # break
