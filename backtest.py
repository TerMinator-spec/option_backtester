# backtester.py

from datetime import datetime, time, timedelta
import pandas as pd
import json
import boto3
import json
from io import BytesIO
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Access them
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION")

# S3 setup - use your real keys if not configured globally
s3 = boto3.client(
    's3',
    aws_access_key_id = aws_access_key,
    aws_secret_access_key = aws_secret_key,
    region_name = aws_region
)

bucket_name = 'nifty-options-data-chokli'

class data_loader:
    def __init__(self, date):
        self.date = date
    
    def get_data_from_s3(self, folder='nifty_options'):
        file_key = f"{folder}/{self.date}.json"
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    def load_data(self):
        data = self.get_data_from_s3()
        flattened_data = []

        for item in data:
            base_info = {
                'date': item['date'],
                'strike': item['strike'],
                'atm': item['atm'],
                'right': item['right'],
                'expiry': item['expiry']
            }
            option_info = item['option_data']
            
            # Merge base info with option_data
            merged = {**base_info, **option_info}
            flattened_data.append(merged)

        # Create DataFrame
        flattened_data = pd.DataFrame(flattened_data)
        return flattened_data

class Strategy:
    def __init__(self, config):
        self.config = config

    def get_strikes(self, atm):
        strikes = []
        for leg in self.config['legs']:
            offset = leg['otm'] * 50 if 'otm' in leg else 0
            strike = atm + offset if leg['type'].lower() == 'call' else atm - offset
            strikes.append((strike, leg['type'].lower()))
        return strikes

class OptionBacktester:
    def __init__(self, data, data_spot,date,strategy_name, strategy_config_path='strategy_config.json'):
        self.data = data.copy()
        self.date = date
        self.data_spot = data_spot.copy()
        self.strategy_name = strategy_name
        self.config = self.load_strategy_config(strategy_config_path)
        self.strategy = Strategy(self.config)
        self.trade_log = []

    def load_strategy_config(self, path):
        with open(path, 'r') as f:
            strategies = json.load(f)
        return strategies[self.strategy_name]

    def run(self):
        self.run_day()
        return pd.DataFrame(self.trade_log)

    def run_day(self):
        #day_data = self.data[self.data['date'] == date].copy()
        day_data = self.data
        day_data['datetime'] = pd.to_datetime(day_data['datetime'])
        entry_time = self.config['entry_time']
        exit_time = self.config['exit_time']
        reentry = self.config.get('reentry_on_sl', False)
        reentry_no = self.config.get('max_rentries', 0)
        max_loss = self.config.get('max_loss', None)
        
        data_spot = self.data_spot.copy()
        data_spot['datetime'] = pd.to_datetime(data_spot['datetime'])
        data_spot_price = float(data_spot[(data_spot['datetime'].dt.strftime('%H:%M') >= entry_time)]['close'].iloc[0])
        atm_strike = int(round(data_spot_price / 50) * 50)

        #atm_strike = day_data['atm'].iloc[0]
        strike_pairs = self.strategy.get_strikes(atm_strike)
        #print('strike_pairs', strike_pairs)

        active_legs = []

        # Initialize entry for each leg
        for strike, opt_type in strike_pairs:
            leg_data = day_data[
                (day_data['strike'] == strike) &
                (day_data['right'].str.lower() == opt_type) &
                (day_data['datetime'].dt.strftime('%H:%M') >= entry_time)
            ].copy()
            if leg_data.empty:
                continue

            entry_row = leg_data.iloc[0]
            entry_time_leg = entry_row['datetime']
            entry_price = float(entry_row['open'])
            sl_price = entry_price * (1 + self.config['stop_loss'])
            tgt_price = entry_price * (1 - self.config['target']) if self.config['target'] else None

            active_legs.append({
                "strike": strike,
                "type": opt_type,
                "leg_data": leg_data,
                "entry_time": entry_time_leg,
                "entry_price": entry_price,
                "sl_price": sl_price,
                "tgt_price": tgt_price,
                "status": "active",
                "exit_price": None,
                "exit_reason": None,
                "reentries": 0
            })
        #print(active_legs)
        # Start iterating through minute-by-minute to simulate live P&L
        timestamps = sorted(day_data['datetime'].unique())
        static_sl = 0
        static_trgt = 0
        for timestamp in timestamps:
            total_pnl = 0

            for leg in active_legs:
                if leg["status"] != "active":
                    continue

                leg_candles = leg["leg_data"]
                candle = leg_candles[leg_candles['datetime'] == timestamp]
                if candle.empty:
                    continue

                row = candle.iloc[0]
                high_price = float(row['high'])
                low_price = float(row['low'])
                

                # Check target
                if leg["tgt_price"] and low_price <= leg["tgt_price"]:
                    self.trade_log.append({
                        "date": self.date,
                        "strike": leg["strike"],
                        "type": leg["type"],
                        "exit_reason": "target",
                        "entry_time": leg["entry_time"],
                        "exit_time": timestamp,
                        "entry_price": leg["entry_price"],
                        "exit_price": low_price,
                        "pnl": leg["entry_price"] - low_price,
                        "reentry_id": leg["reentries"]
                    })
                    leg.update({
                        "status": "closed",
                        "exit_reason": "target",
                        "exit_price": low_price
                    })
                    static_trgt += leg["entry_price"] - low_price
                    continue

                # Check stop loss
                if high_price >= leg["sl_price"]:
                    self.trade_log.append({
                        "date": self.date,
                        "strike": leg["strike"],
                        "type": leg["type"],
                        "exit_reason": "stop_loss",
                        "entry_time": leg["entry_time"],
                        "exit_time": timestamp,
                        "entry_price": leg["entry_price"],
                        "exit_price": high_price,
                        "pnl": leg["entry_price"] - high_price,
                        "reentry_id": leg["reentries"]
                    })
                    leg.update({
                        "status": "closed",
                        "exit_reason": "stop_loss",
                        "exit_price": high_price
                    })
                    static_sl += leg["entry_price"] - high_price

                    if reentry and leg["reentries"] < reentry_no:
                        # reentry starts from next candle
                        next_leg_data = leg_candles[leg_candles['datetime'] > timestamp]
                        if not next_leg_data.empty:
                            new_entry = next_leg_data.iloc[0]
                            entry_price = float(new_entry['open'])
                            leg.update({
                                "entry_time": new_entry['datetime'],
                                "entry_price": entry_price,
                                "sl_price": entry_price * (1 + self.config['stop_loss']),
                                "tgt_price": entry_price * (1 - self.config['target']) if self.config['target'] else None,
                                "status": "active",
                                "reentries": leg["reentries"] + 1
                            })

                # Calculate current P&L for the leg
                if leg["status"] == "active":
                    current_price = float(row['close'])
                    leg_pnl = leg["entry_price"] - current_price  # short premium
                    total_pnl += leg_pnl
                # elif leg["exit_price"] is not None:
                #     leg_pnl = leg["entry_price"] - leg["exit_price"]
                #     total_pnl += leg_pnl
            total_pnl += static_sl + static_trgt
            if max_loss is not None and total_pnl <= -abs(max_loss):
                for leg in active_legs:
                    if leg["status"] == "active":
                        candle = leg["leg_data"][leg["leg_data"]['datetime'] == timestamp]
                        if not candle.empty:
                            exit_price = float(candle.iloc[0]['close'])
                            self.trade_log.append({
                                "date": self.date,
                                "strike": leg["strike"],
                                "type": leg["type"],
                                "exit_reason": "max_loss_hit",
                                "entry_time": leg["entry_time"],
                                "exit_time": timestamp,
                                "entry_price": leg["entry_price"],
                                "exit_price": exit_price,
                                "pnl": leg["entry_price"] - exit_price,
                                "reentry_id": leg["reentries"]
                            })
                            leg.update({
                                "status": "closed",
                                "exit_reason": "max_loss_hit",
                                "exit_price": float(candle.iloc[0]['close'])
                            })
                break  # Exit all trades for the day

        # Save trades
        for leg in active_legs:
            if leg["status"] != "closed":
                exit_price = float(candle.iloc[0]['close'])
                self.trade_log.append({
                    "date": self.date,
                    "strike": leg["strike"],
                    "type": leg["type"],
                    "exit_reason": 'day end',
                    "entry_time": leg["entry_time"],
                    "exit_time": timestamp,
                    "entry_price": leg["entry_price"],
                    "exit_price": exit_price,
                    "pnl": leg["entry_price"] - exit_price,
                    "reentry_id": leg["reentries"]
                })

if __name__ == "__main__":

    start_date = datetime(2024, 7, 5)
    end_date = datetime(2024, 7, 12)
    delta = timedelta(days=1)
    current = start_date
    final_df = pd.DataFrame()
    while current <= end_date:
        if current.weekday() >= 5:
            current += delta
            continue
        try:
            data_class = data_loader(current.strftime('%Y-%m-%d'))
            data = data_class.load_data()
            spot_data = pd.DataFrame(data_class.get_data_from_s3(folder='nifty_spot'))
            #print(data)

            bactest_class = OptionBacktester(data, spot_data, current.strftime('%Y-%m-%d'), 'straddle', strategy_config_path='strategy_config.json')
            bactest_results = bactest_class.run()
            print(bactest_results)
            final_df = pd.concat([final_df, bactest_results], ignore_index=True)

            current += delta
        except Exception as e:
            print(f"Error processing date {current.strftime('%Y-%m-%d')}: {e}")
            current += delta

    final_df.to_csv('backtest_results.csv', index=False)
    # data_class = data_loader('2025-02-03')
    # data = data_class.load_data()
    # #print(data)

    # bactest_class = OptionBacktester(data, '2025-02-03', 'strangle', strategy_config_path='strategy_config.json')
    # bactest_results = bactest_class.run()
    # print(bactest_results)
