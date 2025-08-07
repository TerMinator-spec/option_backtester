import pandas as pd
import numpy as np
from datetime import timedelta
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

class BiDirectionalHedgedStraddleStrategy:
    def __init__(self, date,spot_df, options_df, max_loss=4000, sl_per_leg=None, target_per_leg=None):
        self.date = date
        self.spot_df = spot_df
        self.options_df = options_df
        self.max_loss = max_loss
        self.sl_per_leg = sl_per_leg
        self.target_per_leg = target_per_leg
        self.positions = []
        self.entry_time = None
        self.exit_time = None
        self.bias = None

    def calculate_bias(self, friday_920_df):
        friday_920_df['ema_50'] = friday_920_df['close'].ewm(span=50, adjust=False).mean()
        friday_920_df['ema_100'] = friday_920_df['close'].ewm(span=100, adjust=False).mean()
        last_row = friday_920_df.iloc[-1]
        if last_row['ema_50'] > last_row['ema_100']:
            self.bias = "positive"
        else:
            self.bias = "negative"

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

    def get_weekly_data(self):
        date_str = self.date.strftime('%Y-%m-%d')
        option_data = self.get_data_from_s3(date_str, 'nifty_options')
        expiry = option_data[0]['expiry']
        expiry = datetime.strptime(expiry.replace('Z', ''), "%Y-%m-%dT%H:%M:%S.%f")
        option_data = self.load_data(option_data)

        delta = timedelta(days=1)
        current = self.date + delta

        while current <= expiry:
            date_str = current.strftime('%Y-%m-%d')
            try:
                fetched_data = self.get_data_from_s3(date_str, 'nifty_options')
                fetched_data = self.load_data(fetched_data)
                option_data = pd.concat([option_data, fetched_data]).reset_index(drop =True)
            except Exception as e:
                print(f"❌ Error fetching data for {date_str}: {e}")
                pass
            
            current += delta

        return option_data
    
    def calculate_ema(self,series, period):
        return series.ewm(span=period, adjust=False).mean()

    def get_spot_hourly_data(self):
        
        
        start_date = self.date - timedelta(days=7)
        delta = timedelta(1)
        current = start_date
        final_data = pd.DataFrame()
        while current <= self.date:
            try:
                date_str = current.strftime('%Y-%m-%d')
                data = self.get_data_from_s3(date_str,'nifty_spot')
                data = pd.DataFrame(data)
                final_data = pd.concat([final_data, data])
                
            except Exception as e:
                print(e)
                pass
            current += delta

        final_data['datetime'] = pd.to_datetime(final_data['datetime'])
        self.spot_df = final_data.copy()
        spot_df = final_data.set_index('datetime')

        # Resample from 9:15 to 10:15, 10:15 to 11:15, etc.
        spot_hourly = spot_df.resample('1H', offset='15min').last()

        # Now calculate EMAs
        spot_hourly['ema_50'] = self.calculate_ema(spot_hourly['close'], 50)
        spot_hourly['ema_100'] = self.calculate_ema(spot_hourly['close'], 100)

        return spot_hourly

    def get_option_price(self, dt, strike, right):
        df = self.options_df
        mask = (
            (df['datetime'] == dt) &
            (df['strike'] == strike) &
            (df['right'].str.lower() == right.lower())
        )
        filtered = df[mask]
        if not filtered.empty:
            return float(filtered.iloc[0]['option_data']['open'])  # You can use 'ltp' if available
        return None

    def select_strikes(self, atm, time_str):
        # Get call and put hedges based on ~70–75 premium
        calls = self.options_df[
            (self.options_df['datetime'] == time_str) &
            (self.options_df['right'].str.lower() == 'call')
        ]
        puts = self.options_df[
            (self.options_df['datetime'] == time_str) &
            (self.options_df['right'].str.lower() == 'put')
        ]

        def find_strike(df, target_premium):
            df = df.copy()
            df['premium'] = df['option_data'].apply(lambda x: float(x['open']))
            df['diff'] = abs(df['premium'] - target_premium)
            return df.sort_values(by='diff').iloc[0]['strike'] if not df.empty else None

        call_hedge_strike = find_strike(calls, 75)
        put_hedge_strike = find_strike(puts, 75)

        return call_hedge_strike, put_hedge_strike

    def enter_trade(self, time_str):
        spot_price = float(self.spot_df[self.spot_df['datetime'] == time_str]['close'].iloc[0])
        atm = round(spot_price / 50) * 50

        call_hedge_strike, put_hedge_strike = self.select_strikes(atm, time_str)
        positions = []

        if self.bias == "positive":
            positions = [
                {"type": "sell", "right": "call", "strike": atm},
                {"type": "buy", "right": "call", "strike": call_hedge_strike},
                {"type": "sell", "right": "put", "strike": atm},
                {"type": "buy", "right": "put", "strike": put_hedge_strike},
            ]
        elif self.bias == "negative":
            positions = [
                {"type": "sell", "right": "put", "strike": atm},
                {"type": "buy", "right": "put", "strike": atm - 100},
                {"type": "sell", "right": "call", "strike": atm},
                {"type": "buy", "right": "call", "strike": call_hedge_strike},
            ]

        for pos in positions:
            entry_price = self.get_option_price(time_str, pos['strike'], pos['right'])
            if entry_price is not None:
                pos['entry_price'] = entry_price
                pos['exit_price'] = None
                pos['active'] = True
        self.positions = positions
        self.entry_time = time_str

    def update_pnl_and_exit(self):
        total_pnl = 0
        for time_str in self.spot_df[self.spot_df['datetime'] > self.entry_time]['datetime']:
            for pos in self.positions:
                if not pos['active']:
                    continue
                price_now = self.get_option_price(time_str, pos['strike'], pos['right'])
                if price_now is None:
                    continue

                pnl = (pos['entry_price'] - price_now) if pos['type'] == 'sell' else (price_now - pos['entry_price'])

                if self.sl_per_leg and pnl < -self.sl_per_leg:
                    pos['exit_price'] = price_now
                    pos['active'] = False
                elif self.target_per_leg and pnl > self.target_per_leg:
                    pos['exit_price'] = price_now
                    pos['active'] = False

            total_pnl = sum(
                (pos['entry_price'] - self.get_option_price(time_str, pos['strike'], pos['right']))
                if pos['type'] == 'sell' else
                (self.get_option_price(time_str, pos['strike'], pos['right']) - pos['entry_price'])
                for pos in self.positions if pos['active']
            )

            if total_pnl <= -self.max_loss:
                for pos in self.positions:
                    if pos['active']:
                        pos['exit_price'] = self.get_option_price(time_str, pos['strike'], pos['right'])
                        pos['active'] = False
                self.exit_time = time_str
                break

    def run(self,date):
        # 1. Find Friday 9:20 AM row
        self.spot_df['datetime'] = pd.to_datetime(self.spot_df['datetime'])
        self.spot_df['date'] = self.spot_df['datetime'].dt.date
        self.spot_df['time'] = self.spot_df['datetime'].dt.time

        #for date in sorted(self.spot_df['date'].unique()):
        friday_920_time = pd.to_datetime(f"{date} 09:20:00")
        # if friday_920_time not in self.spot_df['datetime'].values:
        #     continue

        # Filter till 9:20
        df_till_920 = self.spot_df[self.spot_df['datetime'] <= friday_920_time].copy()
        if len(df_till_920) < 100:
            return

        self.calculate_bias(df_till_920)
        self.enter_trade(friday_920_time.strftime('%Y-%m-%d %H:%M:%S'))
        self.update_pnl_and_exit()

          # Only first Friday, break after one week

        return self.positions, self.entry_time, self.exit_time
