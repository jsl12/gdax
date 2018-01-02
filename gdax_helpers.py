import pandas as pd
import os
from pathlib import Path
import gdax
from datetime import datetime, timedelta
import time
from api_key import *


def get_auth_client():
    return gdax.AuthenticatedClient(KEY,
                                    B64SECRET,
                                    PASSPHRASE)


def cols2nums(df, column_names):
    try:
        for c in column_names:
            df[c] = pd.to_numeric(df[c])
    except KeyError:
        pass
    return df


def cols2datetimes(df, column_names):
    try:
        for c in column_names:
            df[c] = pd.to_datetime(df[c])
    except KeyError:
        pass
    return df


def get_account_df(client):
    accounts = pd.DataFrame(client.get_accounts())
    accounts = accounts.set_index('currency')
    num_cols = ['available', 'balance', 'hold']
    accounts = cols2nums(accounts, num_cols)
    return accounts


def get_history_df(client, account_df):
    # Get history from api
    hist = {acc: client.get_account_history(row['id'])[0] for acc, row in account_df.iterrows()}

    # Convert to dataframes
    hist = {acc: pd.DataFrame(hist[acc]) for acc in hist}

    # Drop dataframes with no history
    hist = {acc: hist[acc] for acc in hist if not hist[acc].empty}

    # Convert numeric columns
    num_cols = ['amount', 'balance']
    hist = {acc: cols2nums(hist[acc], num_cols) for acc in hist}

    # Convert to datetimes and index by that
    date_cols = ['created_at']
    hist = {acc: cols2datetimes(hist[acc], date_cols) for acc in hist}
    hist = {acc: hist[acc].set_index(date_cols[0]) for acc in hist}

    # Convert into a single multi-indexed dataframe
    df = pd.concat([hist[acc] for acc in hist],
                   keys=list(hist.keys()))

    # Expand the details column
    df = pd.concat([df.drop(['details'], axis=1), df['details'].apply(pd.Series)], axis=1)

    # Add the payment column
    df = add_payment_col(df)
    return df


def add_payment_col(df):
    holdings = slice_holdings(df)
    holdings['payment'] = [findpayment(df, row) for i, row in holdings.iterrows()]
    df['payment'] = holdings['payment']
    return df


def slice_holdings(df):
    holdings = df.drop('USD', level=0)
    holdings = holdings[holdings['amount'] > 0]
    return holdings


def get_value_df(client, df):
    holdings = slice_holdings(df)
    holdings['price'] = [client.get_product_ticker(row.product_id)['price'] for i, row in holdings.iterrows()]
    holdings['price'] = pd.to_numeric(holdings['price'])
    holdings['value'] = holdings['price'] * holdings['amount']
    holdings['abs_gain'] = holdings['payment'] + holdings['value']
    holdings['gain_rate'] = (holdings['abs_gain'] / (-holdings['payment'])) * 100
    return holdings


def findpayment(df, holding):
    return df[df.trade_id == holding.trade_id].loc['USD'].amount.sum()


def time_series(start, end, n):
    # Like range(), but for time series
    t = end - start
    s = t.total_seconds() / n
    dt = timedelta(seconds=s)
    res = [start]
    for i in range(n - 1):
        res.append(res[-1] + dt)
    res.append(end)
    return res


def get_value_history(client, product, start, end=None, gran=None, sleep_time=.5):
    # df = check_for_price_data(product)
    # if df is None:
    df = pd.DataFrame()

    if end is None:
        end = datetime.now().replace(microsecond=0, second=0, minute=0)

    if gran is None or not isinstance(gran, timedelta):
        gran = timedelta(hours=1)

    gran = int(gran.total_seconds())
    num_results = (end - start).total_seconds() / gran

    try:
        if num_results > 200:
            overflow = num_results % 200
            num_results -= overflow
            num_reqs = int(num_results / 200)

            dt = timedelta(seconds=gran)
            print('{} requests, time per request: {}'.format(num_reqs, dt))

            temp_end = start + (200 * dt)
            for i in range(num_reqs):
                print(start.strftime('%Y-%m-%d %H:%M:%S'))
                # print(temp_end.strftime('%Y-%m-%d %H:%M:%S'))
                values = client.get_product_historic_rates(product,
                                                           granularity=gran,
                                                           start=start.isoformat(),
                                                           end=temp_end.isoformat())
                df = pd.concat([df, pd.DataFrame(values)])

                start = temp_end
                temp_end += (200 * dt)

                # Needed to avoid hitting the rate limit
                if num_reqs > 5:
                    time.sleep(sleep_time)
            temp_end = end
            values = client.get_product_historic_rates(product,
                                                       granularity=gran,
                                                       start=start.isoformat(),
                                                       end=temp_end.isoformat())
            df = pd.concat([df, pd.DataFrame(values)])
        else:
            df = pd.DataFrame(
                client.get_product_historic_rates(product,
                                                  granularity=gran,
                                                  start=start.isoformat(),
                                                  end=end.isoformat()))

        df.columns = ['time', 'low', 'high', 'open', 'close', 'volume']
        df['time'] = df['time'].apply(datetime.fromtimestamp)
        df = df.set_index('time')
        df = df.sort_index(axis=0)
        return df
    except ValueError as e:
                print(e, type(values))
                if isinstance(values, list):
                    print('length: {}'.format(len(values)))
                print(values)


def get_performance_history(client, holding_row, hour_res=1):
    df = get_value_history(client, holding_row.product_id, holding_row.name[1], gran=timedelta(hours=hour_res))
    df = df * holding_row.amount
    return df


def store_price_data(data, product, filename='prices.h5'):
    assert isinstance(data, pd.DataFrame), 'data is not a dataframe: {}'.format(type(data))
    assert isinstance(product, str), 'invalid product: {}'.format(type(product))
    assert isinstance(filename, str), 'invalid filename : {}'.format(type(filename))
    p = Path(os.getcwd()) / filename
    product = product.replace('-', '')
    print('storing {} in {}'.format(product, p))
    data.to_hdf(p, product)


def load_price_data(product, filename='prices.h5'):
    assert isinstance(product, str), 'invalid product: {}'.format(type(product))
    assert isinstance(filename, str), 'invalid filename : {}'.format(type(filename))
    try:
        df = pd.read_hdf(filename, product.replace('-', ''))
        print('loaded {} from {}'.format(product, filename))
        return df
    except KeyError as e:
        print('{}} not found'.format(product))
        pass
    except FileNotFoundError as e:
        print('{} not found'.format(filename))
        pass


if __name__ == '__main__':
    def main():
        ac = get_auth_client()
        print(ac.get_time())
        prod = 'BTC-USD'
        df = get_value_history(ac, prod, datetime.now() - timedelta(days=60), gran=timedelta(hours=1))
        print(df)
        store_price_data(df, prod)
        print(check_for_price_data(prod).info())
    main()
