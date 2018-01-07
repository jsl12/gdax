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
    # df = add_payment_col(df)
    # ^ Throwing an error for some reason?
    return df


def round_time(time):
    # Converts the time to only have hours
    assert isinstance(time, datetime), 'time is not a datetime object: {}'.format(type(time))
    return time.replace(minute=0, second=0, microsecond=0)


def get_value_history(client, product, start, end=None, gran=None, sleep_time=.5, load=True):
    # Either loads existing data and/or fetches new data to cover the time period designated by start and end

    # Check for existing data
    if load:
        df = load_price_data(product)
        if df is None:
            df = pd.DataFrame()
        else:
            print('found {} data'.format(product))
            print('from {}\nto   {}'.format(df.index[0], df.index[-1]))
    else:
        df = pd.DataFrame()

    # Rounds the incoming data
    if end is None:
        end = round_time(datetime.now())
    else:
        end = round_time(end)
    start = round_time(start)

    # Sets the granularity
    def_gran = timedelta(hours=1)
    if gran is None or not isinstance(gran, timedelta):
        gran = def_gran
    elif gran < def_gran:
        gran = def_gran
    gran = int(gran.total_seconds())

    def get_mult(client, product, start, end, gran, sleep_time):
        df = pd.DataFrame()
        num_results = (end - start).total_seconds() / gran
        if num_results > 200:
            overflow = num_results % 200
            num_results -= overflow
            num_reqs = int(num_results / 200)

            dt = timedelta(seconds=gran)
            print('{} requests\ndt: {}\ntotal: {}'.format(num_reqs + 1, dt, dt * 200))

            temp_end = start + (200 * dt)
            for i in range(num_reqs):
                print(start.strftime('%Y-%m-%d %H:%M:%S'))
                values = client.get_product_historic_rates(product,
                                                           granularity=gran,
                                                           start=start.isoformat(),
                                                           end=temp_end.isoformat())
                if not isinstance(values, dict):
                    df = pd.concat([df, pd.DataFrame(values)])
                    start = temp_end
                    temp_end += (200 * dt)

                    # Needed to avoid hitting the rate limit
                    if num_reqs > 5:
                        time.sleep(sleep_time)
                else:
                    print(values)
            temp_end = end
            values = client.get_product_historic_rates(product,
                                                       granularity=gran,
                                                       start=start.isoformat(),
                                                       end=temp_end.isoformat())
            df = pd.concat([df, pd.DataFrame(values)])
        else:
            values = client.get_product_historic_rates(product,
                                                       granularity=gran,
                                                       start=start.isoformat(),
                                                       end=end.isoformat())
            if not isinstance(values, dict):
                df = pd.DataFrame(values)
            else:
                print(values)

        if not df.empty:
            df.columns = ['time', 'low', 'high', 'open', 'close', 'volume']
            df['time'] = df['time'].apply(datetime.fromtimestamp)
            df = df.set_index('time')
            df = df.sort_index(axis=0)
            return df

    if df.empty:
        df = get_mult(client, product, start, end, gran, sleep_time)
    elif start < df.index[0]:
        print('Filling from\n{}\nto\n{}'.format(start, df.index[0]))
        df = pd.concat([df,
                        get_mult(client, product, start, df.index[0], gran, sleep_time)],
                       axis=0,
                       join='inner')
        df = df.sort_index(axis=0)

    time.sleep(sleep_time)

    if end > df.index[-1]:
        print('Filling from\n{}\nto\n{}'.format(df.index[-1], end))
        df = pd.concat([df,
                        get_mult(client, product, df.index[-1], end, gran, sleep_time)],
                       axis=0,
                       join='inner')
        df = df.sort_index(axis=0)

    store_price_data(df, product)
    df = df[~df.index.duplicated(keep='first')]
    return df[df.index > start]


def findpayment(df, holding):
    return df[df.trade_id == holding.trade_id].loc['USD'].amount.sum()


def get_portfolio_history(client, type='rate', hour_res=1):
    # Returns a dictionary with keys of product_ids and values of DataFrames
    # that contain the high, low, close, etc. data

    dfhist = get_history_df(client, get_account_df(client)).sort_index()

    # Gets the total histories of all the holdings
    holdings = dfhist.drop('USD', level=0)

    # Adds the payment column for the holdings, so that the principal series can be calculated
    for i, row in holdings.iterrows():
        pymt = -findpayment(dfhist, row)
        dfhist.loc[i, 'payment'] = pymt

    # Calculates the principal series
    prin = dfhist.payment.dropna().cumsum().reset_index(level=0).drop('level_0', axis='columns').iloc[:, 0]

    # Find the set of unique product_ids
    prods = set(holdings['product_id'].values)

    # Makes a dictionary where the key is the product_id and the value
    # is a DataFrame of the low, high, close, etc. values that starts at the
    # time of the earliest holding of that product_id
    prod_values = {}
    for p in prods:
        start = holdings[holdings['product_id'] == p].index[0][1]
        print('{} Starting {}'.format(p, start))
        prod_values[p] = get_value_history(client, p, start=start)

    # Gets a master index of all the prices
    idx = pd.Index([])
    for p in prod_values:
        idx = idx.union(prod_values[p].index)

    # Reindex all the DataFrames in prod_values
    prod_values = {p: prod_values[p].reindex(idx).fillna(0) for p in prod_values}

    # Creates a dictionary of the balance series for each product_id
    balances = {p: holdings[holdings.loc[:, 'product_id'] == p].loc[:, 'balance'] for p in prods}

    # Reindexes the dictionary values to include all the values from idx
    for p, val in balances.items():
        balances[p] = balances[p].reset_index(level=0)
        balances[p] = balances[p].drop('level_0', axis='columns')
        balances[p] = balances[p].reindex(idx, method='pad').fillna(0)
        balances[p] = balances[p]['balance']

    # Multiply the DataFrames in prod_values by the Series in balances
    for p in prod_values:
        prod_values[p] = prod_values[p].drop('volume', axis='columns')
        prod_values[p] = prod_values[p].multiply(balances[p], axis=0)

    # Add all the DataFrames together
    res = prod_values[list(prods)[0]]
    for p in list(prods)[1:]:
        res = res.add(prod_values[p])

    # Reindex the principal series
    prin = prin.reindex(idx, method='pad')

    return prin, res


def store_price_data(data, product, filename='prices.h5'):
    assert isinstance(data, pd.DataFrame), 'data is not a dataframe: {}'.format(type(data))
    assert isinstance(product, str), 'invalid product: {}'.format(type(product))
    assert isinstance(filename, str), 'invalid filename : {}'.format(type(filename))
    print('storing {} in {}'.format(product, filename))
    loaded = load_price_data(product, filename)
    if loaded is not None:
        data = pd.concat([data, loaded],
                         join='inner',
                         axis=0)
        data = data.sort_index()
    p = Path(os.getcwd()) / filename
    product = product.replace('-', '')
    data = data[~data.index.duplicated(keep='first')]
    data.to_hdf(p, product)


def load_price_data(product, filename='prices.h5'):
    assert isinstance(product, str), 'invalid product: {}'.format(type(product))
    assert isinstance(filename, str), 'invalid filename : {}'.format(type(filename))
    try:
        df = pd.read_hdf(filename, product.replace('-', ''))
        return df
    except KeyError as e:
        print('{} not found'.format(product))
        pass
    except FileNotFoundError as e:
        print('{} not found'.format(filename))
        pass


if __name__ == '__main__':
    def main():
        ac = get_auth_client()
        # print(ac.get_time())
        # prod = 'BTC-USD'
        # df = get_portfolio_history(ac)
        # if df is not None:
        #     print('Total data:')
        #     print(df.info())

        res = get_portfolio_history(ac)
        print(res)
    main()
