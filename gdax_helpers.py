import pandas as pd
import gdax
from datetime import datetime, timedelta
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


def get_value_df(df):
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


def get_value_history(client, product, start, end=None, gran=None):
    values = []

    if end is None:
        end = datetime.now()

    if gran is None or not isinstance(gran, timedelta):
        gran = timedelta(days=1)

    gran = int(gran.total_seconds())
    n = (end - start).total_seconds() / gran

    if n > 200:
        overflow = n % 200
        n -= overflow
        num_reqs = int(n / 200)

        dt = timedelta(seconds=gran)
        # print(num_reqs, dt)

        temp_end = start + (200 * dt)
        for i in range(num_reqs):
            # print(start.strftime('%Y-%m-%d %H:%M:%S'))
            # print(temp_end.strftime('%Y-%m-%d %H:%M:%S'))
            values.extend(client.get_product_historic_rates(product,
                                                            granularity=gran,
                                                            start=start.isoformat(),
                                                            end=temp_end.isoformat()))
            start = temp_end
            temp_end += (200 * dt)
        temp_end = end
        values.extend(client.get_product_historic_rates(product,
                                                        granularity=gran,
                                                        start=start.isoformat(),
                                                        end=temp_end.isoformat()))
    else:
        values = client.get_product_historic_rates(product,
                                                   granularity=gran,
                                                   start=start.isoformat(),
                                                   end=end.isoformat())

    if values:
        df = pd.DataFrame(values)
        df.columns = ['time', 'low', 'high', 'open', 'close', 'volume']
        df['time'] = df['time'].apply(datetime.fromtimestamp)
        df = df.set_index('time')
        return df.sort_index(axis=0)


def get_performance_history(client, holding_row):
    df = get_value_history(client, holding_row.product_id, holding_row.name[1], 200)
    df = df * holding_row.amount
    return df


if __name__ == '__main__':
    def main():
        ac = get_auth_client()
        print(get_value_history(ac, 'BTC-USD', datetime.now() - timedelta(hours=3), gran=timedelta(seconds=60)))
        print(ac.get_time())
    main()