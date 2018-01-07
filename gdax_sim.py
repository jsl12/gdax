from datetime import datetime
import pandas as pd


def sim_buy(history, t_type, **kwargs):
    '''
    Returns a DataFrame containing the transactions
    Intended to be appended onto an account histories DataFrame

    Required arguments:
    history - DataFrame of transaction history
    t_type - type of transaction, in the format of 'AAA-BBB'
    date - Timestamp of transaction
    amount - amount of currency
    payment - amount of payment
    '''

    res = {}
    try:
        res['type'] = 'simulated buy'
        assert isinstance(kwargs['date'], datetime)
        res['created_at'] = kwargs['date']
        res['product_id'] = t_type
        res['amount'] = kwargs['amount']
        res['payment'] = kwargs['payment']

        # Gets the currency type to use for the multi-level index
        ind = t_type.split('-')[0]

        # Gets the previous balance from the history
        bal = history.loc[ind].sort_index().iloc[-1]['balance']
        res['balance'] = bal + res['amount']

        res = {key: [res[key]] for key in res}
        res = pd.DataFrame(res).set_index([[ind], 'created_at'])
        res = pd.concat([history, res]).sort_index()
        return res
    except KeyError as e:
        print(e)
        return