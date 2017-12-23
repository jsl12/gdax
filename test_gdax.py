import gdax
from api_key import *

auth_client = gdax.AuthenticatedClient(KEY,
                                       B64SECRET,
                                       PASSPHRASE)

accounts = auth_client.get_accounts()

print(accounts)