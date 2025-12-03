import os

URL_LOGIN = '/users/login'
URL_CURRENT = '/smarthisapi/current'
URL_SAMPLE = '/smarthisapi/historical/sampled'
URL_CALCULATED = '/smarthisapi/historical/calculated'

CA_CERT_PATH = 'acquisition/smarthis/client_crt/signing-ca.crt'
CLIENT_CERT_PATH = 'acquisition/smarthis/client_crt/client.crt'
CLIENT_KEY_PATH = 'acquisition/smarthis/client_crt/client.key'
PASSWORD_PATH = 'acquisition/smarthis/client_crt/password.txt'

MAXIMUM_LEN_GET_DATA = 30 * 24*60*60000
TIME_OUT_REQUEST = 60  # seconds
TAKE_QUALITY = [0, 1, 2]

CALCULATE_MODE = {
    0: 'Count',
    1: 'Total',
    2: 'Minimum',
    3: 'Maximum',
    4: 'Average',
    5: 'Total_Abs',
    6: 'Minimum_Abs',
    7: 'Maximum_Abs',
    8: 'Average_Abs',
    9: 'Subtraction_Abs',
}

QUERY_MODE = {
    0: 'Raw data',
    1: 'Sampled data',
}

INTERVAL = {
    '1m': 60000,
    '5m': 5*60000,
    '7.5m': 7.5*60000,
    '10m': 10*60000,
    '15m': 15*60000,
    '30m': 30*60000,
    '1h': 60*60000
}

HEADERS = {
    'Content-Type': 'application/json'
}

BATCH_SIZE = 10
MAX_WORKERS = 5


def read_file(file_path):
    if os.path.exists(file_path) is False:
        return None
    with open(file_path) as f:
        lines = f.readlines()
    return lines