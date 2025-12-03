import json
import ssl
import logging
from http.client import HTTPSConnection, HTTPConnection
from ._header import *

logger = logging.getLogger(__name__)

def get_sampled(conn, token, data):
    try:
        conn.request("POST", URL_SAMPLE, json.dumps(data), get_headers(token))
        res = conn.getresponse()
        if res.status != 200:
            logger.warning(f"Sampled request returned status {res.status}: {res.reason}")
        return res
    except Exception as e:
        logger.error(f"Failed to get sampled data: {e}", exc_info=True)
        raise

def get_calculated(conn, token, data):
    try:
        conn.request("POST", URL_CALCULATED, json.dumps(data), get_headers(token))
        res = conn.getresponse()
        if res.status != 200:
            logger.warning(f"Calculated request returned status {res.status}: {res.reason}")
        return res
    except Exception as e:
        logger.error(f"Failed to get calculated data: {e}", exc_info=True)
        raise

def get_current(conn, token, point_name):
    try:
        data = {
            'point_names': [point_name],
            'expression_mode': False
        }
        conn.request("POST", URL_CURRENT, json.dumps(data), get_headers(token))
        res = conn.getresponse()
        if res.status != 200:
            logger.warning(f"Current request for point '{point_name}' returned status {res.status}: {res.reason}")
        return res
    except Exception as e:
        logger.error(f"Failed to get current data for point '{point_name}': {e}", exc_info=True)
        raise
# -------- CONNECT HIS ----------
def get_connection(address, port=None, timeout=TIME_OUT_REQUEST):
    is_https = 'https' in address
    if is_https:
        cert = load_cert()
        return load_conn_https(address, port, cert, timeout)
    else:
        return load_conn_http(address, port, timeout)


def load_cert():
    try:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ssl_context.load_verify_locations(cafile=CA_CERT_PATH)
        
        password_file = read_file(PASSWORD_PATH)
        if password_file is None or len(password_file) == 0:
            logger.error(f"Password file not found or empty: {PASSWORD_PATH}")
            raise FileNotFoundError(f"Password file not found: {PASSWORD_PATH}")
        
        ssl_context.load_cert_chain(
            certfile=CLIENT_CERT_PATH,
            keyfile=CLIENT_KEY_PATH,
            password=password_file[0].strip() if password_file[0] else None
        )
        return ssl_context
    except FileNotFoundError as e:
        logger.error(f"Certificate file not found: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Failed to load SSL certificate: {e}", exc_info=True)
        raise


def load_conn_https(address, port=None, cert=None, timeout=TIME_OUT_REQUEST):
    try:
        clean_address = address.replace("https://", "")
        conn_https = HTTPSConnection(
            clean_address, port,
            context=cert,
            timeout=timeout
        )
        return conn_https
    except Exception as e:
        logger.error(f"Failed to connect HTTPS to {address}: {e}", exc_info=True)
        return None


def load_conn_http(address, port=None, timeout=TIME_OUT_REQUEST):
    try:
        clean_address = address
        if address.startswith("http://"):
            clean_address = address[len("http://"):]

        conn_http = HTTPConnection(clean_address, port, timeout=timeout)
        return conn_http
    except Exception as e:
        logger.error(f"Failed to connect HTTP to {address}: {e}", exc_info=True)
        return None

def close_conn(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        logger.error(f"Error closing connection: {e}", exc_info=True)

def handle_res_sample(res_data):
    data = []
    if res_data.status == 200:
        try:
            response_json = json.loads(res_data.read())
            for record in response_json:
                if record.get('q') in TAKE_QUALITY:
                    data.append(record['v'])
                else:
                    data.append(float('nan'))
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON response: {e}", exc_info=True)
            return data
    else:
        logger.error(f"HTTP Error {res_data.status}: {res_data.reason}")
        return data


# ---------LOGIN & GET TOKEN ----------

def login_and_get_token(address, username, password):
    try:
        login_res = login_his(address, username, password)
        if login_res is None:
            logger.error(f"Failed to login to {address}: connection failed")
            return None
        
        if login_res.status != 200:
            logger.error(f"Login failed for {address}: status {login_res.status}, reason {login_res.reason}")
            return None
        
        try:
            token = json.loads(login_res.read())["token"]
            logger.warning(f"Successfully logged in to {address}")
            return token
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse login response from {address}: {e}", exc_info=True)
            return None
    except Exception as e:
        logger.error(f"Error in login_and_get_token for {address}: {e}", exc_info=True)
        return None


def login_his(address, username, password):
    try:
        conn = get_connection(address)
        if conn is None:
            logger.error(f"Failed to establish connection to {address} for login")
            return None
        
        data = {
            "username": username,
            "password": password
        }
        conn.request("POST", URL_LOGIN, json.dumps(data), get_headers())
        res = conn.getresponse()
        return res
    except Exception as e:
        logger.error(f"Error in login_his for {address}: {e}", exc_info=True)
        return None


def get_headers(token=None):
    headers = {}
    headers["Content-type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers

