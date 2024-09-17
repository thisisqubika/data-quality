import os
from dotenv import load_dotenv
import base64
load_dotenv()

snowflake_conn_prop_local = {
   "account": os.environ.get('account'),
   "user": os.environ.get('user'),
   "password": os.environ.get('password'),
#  "password":  base64.b64decode(os.environ.get('password')).decode("utf-8"),
#  "passowrd": bytes.fromhex(os.environ.get('password')).decode(),
   "database": os.environ.get('database'),
   "schema": os.environ.get('schema'),
   "warehouse": os.environ.get('warehouse'),
   "role": os.environ.get('role'),
}