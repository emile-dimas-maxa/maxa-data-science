import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def get_connector():
    snowflake_credentials = {
        'user': os.environ['SNOWFLAKE_USER'],
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ.get('SNOWFLAKE_SCHEMA', None),
    }

    if os.environ.get('SNOWFLAKE_PASSWORD'):
        snowflake_credentials['password'] = os.environ['SNOWFLAKE_PASSWORD']
    else:
        snowflake_credentials['authenticator'] = 'externalbrowser'
    return snowflake.connector.connect(**snowflake_credentials)
