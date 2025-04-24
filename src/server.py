"""
    APSVIZ settings server.
"""

import os
import kuzu
from codetiming import Timer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

from src.common.logger import LoggingUtil

# set the app version
app_version = os.getenv('APP_VERSION', 'Version number not set')

# declare the FastAPI details
APP = FastAPI(title='Graph DB evaluation', version=app_version)

# declare app access details
APP.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("graph-db-eval-fastapi", level=log_level, line_format='medium', log_file_path=log_path)

# get the path to the DB
db_path: str = os.getenv('KUZU_DB_PATH', os.path.dirname(__file__))

# create a DB and a connection to it
db = kuzu.Database(db_path)
db_conn = kuzu.AsyncConnection(db)


@APP.get('/run_kuzu_cypher_query', status_code=200, response_model=None)
async def run_kuzu_cypher_query(query: str) -> PlainTextResponse:
    """
    Executes a CYPHER command and returns the result.
    """
    # init the returned HTML status code
    status_code = 200

    # init the intermediate and return values
    ret_val: list = []

    # start collecting data
    try:
        ret_val: str = await get_kuzu_data(db_conn, query)

        logger.debug("Result: %s", ret_val)

    except Exception:
        # return a failure message
        ret_val = [{'Error': 'Exception detected.'}]

        logger.exception('Exception: Request failure.')

        # set the status to a server error
        status_code = 500

    # return to the caller
    return PlainTextResponse(content=ret_val, status_code=status_code, media_type="text/plain")


async def get_kuzu_data(conn, query: str) -> str:
    """
    gets the Kuzu data results using the cypher query passed.

    :param conn:
    :param query:
    :return:
    """
    logger.debug("\nQuery :\n %s", query)

    t = Timer(name="results", text="Results gathered in {:.4f}s")

    with t:
        response = await conn.execute(query)

    result = response.get_as_df()

    return "Elapsed time: " + str(round(t.last, 4)) + "s\n" + result.to_string()
