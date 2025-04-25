"""
    APSVIZ settings server.
"""

import os
import kuzu
from codetiming import Timer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from kuzu import Database
from contextlib import asynccontextmanager
from src.common.logger import LoggingUtil

# set the app version
app_version = os.getenv('APP_VERSION', 'Version number not set')

# create a placeholder for the DB load
db: kuzu.database.Database = Database()


@asynccontextmanager
async def lifespan(APP: FastAPI):
    """
    method to load the Kuzu DB once on startup

    :param APP:
    :return:
    """
    print("Loading DB...")

    # get the path to the DB
    db_path: str = os.getenv('KUZU_DB_PATH', os.path.dirname(__file__))

    # grab the db variable created above
    global db

    # load the DB
    db = kuzu.Database(str(db_path))

    # wait for shutdown
    yield

    # release resources
    db = None

# declare the FastAPI details
APP = FastAPI(title='Graph DB evaluation', version=app_version, lifespan=lifespan)

# declare app access details
APP.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("graph-db-eval-fastapi", level=log_level, line_format='medium', log_file_path=log_path)


@APP.get('/run_kuzu_cypher_query', status_code=200, response_model=None)
async def run_kuzu_cypher_query(query: str) -> PlainTextResponse:
    """
    Executes a CYPHER command and returns the result.
    """
    # init the returned HTML status code
    status_code = 200

    # init the intermediate and return values
    ret_val: str = ""

    # start collecting data
    try:
        # grab the global variable above
        global db

        # create a DB and a connection to it
        db_conn = kuzu.AsyncConnection(db)

        # submit the query and get some data
        ret_val: str = await get_kuzu_data(db_conn, query)

        # log the result
        logger.debug("Result: %s", ret_val)

    except Exception as e:
        # return a failure message
        ret_val: str = f'Exception: Request failure. {str(e)}'

        logger.exception('Exception: Request failure.', e)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return PlainTextResponse(content=ret_val, status_code=status_code, media_type="text/plain")


async def get_kuzu_data(conn, query: str) -> str:
    """
    gets the Kuzu data results using the CYPHER query passed.

    :param conn:
    :param query:
    :return:
    """
    logger.debug("\nQuery: %s", query)

    # create a timer for query duration
    t = Timer(name="results", text="Results gathered in {:.4f}s")

    # use the timer
    with t:
        # make the call to execute the query
        response = await conn.execute(query)

    # get the query result
    result = response.get_as_df()

    # return the result
    return "Elapsed time: " + str(round(t.last, 4)) + "s" + result.to_string()
