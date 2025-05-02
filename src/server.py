"""
    Graph DB evaluation FastAPI server.

    powen@renci.org, 2025-25-04
"""
import os
import kuzu
from neo4j import GraphDatabase
import pandas as pd

from codetiming import Timer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from kuzu import Database
from contextlib import asynccontextmanager
from src.common.logger import LoggingUtil


# set the app version
app_version = os.getenv('APP_VERSION', 'Version number not set')

# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("graph-db-eval-fastapi", level=log_level, line_format='medium', log_file_path=log_path)

# create a placeholder for the DB load
db: kuzu.database.Database = Database()


@asynccontextmanager
async def lifespan(APP: FastAPI):
    """
    method to load the Kuzu DB once on startup

    :param APP:
    :return:
    """
    logger.info("Now loading Kuzu DB...")

    # get the path to the DB
    db_path: str = os.getenv('KUZU_DB_PATH', os.path.dirname(__file__))

    # grab the db variable created above
    global db

    # load the DB
    db = kuzu.Database(str(db_path))

    logger.info("Kuzu DB loaded.")

    # wait for shutdown
    yield

    # release resources
    db = None

# declare the FastAPI details
APP = FastAPI(title='Graph DB evaluation', version=app_version, lifespan=lifespan)

# declare app access details
APP.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@APP.get('/run_kuzu_cypher_query', status_code=200, response_model=None)
async def run_kuzu_cypher_query(query: str) -> PlainTextResponse:
    """
    Executes a CYPHER command on a Kuzu DB and returns the result.
    """
    # init the returned HTML status code
    status_code = 200

    # init the returned data
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
        logger.info("Result: %s", ret_val)

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
    logger.info("\nQuery: %s", query)

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


@APP.get('/run_mg_cypher_query', status_code=200, response_model=None)
async def run_mg_cypher_query(query: str) -> PlainTextResponse:
    """
    Executes a CYPHER command on a MemGraph DB and returns the result.
    """
    # init the returned HTML status code
    status_code = 200

    # init the intermediate and return values
    ret_val: str = ""

    # start collecting data
    try:
        # get the host name:port and auth (none)
        host_name = f"bolt://{os.getenv('MEMGRAPH_DB_HOST', 'localhost')}:{os.getenv('MEMGRAPH_DB_PORT', '7687')}"
        auth = ("", "")

        # Connect to Memgraph
        with GraphDatabase.driver(host_name, auth=auth) as client:
            # check the connection
            client.verify_connectivity()

            # get the data
            ret_val: str = await get_mg_data(client, query)

    except Exception as e:
        # return a failure message
        ret_val: str = f'Exception: Request failure. {str(e)}'

        logger.exception('Exception: Request failure.', e)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return PlainTextResponse(content=ret_val, status_code=status_code, media_type="text/plain")


async def get_mg_data(client, query: str) -> str:
    """
    gets the Memgraph data results using the CYPHER query passed.

    :param client:
    :param query:
    :return:
    """
    logger.info("\nMemGraph query: %s", query)

    # create a timer for query duration
    t = Timer(name="results", text="Results gathered in {:.4f}s")

    # use the timer
    with t:
        records, summary, keys = client.execute_query(query)

    # get the query result into a dataframe
    result = pd.DataFrame.from_records(records, columns=keys)

    # return the result
    return "Elapsed time: " + str(round(t.last, 4)) + "s\n" + result.to_string()
