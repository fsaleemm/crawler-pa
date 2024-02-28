import logging
import azure.functions as func

import azure.functions as func
import logging


from crawler.Orchestrator import Orchestrator

import uuid, base64, os, time, requests, json
from datetime import datetime

from azure.cosmos import CosmosClient, PartitionKey


app = func.FunctionApp()

schedule = os.getenv('SCHEDULE', '0 0 * * *')

@app.schedule(schedule=schedule, arg_name="myTimer", run_on_startup=False, use_monitor=True) 
def webcrawler(myTimer: func.TimerRequest) -> None:
    
    cosmos_logger = os.environ.get("Use_COSMOS_Logger", "true")
    cosmos_logger_bool = cosmos_logger.lower() in ['true', '1']

    # Generate a unique run ID
    run_id = str(uuid.uuid4())

    logger = get_cosmosdb_logger(run_id) if cosmos_logger_bool else logging.getLogger()

    if myTimer.past_due:
        logger.info('The timer is past due!')

    start_time = time.time()
    formatted_start_time = datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Starting crawler: {formatted_start_time}")

    # Do Something
    orchestrator = Orchestrator(logger)
    orchestrator.orchestrate()

    end_time = time.time()
    formatted_end_time = datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Ending crawler: {formatted_end_time}")

    elapsed_time = end_time - start_time
    logger.info("Elapsed time: {:.2f} seconds".format(elapsed_time))

# Custom Logging Handler
class CosmosDBHandler(logging.Handler):
    def __init__(self, cosmos_url, cosmos_key, database_name, container_name, run_id):
        super().__init__()
        self.client = CosmosClient(cosmos_url, credential=cosmos_key)
        self.database = self.client.create_database_if_not_exists(database_name)
        self.container = self.database.create_container_if_not_exists(
            id=container_name,
            partition_key=PartitionKey(path="/level"),
        )
        self.formatter = logging.Formatter()
        self.run_id = run_id

    def emit(self, record):
        record.asctime = self.formatter.formatTime(record)
        log_entry = {
            "id": str(uuid.uuid4()),
            "time": record.asctime,
            "run_id": self.run_id,
            "name": record.name,
            "level": record.levelname,
            "message": record.getMessage(),  # This will format the message
        }
        self.container.upsert_item(body=log_entry)

def get_cosmosdb_logger(run_id):
    # Create a logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Create a CosmosDB handler
    cosmos_handler = CosmosDBHandler(
        cosmos_url = os.environ.get("COSMOS_URL"),
        cosmos_key = os.environ.get("COSMOS_DB_KEY"),
        database_name = os.environ.get("COSMOS_DATABASE_NAME", "CrawlStore"),
        container_name = "CrawlerLog",
        run_id = run_id
    )

    # Use JSON formatter
    formatter = logging.Formatter('%(message)s')
    cosmos_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(cosmos_handler)

    return logger

