import logging
import time
from jobs import *
from config import Config  # Assuming Config class is defined in config.py
from XML_parse import XMLParser  # Importing the XMLParser class
from database import Database  # Assuming Database class is defined in database.py

# Configure logging
logging.basicConfig(
    filename='database_operations.log',
    level=logging.DEBUG,  # Changed to DEBUG to capture all messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'  # Ensure the file is overwritten each time for clean logs
)

def log_execution_time(job_name, start_time):
    end_time = time.time()
    execution_time = end_time - start_time
    logging.info(f"Execution time for {job_name}: {execution_time:.2f} seconds")

def main():
    config_file = "config.yaml"
    config = Config(config_file)

    # Retrieve JDBC parameters and create a Database instance
    jdbc_params = config.get_jdbc_parameters()
    # logging.debug(f"JDBC Parameters: {jdbc_params}")

    db = Database(jdbc_params)
    db.set_jdbc_parameters(jdbc_params)
    db.connect_JDBC()

    # Get the execution date
    execution_date_query = config.get_param('queries', 'TRANSVERSE_QUERY_LASTEXECUTIONDATE')
    execution_date = db.get_execution_date(execution_date_query)
    logging.info(f"Execution Date: {execution_date}")

    # Execute LOCAL_TO_DBBRUT_QUERY
    local_to_dbbrut_query = config.get_param('queries', 'LOCAL_TO_DBBRUT_QUERY')
    logging.info(f"Executing query: {local_to_dbbrut_query}")
    local_to_dbbrut_query_results = db.execute_query(local_to_dbbrut_query)
    # logging.debug(f"local_to_dbbrut_query_results: {local_to_dbbrut_query_results}")

    items_directory = config.get_param('Directories', 'items_directory')
    xml_parser = XMLParser()
    parsed_files_data = xml_parser.loop_parse_items(items_directory)
    # logging.debug(f"Parsed Files Data: {parsed_files_data}")

    # Job execution without a loop
    start_time = time.time()
    logging.info("Starting AUD_301_ALIMELEMENTNODE...")
    AUD_301_ALIMELEMENTNODE(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_301_ALIMELEMENTNODE", start_time)

    start_time = time.time()
    logging.info("Starting AUD_302_ALIMCONTEXTJOB...")
    AUD_302_ALIMCONTEXTJOB(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_302_ALIMCONTEXTJOB", start_time)

    start_time = time.time()
    logging.info("Starting AUD_303_ALIMNODE...")
    AUD_303_ALIMNODE(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_303_ALIMNODE", start_time)

    start_time = time.time()
    logging.info("Starting AUD_303_BIGDATA_PARAMETERS...")
    AUD_303_BIGDATA_PARAMETERS(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_303_BIGDATA_PARAMETERS", start_time)


    start_time = time.time()
    logging.info("Starting AUD_305_ALIMVARTABLE_XML...")
    AUD_305_ALIMVARTABLE_XML(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_305_ALIMVARTABLE_XML", start_time)

    start_time = time.time()
    logging.info("Starting AUD_305_ALIMVARTABLE...")
    AUD_305_ALIMVARTABLE(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_305_ALIMVARTABLE", start_time)

    start_time = time.time()
    logging.info("Starting AUD_306_ALIMOUTPUTTABLE...")
    AUD_306_ALIMOUTPUTTABLE(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_306_ALIMOUTPUTTABLE", start_time)

    #i need to update it

    # start_time = time.time()
    # logging.info("Starting AUD_307_ALIMINPUTTABLE_XML...")
    # AUD_307_ALIMINPUTTABLE_XML(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    # log_execution_time("AUD_307_ALIMINPUTTABLE_XML", start_time)


    start_time = time.time()
    logging.info("Starting AUD_307_ALIMINPUTTABLE...")
    AUD_307_ALIMINPUTTABLE(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_307_ALIMINPUTTABLE", start_time)

    start_time = time.time()
    logging.info("Starting AUD_308_ALIMCONNECTIONCOMPONENT...")
    AUD_308_ALIMCONNECTIONCOMPONENT(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_308_ALIMCONNECTIONCOMPONENT", start_time)

    start_time = time.time()
    logging.info("Starting AUD_309_ALIMELEMENTPARAMETER...")
    AUD_309_ALIMELEMENTPARAMETER(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_309_ALIMELEMENTPARAMETER", start_time)

    start_time = time.time()
    logging.info("Starting AUD_309_ALIMROUTINES...")
    AUD_309_ALIMROUTINES(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_309_ALIMROUTINES", start_time)

    start_time = time.time()
    logging.info("Starting AUD_310_ALIMLIBRARY...")
    AUD_310_ALIMLIBRARY(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_310_ALIMLIBRARY", start_time)

    start_time = time.time()
    logging.info("Starting AUD_311_ALIMELEMENTVALUENODE...")
    AUD_311_ALIMELEMENTVALUENODE(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_311_ALIMELEMENTVALUENODE", start_time)

    start_time = time.time()
    logging.info("Starting AUD_312_ALIMJOBFILS...")
    AUD_312_ALIMJOBFILS(config, db, parsed_files_data, execution_date)
    log_execution_time("AUD_312_ALIMJOBFILS", start_time)

    start_time = time.time()
    logging.info("Starting AUD_314_ALIMSUBJOBS_OPT...")
    AUD_314_ALIMSUBJOBS_OPT(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_314_ALIMSUBJOBS_OPT", start_time)

    start_time = time.time()
    logging.info("Starting AUD_315_DELETEINACTIFNODES...")
    AUD_315_DELETEINACTIFNODES(config, db, parsed_files_data)
    log_execution_time("AUD_315_DELETEINACTIFNODES", start_time)

    # start_time = time.time()
    # logging.info("Starting AUD_317_ALIMJOBSERVERPROPRETY...")
    # AUD_317_ALIMJOBSERVERPROPRETY(config, db, parsed_files_data, items_directory)
    # log_execution_time("AUD_317_ALIMJOBSERVERPROPRETY", start_time)

    # start_time = time.time()
    # logging.info("Starting AUD_318_ALIMCONFQUARTZ...")
    # AUD_318_ALIMCONFQUARTZ(config, db, parsed_files_data, items_directory)
    # log_execution_time("AUD_318_ALIMCONFQUARTZ", start_time)

    parsed_files_properties = xml_parser.loop_parse_properties(items_directory)
    # logging.debug(f"Parsed Files Data: {parsed_files_properties}")
    start_time = time.time()
    logging.info("Starting AUD_319_ALIMDOCCONTEXTGROUP...")
    AUD_319_ALIMDOCCONTEXTGROUP(config, db, parsed_files_properties)
    log_execution_time("AUD_319_ALIMDOCCONTEXTGROUP", start_time)

    start_time = time.time()
    logging.info("Starting AUD_320_ALIMDOCJOBS...")
    AUD_320_ALIMDOCJOBS(config, db, parsed_files_properties, local_to_dbbrut_query_results)
    log_execution_time("AUD_320_ALIMDOCJOBS", start_time)

    start_time = time.time()
    logging.info("Starting AUD_323_ALIMELEMENTNODEFILTER...")
    AUD_323_ALIMELEMENTNODEFILTER(config, db, parsed_files_data)
    log_execution_time("AUD_323_ALIMELEMENTNODEFILTER", start_time)

    start_time = time.time()
    logging.info("Starting AUD_324_ALIMMETADATAFILTER...")
    AUD_324_ALIMMETADATAFILTER(config, db, parsed_files_data)
    log_execution_time("AUD_324_ALIMMETADATAFILTER", start_time)

    # Step 3: Parse screenshot files from the directory
    screenshots_directory = config.get_param('Directories', 'screenshots_directory')
    # Assuming the `loop_parse_screenshots` method parses all screenshot XMLs in the directory
    parsed_files_items = xml_parser.loop_parse_screenshots(screenshots_directory)
    start_time = time.time()
    logging.info("Starting AUD_701_CONVERTSCREENSHOT...")
    AUD_701_CONVERTSCREENSHOT(config, db, parsed_files_items,execution_date,local_to_dbbrut_query_results)
    log_execution_time("AUD_701_CONVERTSCREENSHOT", start_time)

    start_time = time.time()
    logging.info("Starting AUD_304_ALIMMETADATA...")
    AUD_304_ALIMMETADATA(config, db, parsed_files_data, execution_date, local_to_dbbrut_query_results)
    log_execution_time("AUD_304_ALIMMETADATA", start_time)


    # Optionally, you can add a final log or print statement indicating that all jobs have finished.
    logging.info("All jobs have been executed.")

if __name__ == "__main__":
    main()
