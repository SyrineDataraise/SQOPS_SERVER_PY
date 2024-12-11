def AUD_701_CONVERTSCREENSHOT(config: Config, db: Database, parsed_files_data: List[Tuple[str, str, dict]],execution_date : str,local_to_dbbrut_query_results:tuple,batch_size=100 ):

    """
    Perform various database operations including retrieving execution dates,
    executing queries, deleting records, and inserting parsed XML data.

    Args:
        config (Config): Configuration instance for retrieving parameters.
        db (Database): Database instance for executing queries.
        parsed_files_data (List[Tuple[str, str, dict]]): List of parsed data from XML files.
        execution_date (str): The execution date to use in data insertion.
        local_to_dbbrut_query_results (tuple): Results from the local to DB brut query.
        batch_size (int): Number of rows to insert in each batch.
    """
    try:
        # Step 3: Delete output from aud_screenshot based on query results
        aud_screenshot_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in local_to_dbbrut_query_results
        ]
        if aud_screenshot_conditions_batch:
            db.delete_records_batch('aud_screenshot', aud_screenshot_conditions_batch)

        # Step 4: Execute aud_screenshot query
        aud_screenshot_query = config.get_param('queries', 'aud_screenshot')
        logging.info(f"Executing query: {aud_screenshot_query}")
        aud_screenshot_results = db.execute_query(aud_screenshot_query)
        #logging.debug(f"aud_screenshot_results: {aud_screenshot_results}")

        # Step 5: Delete output from aud_contextjob based on query results
        aud_contextjob_conditions_batch = [
            {'NameProject': result[0], 'NameJob': result[1]}
            for result in aud_screenshot_results
        ]
        if aud_contextjob_conditions_batch:
            db.delete_records_batch('aud_contextjob', aud_contextjob_conditions_batch)

        # Step 6: Prepare batches for insertion into aud_screenshot and aud_screenshot_elementvalue tables
        aud_screenshot_batch = []
        aud_screenshot_elementvalue_batch = []

        for project_name, job_name, parsed_data in parsed_files_data:
            # Prepare aud_screenshot batch
            for param_data in parsed_data['parameters']:
                aud_screenshot_batch.append((
                    param_data['field'],
                    param_data['name'],
                    param_data['show'],
                    param_data['value'],
                    project_name,
                    job_name,
                    execution_date
                ))

                # Prepare aud_screenshot_elementvalue batch
                for elementValue in param_data['elementValues']:
                    aud_screenshot_elementvalue_batch.append((
                        elementValue['elementRef'],
                        elementValue['value'],
                        param_data['name'],
                        project_name,
                        job_name,
                        execution_date
                    ))

            # Step 7: Insert data in batches
            if len(aud_screenshot_batch) == batch_size:
                insert_query = config.get_param('insert_queries', 'aud_screenshot')
                db.insert_data_batch(insert_query, 'aud_screenshot', aud_screenshot_batch)
                aud_screenshot_batch.clear()  # Clear the batch after insertion

        # Insert remaining data in the batch
        if aud_screenshot_batch:
            insert_query = config.get_param('insert_queries', 'aud_screenshot')
            db.insert_data_batch(insert_query, 'aud_screenshot', aud_screenshot_batch)
            logging.info(f"Inserted remaining batch of data into aud_screenshot: {len(aud_screenshot_batch)} rows")

        # Insert data for aud_screenshot_elementvalue
        if len(aud_screenshot_elementvalue_batch) >= batch_size:
            insert_query = config.get_param('insert_queries', 'aud_screenshot_elementvalue')
            db.insert_data_batch(insert_query, 'aud_screenshot_elementvalue', aud_screenshot_elementvalue_batch)
            aud_screenshot_elementvalue_batch.clear()  # Clear the batch after insertion

        # Insert remaining data in the batch for aud_screenshot_elementvalue
        if aud_screenshot_elementvalue_batch:
            insert_query = config.get_param('insert_queries', 'aud_screenshot_elementvalue')
            db.insert_data_batch(insert_query, 'aud_screenshot_elementvalue', aud_screenshot_elementvalue_batch)
            logging.info(f"Inserted remaining batch of data into aud_screenshot_elementvalue: {len(aud_screenshot_elementvalue_batch)} rows")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        if db:
            logging.info("Done!")
            # Uncomment to close the database connection if needed
            # db.close()