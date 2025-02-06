#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from etlModule import EmailUtility
from etlModule import ETLProcess

from datetime import datetime
import logging
import os
import requests

logging.basicConfig(level=logging.INFO)

def main():
    start_time = datetime.now()
    etl = ETLProcess('e:\\ETLsolutions\\config_url.ini')

    try:
        etl.process_url()
        execution_time = (datetime.now() - start_time).total_seconds()
        etl.email_util.send_email(
            "ETL Process Successful",
            f"The ETL process completed successfully in {execution_time} seconds."
        )
        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        etl.email_util.send_email(
            "ETL Process Failed",
            f"ETL process failed with error: {e}"
        )

if __name__ == "__main__":
    main()

