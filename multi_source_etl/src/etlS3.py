#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from etlModule import EmailUtility
from etlModule import ETLProcess

from datetime import datetime
import logging
import os


logging.basicConfig(level=logging.INFO)

def main():
    start_time = datetime.now()
    etl = ETLProcess('e:\\ETLsolutions\\config_s3.ini')

    try:
        download_path = etl.config['ETL']['download_path']
        etl.empty_folder_of_zip_csv(download_path)

        archive_path = etl.config['ETL']['archive_path']
        table_name = etl.config['MSSQL']['table_name']

        etl.download_from_s3(
            etl.config['S3_SOURCE']['s3_bucket'],
            etl.config['S3_SOURCE']['s3_folder'],
            download_path
        )

        for filename in os.listdir(download_path):
            target_file = os.path.join(download_path, filename)
            etl.process_file(target_file, archive_path, table_name)
            etl.empty_folder_of_zip_csv(download_path)

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

