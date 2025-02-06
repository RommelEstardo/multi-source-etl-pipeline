#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from etlModule import EmailUtility
from etlModule import ETLProcess

from datetime import datetime
import logging
import os
import boto3


logging.basicConfig(level=logging.INFO)

def main():
    start_time = datetime.now()
    etl = ETLProcess('e:\\ETLsolutions\\config_sftp.ini')
    ssm = boto3.client('ssm', region_name='us-west-2')
    sftp_password = ssm.get_parameter(Name='sftp_password', WithDecryption=True)['Parameter']['Value']

    try:
        download_path = etl.config['ETL']['download_path']
        etl.empty_folder_of_zip_csv(download_path)

        archive_path = etl.config['ETL']['archive_path']
        table_name = etl.config['MSSQL']['table_name']

        etl.download_from_sftp(
            etl.config['SFTP_SOURCE']['host'],
            etl.config['SFTP_SOURCE']['port'],
            etl.config['SFTP_SOURCE']['username'],
            sftp_password,
            etl.config['SFTP_SOURCE']['remote_path'],
            download_path
        )

        for filename in os.listdir(download_path):
            target_file = os.path.join(download_path, filename)
            etl.process_file(target_file, archive_path, table_name)

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


