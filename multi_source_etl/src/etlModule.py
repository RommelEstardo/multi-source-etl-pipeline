#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import configparser
import logging
import requests
import os
import glob
import zipfile
import subprocess
import pyodbc
import shutil
import csv
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import pandas as pd
import boto3
import paramiko
import json
from sqlalchemy import create_engine
import numpy as np

class EmailUtility:
    def __init__(self, email_config):
        ssm = boto3.client('ssm', region_name='us-west-2')
        self.smtp_password = ssm.get_parameter(Name='smtp_password', WithDecryption=True)['Parameter']['Value']
        self.server = email_config['smtp_server']
        self.port = email_config['smtp_port']
        self.user = email_config.get('user')
        self.recipient = email_config['recipient']

    def send_email(self, subject, body):
        msg = MIMEMultipart()
        msg['From'] = self.user if self.user else 'anonymous@example.com'
        msg['To'] = self.recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP(self.server, self.port)
        server.starttls()
        if self.user and self.smtp_password:
            server.login(self.user, self.smtp_password)
        server.sendmail(self.user if self.user else 'anonymous@example.com', self.recipient, msg.as_string())
        server.quit()
        logging.info("Email sent successfully")

class ETLProcess:
    def __init__(self, config_file):
        ssm = boto3.client('ssm', region_name='us-west-2')
        self.pwd = ssm.get_parameter(Name='sql_password', WithDecryption=True)['Parameter']['Value']
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.setup_logging()
        self.email_util = EmailUtility(self.config['EMAIL'])
        self.db_type = self.config['ETL']['database_type']
        self.dbServer = self.config['MSSQL']['server']
        self.dbName = self.config['MSSQL']['database']
        self.uid = self.config['MSSQL']['user']
        self.tableName = self.config['MSSQL']['table_name']
        self.drop_table_if_exists = self.config['MSSQL'].getboolean('drop_table_if_exists')
        delimiter = self.config['ETL']['field_delimiter']
        if delimiter == r'\t':
            self.field_delimiter = '\t'
        else:
            self.field_delimiter = delimiter
        self.file_name = self.config['ETL']['file_name']
        self.file_prefix = self.config['ETL']['file_prefix']
        self.file_suffix = self.config['ETL']['file_suffix']
        self.file_extensions = self.config['ETL']['file_extensions']
        self.file_type = self.config['ETL']['file_type']
        self.file_has_header = self.config['ETL'].getboolean('file_has_header')
        self.archive_path = self.config['ETL']['archive_path']
        bcp_end_of_row = self.config['ETL']['bcp_end_of_row']
        if bcp_end_of_row == r'\n':
            self.bcp_end_of_row = '"\\n"'
        else:
            self.bcp_end_of_row = bcp_end_of_row
        self.bcp_row_start = self.config['ETL']['bcp_row_start']
        self.bcp_batch_commit_size = self.config['ETL']['bcp_batch_commit_size']
        self.bcp_end_of_row = self.config['ETL']['bcp_end_of_row']
        self.bcp_import_bool = self.config['IMPORT_METHOD'].getboolean('bcp_import')
        self.bulkInsert_import_bool = self.config['IMPORT_METHOD'].getboolean('bulkInsert_import')
        self.pandas_import_bool = self.config['IMPORT_METHOD'].getboolean('pandas_import')

    def empty_folder_of_zip_csv(self, folder_path):
        file_patterns = ['*.zip', '*.csv']
        for pattern in file_patterns:
            full_pattern = os.path.join(folder_path, pattern)
            for filename in glob.glob(full_pattern):
                try:
                    os.remove(filename)
                    print(f"Removed {filename}")
                except OSError as e:
                    print(f"Error: {e.strerror}")
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')

    def setup_logging(self):
        logging.basicConfig(
            filename='etl_log.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def download_from_url(self, url, target_file):
        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(target_file, 'wb') as f:
                f.write(response.content)
            logging.info(f"File downloaded: {target_file}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading from URL {url}: {e}")

    def download_from_s3(self, s3_bucket, s3_folder, destination_folder):
        s3 = boto3.client('s3')
        objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_folder)
        files = [item['Key'] for item in objects['Contents'] if not item['Key'].endswith('/')]
        files_without_folders = []
        file_extensions = []
        for file in files:
            if not file.endswith('/'):
                file_name = file.split('/')[-1]
                files_without_folders.append(file_name)
                _, file_extension = os.path.splitext(file_name)
                file_extensions.append(file_extension)
        for file_name in files_without_folders:
            if file_name.startswith(self.file_prefix):
                _, file_extension = os.path.splitext(file_name)
                file_extension = file_extension[1:]
                if file_extension in self.file_extensions:
                    destination_path = os.path.join(destination_folder, file_name)
                    s3.download_file(s3_bucket, s3_folder+file_name, destination_path)
                    print(f"Copied s3://{s3_bucket}{s3_folder}{file_name} to {destination_path}")

    def download_from_sftp(self, host, port, username, password, remote_path, local_path):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(host, port, username, password)
            sftp = ssh.open_sftp()
            files = sftp.listdir(remote_path)
            extension_list = [ext.strip() for ext in self.file_extensions.split(',')]
            filtered_files = [
                f for f in files if f.startswith(self.file_prefix) or any(f.endswith(self.file_suffix + '.' + ext) for ext in extension_list)
            ]
            for file_name in filtered_files:
                remote_file_path = os.path.join(remote_path, file_name)
                local_file_path = os.path.join(local_path, file_name)
                sftp.get(remote_file_path, local_file_path)
                logging.info(f"Downloaded {file_name} to {local_path}")
                if zipfile.is_zipfile(local_file_path):
                    with zipfile.ZipFile(local_file_path, 'r') as zip_ref:
                        extract_path = os.path.join(local_path, os.path.splitext(file_name)[0])
                        zip_ref.extractall(extract_path)
                        logging.info(f"Extracted {file_name} to {extract_path}")
        except paramiko.SSHException as e:
            logging.error(f"Failed to download files from SFTP: {e}")
        finally:
            sftp.close()
            ssh.close()

    def extract_file_if_compressed(self, file_path):
        if os.path.splitext(file_path)[1] == '.zip':
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(file_path))
            logging.info(f"Extracted {file_path}")
            dest_file_path = os.path.join(self.archive_path, os.path.basename(file_path))
            if os.path.exists(dest_file_path):
                os.remove(dest_file_path)
            print(f"Moving {file_path} to the archive folder {self.archive_path} ...")
            shutil.move(file_path, self.archive_path)

    def bcp_import(self, file_path, tableName):
        delimiter = self.field_delimiter
        if delimiter == '\t':
            delimiter = '"\\t"'
        bcp_command = f"bcp {self.dbName}.dbo.{tableName}_View IN {file_path} -F {self.bcp_row_start} -c -b {self.bcp_batch_commit_size} -t{delimiter} -S {self.dbServer} -r {self.bcp_end_of_row} "
        if self.uid > '':
            bcp_command += f"-U {self.uid} -P {self.pwd}"
        else:
            bcp_command += "-T"
        try:
            subprocess.run(bcp_command, shell=True)
        except Exception as e:
            print(f'BCP import failed: {e}')
        else:
            print('BCP import succeeded')

    def bulkInsert_import(self, file_path, tableName):
        conn = self.connect_to_database()
        cursor = conn.cursor()
        sql = f"""
        BULK INSERT {tableName}
        FROM '{file_path}'
        WITH (
            FIELDTERMINATOR = '{self.field_delimiter}',
            ROWTERMINATOR = '{self.bcp_end_of_row}',
            FIRSTROW = {self.bcp_row_start},
            BATCHSIZE = {self.bcp_batch_commit_size}
        )
        """
        try:
            cursor.execute(sql)
            conn.commit()
        except pyodbc.Error as e:
            print(f'BULK INSERT failed: {e}')
        else:
            print('BULK INSERT succeeded')
        finally:
            cursor.close()
            conn.close()

    def pandas_import(self, file_path, tableName):
        try:
            delimiter = self.field_delimiter.replace('"', '')
            df = pd.read_csv(file_path, delimiter=delimiter, engine='python')
            def convert_values(val):
                if isinstance(val, str):
                    val = val.strip('"')
                    if val.startswith("(") and val.endswith(")"):
                        return -float(val[1:-1])
                    elif val == "-":
                        return None
                    elif val == "<NA>":
                        return None
                return val
            df = df.apply(lambda col: col.apply(convert_values)).convert_dtypes()
            df = df.astype(str)
            conn = self.connect_to_database()
            cursor = conn.cursor()
            cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}_View' ORDER BY ORDINAL_POSITION")
            columns = ', '.join([f'[{row[0]}]' for row in cursor.fetchall()])
            query = f"INSERT INTO {tableName}_View ({columns}) VALUES ({', '.join('?' * len(df.columns))})"
            data = [tuple(row) for row in df.values]
            cursor.executemany(query, data)
            num_rows = len(df)
            print(f'{num_rows} rows were imported.')
            conn.commit()
            logging.info(f"Pandas data from {file_path} inserted successfully.")
        except Exception as e:
            print(f'Pandas import failed: {e}')
        else:
            print('Pandas import succeeded')
        finally:
            cursor.close()
            conn.close()

    def connect_to_database(self):
        if self.db_type == 'mssql':
            conn_str = f'DRIVER={{SQL Server}};SERVER={self.dbServer};DATABASE={self.dbName};'
            if self.uid and self.pwd:
                conn_str += f'UID={self.uid};PWD={self.pwd}'
            else:
                conn_str += 'Trusted_Connection=yes;'
            return pyodbc.connect(conn_str)
        else:
            raise ValueError("Unsupported database type")

    def process_file(self, file_path, archive_path, tableName):
        file_type_handlers = {
            'txt': self.handle_csv,
            'csv': self.handle_csv,
            'json': self.handle_json,
        }
        try:
            print(f"Processing file: {file_path}")
            if file_path.endswith('.zip'):
                print(f"Extracting file: {file_path}")
                self.extract_file_if_compressed(file_path)
            directory_path = os.path.dirname(file_path)
            print(f"Processing files in directory: {directory_path}")
            for root, dirs, files in os.walk(directory_path):
                for file in files:
                    print(f"Processing file: {file}")
                    if file.endswith(self.file_suffix + '.' + self.file_type):
                        csv_file_path = os.path.join(root, file)
                        print(f"Valid file found: {csv_file_path}")
                        try:
                            handler = file_type_handlers[self.file_type]
                            print(f"Processing {self.file_type} file: {csv_file_path}")
                            handler(csv_file_path, tableName)
                            self.drop_table_if_exists = False
                            logging.info(f"Processed {self.file_type} file: {csv_file_path}")
                        except Exception as e:
                            logging.error(f"Error processing file {csv_file_path}: {str(e)}")
                            continue
                        try:
                            shutil.move(csv_file_path, os.path.join(archive_path, file))
                            logging.info(f"Moved {file} to the archive folder.")
                            self.drop_table_if_exists = False
                        except Exception as e:
                            logging.error(f"Error moving file {csv_file_path} to archive: {str(e)}")
                    else:
                        print(f"Invalid file found: {file}")
                        print(f"Expected file suffix: {self.file_suffix + '.' + self.file_type}")
        except Exception as e:
            logging.error(f"Error in process_file method: {str(e)}")

    def handle_csv(self, file_path, table_name):
        logging.info(f"Processing CSV file: {file_path}")
        try:
            delimiter = self.field_delimiter
            if self.file_has_header:
                self._create_table_from_csv_header(file_path, table_name)
            self._import_data(file_path, table_name)
        except Exception as e:
            logging.error(f"Error processing CSV file {file_path}: {e}")

    def _create_table_from_csv_header(self, file_path, table_name):
        with open(file_path, 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=self.field_delimiter, quoting=csv.QUOTE_MINIMAL)
            columns = next(reader)
            columns_sql = ['[' + column + '] varchar(max)' for column in columns]
        self.create_table_and_view(columns_sql, table_name)

    def _import_data(self, file_path, table_name):
        try:
            if self.bcp_import_bool:
                self.bcp_import(file_path, table_name)
            elif self.pandas_import_bool:
                self.pandas_import(file_path, table_name)
            else:
                logging.warning("No import method selected")
        except Exception as e:
            logging.error(f"Error importing data from {file_path} to {table_name}: {e}")

    def create_table_and_view(self, columns_sql, tableName):
        try:
            conn = self.connect_to_database()
            cursor = conn.cursor()
            if isinstance(columns_sql, str):
                columns_sql = columns_sql.split(', ')
            if self.drop_table_if_exists:
                drop_table_query = f"IF EXISTS (SELECT * FROM sys.tables WHERE name = N'{tableName}' AND type = 'U') DROP TABLE {tableName}"
                cursor.execute(drop_table_query)
                conn.commit()
                create_table_query = f"CREATE TABLE {tableName} (RecId INT PRIMARY KEY IDENTITY(1,1), {', '.join(columns_sql)})"
                cursor.execute(create_table_query)
                conn.commit()
                logging.info(f"Table {tableName} created or verified successfully.")
                cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{tableName}' ORDER BY ORDINAL_POSITION")
                columns_sql = [row[0] for row in cursor.fetchall()]
                drop_view_query = f"IF EXISTS (SELECT * FROM sys.views WHERE name = N'{tableName}_View') DROP VIEW {tableName}_View"
                cursor.execute(drop_view_query)
                conn.commit()
                columns_without_id = [f"[{column}]" for column in columns_sql[1:]]
                create_view_query = f"CREATE VIEW {tableName}_View AS SELECT {', '.join(columns_without_id)} FROM {tableName}"
                cursor.execute(create_view_query)
                conn.commit()
                logging.info(f"View {tableName}_View created successfully.")
        except Exception as e:
            logging.error(f"An error occurred while creating or verifying the table {tableName}: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def handle_json(self, file_path, tableName):
        try:
            conn = self.connect_to_database()
            cursor = conn.cursor()
            with open(file_path, 'r') as f:
                data = json.load(f)
            columns = data[0].keys()
            columns_sql = ', '.join(f"[{column}] NVARCHAR(MAX)" for column in columns)
            self.create_table_and_view(columns_sql, tableName)
            for item in data:
                columns_str = ', '.join(item.keys())
                values_str = ', '.join(f"'{value}'" for value in item.values())
                insert_query = f"INSERT INTO {tableName} ({columns_str}) VALUES ({values_str})"
                cursor.execute(insert_query)
            conn.commit()
            logging.info(f"JSON data from {file_path} inserted successfully.")
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {str(e)}")
            conn.rollback()
        finally:
            cursor.close()
            conn.close()

    def process_url(self):
        try:
            downloadPath = self.config['ETL']['download_path']
            archivePath = self.config['ETL']['archive_path']
            file_has_header = self.config['ETL'].getboolean('file_has_header')
            url_links = self.config['URL_SOURCE']['url_links'].splitlines()
            url_column_names = self.config['URL_SOURCE']['url_column_names'].splitlines()
            url_table_names = self.config['URL_SOURCE']['url_table_names'].splitlines()
            for url, column_names, table_name in zip(url_links, url_column_names, url_table_names):
                if not url.strip():
                    continue
                try:
                    self.empty_folder_of_zip_csv(downloadPath)
                    file_path = os.path.join(downloadPath, f'{url.split("/")[-1]}')
                    self.download_from_url(url, file_path)
                    if not file_has_header:
                        columns_sql = ', '.join(f"[{column}] NVARCHAR(MAX)" for column in column_names.split(',') if column.strip())
                        self.create_table_and_view(columns_sql, table_name)
                    print(f"Processing file: {file_path}")
                    self.process_file(file_path, archivePath, table_name)
                except requests.exceptions.MissingSchema:
                    logging.error(f"Invalid URL: {url}")
                    continue
        except Exception as e:
            logging.error(f"Error processing URL: {str(e)}")
