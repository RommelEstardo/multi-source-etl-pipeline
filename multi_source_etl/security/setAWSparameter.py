import boto3

ssm = boto3.client('ssm', region_name='us-west-2')

ssm.put_parameter(
    Name='sftp_password',
    Value='',
    Type='SecureString',
    Overwrite=True
)

ssm.put_parameter(
    Name='smtp_password',
    Value='',
    Type='SecureString',
    Overwrite=True
)

ssm.put_parameter(
    Name='sql_password',
    Value='',
    Type='SecureString',
    Overwrite=True
)
