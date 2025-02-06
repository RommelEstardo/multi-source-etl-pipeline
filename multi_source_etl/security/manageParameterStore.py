import boto3
import os
import random
import string

ssm = boto3.client('ssm', region_name='us-west-2')

def fetch_and_save_parameters(prefix, folder_path, file_name):
    paginator = ssm.get_paginator('describe_parameters')
    page_iterator = paginator.paginate(
        ParameterFilters=[{
            'Key': 'Name',
            'Option': 'BeginsWith',
            'Values': [prefix]
        }]
    )
    full_path = os.path.join(folder_path, file_name)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, 'w') as file:
        for page in page_iterator:
            names = [param['Name'] for param in page['Parameters']]
            if names:
                values_response = ssm.get_parameters(Names=names, WithDecryption=True)
                for param in values_response['Parameters']:
                    file.write(f"{param['Name']} = {param['Value']} (Type: {param['Type']})\n")

def process_parameters_from_file(folder_path, file_name):
    full_path = os.path.join(folder_path, file_name)
    with open(full_path, 'r') as file:
        for line in file:
            if line.strip().startswith('#'):
                continue
            line = line.strip()
            if line:
                name_value, type_part = line.rsplit(' (Type: ', 1)
                name, value = name_value.split('=', 1)
                type_name = type_part.rstrip(')')
                if type_name == 'DELETE':
                    if parameter_exists(name):
                        try:
                            ssm.delete_parameter(Name=name)
                            print(f"Deleted parameter: {name}")
                        except Exception as e:
                            print(f"Error deleting parameter {name}: {str(e)}")
                    else:
                        print(f"Parameter {name} does not exist. No deletion needed.")
                elif type_name == 'EDIT':
                    if parameter_exists(name):
                        try:
                            original_parameter = ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']
                            original_type = original_parameter['Type']
                            ssm.put_parameter(
                                Name=name,
                                Value=value.strip(),
                                Type=original_type,
                                Overwrite=True
                            )
                            print(f"Updated value for parameter: {name}, keeping original type: {original_type}")
                        except Exception as e:
                            print(f"Error updating parameter {name}: {str(e)}")
                else:
                    if not parameter_exists(name):
                        try:
                            ssm.put_parameter(
                                Name=name,
                                Value=value,
                                Type=type_name,
                                Overwrite=False
                            )
                            print(f"Added new parameter: {name} with type: {type_name}")
                        except Exception as e:
                            print(f"Failed to add {name}: {str(e)}")
                    else:
                        print(f"Parameter {name} already exists. No action performed.")

def parameter_exists(name):
    try:
        ssm.get_parameter(Name=name)
        return True
    except ssm.exceptions.ParameterNotFound:
        return False
    except Exception as e:
        print(f"Error checking parameter existence for {name}: {str(e)}")
        return False

def manage_parameters(operation, prefix=None, folder_path=None, file_name=None):
    if operation == 'fetch':
        fetch_and_save_parameters(prefix, folder_path, file_name)
    elif operation == 'process':
        process_parameters_from_file(folder_path, file_name)
    else:
        print("Invalid operation. Use 'fetch' to retrieve parameters or 'process' to update or delte them.")

def generate_password(length=12):
    characters = string.ascii_letters + string.digits + string.punctuation
    password = ''.join(random.choice(characters) for i in range(length))
    return password

manage_parameters('process', folder_path='e:\\ETLsolutions\\parameterStore', file_name='parameters.txt')
manage_parameters('fetch', prefix='/', folder_path='e:\\ETLsolutions\\parameterStore', file_name='parameters.txt')
