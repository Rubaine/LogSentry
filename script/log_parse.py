import os
import pandas as pd
from loguru import logger
from glob import glob

"""
This script contains functions to parse access and error log files, extract relevant information, and export the parsed data to CSV files.
Functions:
    parse_access_log_file(log_file_path):
    parse_error_log(log_file_path):
    parse_all_files(access_logs_directory, error_logs_directory, output_csv_access_path, output_csv_error_path):
"""

def parse_access_log_file(log_file_path):
    """
    Parses an access log file and extracts relevant information into a pandas DataFrame.
    Args:
        log_file_path (str): The path to the access log file.
    Returns:
        pd.DataFrame: A DataFrame containing the extracted log information with the following columns:
            - 'ip': The IP address of the client.
            - 'datetime': The date and time of the request.
            - 'request': The HTTP method used in the request (e.g., GET, POST).
            - 'url': The requested URL.
            - 'status_code': The HTTP status code returned by the server.
            - 'user_agent': The user agent string of the client.
    Raises:
        FileNotFoundError: If the log file does not exist.
        IOError: If there is an error reading the log file.
    Notes:
        - The function expects the log format typically returned by nginx.
        - The function counts and logs the number of errors encountered during parsing.
        - The 'datetime' column is converted to a pandas datetime object.
    """
    
    # Extraction of access logs

    requests = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"]
    ip = []
    datetime = []
    request = []
    url = []
    status_code = []
    user_agent = []

    errors = 0

    # Open the log file
    with open(log_file_path, 'r') as f:
        for line in f:
            # Start by splitting the line into parts separated by quotes
            parts = line.split('"')
            # Check that the line contains quotes
            if len(parts) > 1:
                # Split the part containing the request into two parts
                log_part = parts[1].split(' ')
                # Check that the part containing the request is split into several parts
                if len(log_part) > 1:
                    # Check that the first part of the request is an HTTP method
                    if log_part[0] in requests:
                        # Add the HTTP method to the list of requests
                        request.append(log_part[0])
                        # Add the rest of the line to the list of URLs
                        url.append(' '.join(log_part[1:-1]))  
                    # If the first part of the request is not an HTTP method
                    else:
                        # Add None to the list of requests
                        request.append(None)
                        errors += 1
                        # Add the entire line to the list of URLs
                        url.append(' '.join(log_part)) 
                # If the part containing the request is not split into several parts
                else:
                    # Add None to the list of requests
                    request.append(None)
                    # Add None to the list of URLs
                    url.append(None)
                    errors += 2
            # If the line does not contain quotes
            else:
                # Add None to the list of requests
                request.append(None)
                # Add None to the list of URLs
                url.append(None)
                errors += 2

            # Add the end of the line to the list of user agents
            user_agent.append(parts[5:])

            # Add the IP address to the list of IP addresses
            ip.append(parts[0].split(' ')[0])

            # Add the date and time to the list of dates and times
            datetime.append(parts[0].split(' ')[3])
            
            # Add the status code to the list of status codes
            status_code.append(parts[2].split(' ')[1])

    # Create the DataFrame
    df_logs = pd.DataFrame({
        'ip': ip,
        'datetime': datetime,
        'request': request,
        'url': url,
        'status_code': status_code,
        'user_agent': user_agent
    })

    # Convert the datetime column to datetime
    df_logs['datetime'] = pd.to_datetime(df_logs['datetime'], format='[%d/%b/%Y:%H:%M:%S', errors='coerce')
    logger.info(f'File: {log_file_path} added to DataFrame with {errors} recorded')
    # Return the DataFrame
    return df_logs

def parse_error_log(log_file_path):
    """
    Parses an error log file and extracts relevant information into a pandas DataFrame.
    Args:
        log_file_path (str): The path to the log file to be parsed.
    Returns:
        pd.DataFrame: A DataFrame containing the extracted log information with the following columns:
            - date_time (datetime): The date and time of the log entry.
            - error_level (str): The error level (e.g., error, warning).
            - process_id (str): The process ID associated with the log entry.
            - message (str): The error message.
            - client_ip (str): The client IP address.
            - server (str): The server information.
            - request (str): The request information.
            - host (str): The host information.
    Raises:
        ValueError: If the log file cannot be read or parsed correctly.
    Notes:
        - The function expects the log format typically returned by nginx.
        - The 'date_time' column is converted to a pandas datetime object.
    """

    # Extraction of error logs
    date_time = []
    error_level = []
    process_id = []
    message = []
    client_ip = []
    server = []
    request = []
    host = []

    # Open the log file
    with open(log_file_path, 'r') as f:
        for line in f:
            # Split the line into parts separated by commas
            parts = line.split(', ')

            # Extraction of different parts of the log
            date_time.append(parts[0].split(' ')[0] + ' ' + parts[0].split(' ')[1])  # Date and time
            error_level.append(parts[0].split(' ')[2].strip('[]'))  # Error level (e.g., [error])
            process_id.append(parts[0].split(' ')[3].split('#')[1])  # Process ID

            # Error message
            message_part = parts[1].split(':', 1)[1].strip()
            message.append(message_part)

            # Client IP
            client_ip.append(parts[2].split(': ')[1])

            # Server
            server.append(parts[3].split(': ')[1])

            # Request
            request.append(parts[4].split(': ')[1])

            # Host
            host.append(parts[5].split(': ')[1])

    # Create the DataFrame
    df_error_logs = pd.DataFrame({
        'date_time': date_time,
        'error_level': error_level,
        'process_id': process_id,
        'message': message,
        'client_ip': client_ip,
        'server': server,
        'request': request,
        'host': host
    })

    # Convert the date_time column to datetime
    df_error_logs['date_time'] = pd.to_datetime(df_error_logs['date_time'], format='%Y/%m/%d %H:%M:%S', errors='coerce')
    logger.info(f'File: {log_file_path} added to error log DataFrame')
    # Return the DataFrame
    return df_error_logs
 
def parse_all_files(access_logs_directory, error_logs_directory, output_csv_access_path, output_csv_error_path):
    """
    Parses all access and error log files from the specified directories and exports the parsed data to CSV files.
    Args:
        access_logs_directory (str): The directory containing access log files.
        error_logs_directory (str): The directory containing error log files.
        output_csv_access_path (str): The file path where the combined access logs CSV will be saved.
        output_csv_error_path (str): The file path where the combined error logs CSV will be saved.
    Returns:
        None
    """
    
    all_access_logs_df = pd.DataFrame()
    all_error_logs_df = pd.DataFrame()

    # Processing access log files

    access_log_files = glob(os.path.join(access_logs_directory, '*.log.*'))
    for log_file in access_log_files:
        logger.info(f'Parsing access log file: {log_file}')
        df_access_log = parse_access_log_file(log_file)
        all_access_logs_df = pd.concat([all_access_logs_df, df_access_log], ignore_index=True)

    # Export access logs
    all_access_logs_df.to_csv(output_csv_access_path, index=False)
    logger.info(f'All access logs processed and exported to {output_csv_access_path}')

    # Processing error log files

    error_log_files = glob(os.path.join(error_logs_directory, '*.log'))
    for log_file in error_log_files:
        logger.info(f"Processing error log file: {log_file}")
        df_error_log = parse_error_log(log_file)
        all_error_logs_df = pd.concat([all_error_logs_df, df_error_log], ignore_index=True)

    # Export error logs
    all_error_logs_df.to_csv(output_csv_error_path, index=False)
    logger.info(f"All error logs processed and exported to {output_csv_error_path}")

if __name__ == '__main__':
    access_logs_directory = 'logs/access'
    error_logs_directory = 'logs/error'
    output_csv_access_path = 'dataframes/access/all_access_logs_combined.csv'
    output_csv_error_path = 'dataframes/error/all_error_logs_combined.csv'
    parse_all_files(access_logs_directory, error_logs_directory, output_csv_access_path, output_csv_error_path)
