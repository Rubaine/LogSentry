import paramiko
import os
from dotenv import load_dotenv
from loguru import logger
import gzip
import shutil

"""
This script downloads log files from a remote server via SSH and SFTP, decompresses any files in .gz format, and saves them to local directories.
Modules:
    paramiko: Provides SSH and SFTP functionality.
    os: Provides a way of using operating system dependent functionality.
    dotenv: Loads environment variables from a .env file.
    loguru: Provides logging functionality.
    gzip: Provides a way to work with .gz files.
    shutil: Provides high-level file operations.
Functions:
    log_download(host, username, password, port, remote_log_path, local_access_log_path, local_error_log_path) -> None:
        Downloads log files from a remote server via SSH and SFTP, decompresses any files in .gz format, and saves them to local directories.
    decompress_files(local_log_file_path) -> None:
Usage:
    The script loads environment variables from a .env file and uses them to call the log_download function.
"""


# Loading environment variables
load_dotenv()

def log_download(host, username, password, port, remote_log_path, local_access_log_path, local_error_log_path) -> None:
    """
    Downloads log files from a remote server via SSH and SFTP.
    This function connects to a remote server using SSH, retrieves a list of log files from a specified directory,
    and downloads them to local directories. It handles both access and error logs, decompressing any files that are
    in .gz format.
    Args:
        host (str): The hostname or IP address of the remote server.
        username (str): The username to use for SSH authentication.
        password (str): The password to use for SSH authentication.
        port (int): The port number to use for SSH connection.
        remote_log_path (str): The remote directory path where log files are stored.
        local_access_log_path (str): The local directory path to save access log files.
        local_error_log_path (str): The local directory path to save error log files.
    Returns:
        None
    Raises:
        FileNotFoundError: If the remote path or a specific file does not exist.
        Exception: For any other errors that occur during the download process.
    """
    
    # SSH connection
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=username, password=password, port=int(port))

    # Opening the SFTP session
    sftp = client.open_sftp()
    try:
        try:
            # Retrieving the list of files
            files = sftp.listdir(remote_log_path)
            logger.debug(f"Found: {len(files)} files")  
        # Error handling if the path does not exist
        except FileNotFoundError:
            logger.error(f'Error: Remote path {remote_log_path} not found')
            return

        files_downloaded = 0
        # Downloading the files
        for file in files:
            # Path of the file to download
            remote_file = f"{remote_log_path}/{file}"
            # Paths of the local files
            local_access_file = os.path.join(local_access_log_path, file)
            local_error_file = os.path.join(local_error_log_path, file)
            # Downloading the file
            try:
                logger.debug(f"Trying to download: {remote_file}")  
                # Downloading the file
                if file.startswith('access'):
                    sftp.get(remote_file, local_access_file)
                    logger.success(f"Downloaded {remote_file} to {local_access_file}")
                elif file.startswith('error'):
                    sftp.get(remote_file, local_error_file)
                    logger.success(f"Downloaded {remote_file} to {local_error_file}")
                files_downloaded += 1
                
                # Decompressing .gz files
                if file.endswith('.gz'):
                    if file.startswith('access'):
                        logger.debug(f"Decompressing {local_access_file}")
                        decompress_files(local_access_file)
                    elif file.startswith('error'):
                        logger.debug(f"Decompressing {local_error_file}")
                        decompress_files(local_error_file)

            # Error handling if the file does not exist
            except FileNotFoundError:
                logger.error(f"Error: Remote file {remote_file} not found")
            # Handling other errors
            except Exception as e:
                logger.error(f"Error while downloading {remote_file}: {e}")
    finally:
        logger.debug(f"Downloaded: {files_downloaded} files")
        if files_downloaded < len(files):
            logger.warning(f"Only {files_downloaded} of {len(files)} files downloaded")
        elif files_downloaded == len(files):
            logger.success(f"All {files_downloaded} files downloaded")
        # Closing the SFTP connection
        sftp.close()
        # Closing the SSH connection
        client.close()

def decompress_files(local_log_file_path) -> None:
    """
    Decompresses a gzip file and removes the original compressed file.

    Args:
        local_log_file_path (str): The path to the gzip file to be decompressed.

    Raises:
        FileNotFoundError: If the specified gzip file does not exist.
        gzip.BadGzipFile: If the specified file is not a valid gzip file.
        Exception: For any other exceptions that occur during decompression.

    Logs:
        Success message upon successful decompression and removal of the original file.
        Error messages for file not found, bad gzip file, or any other exceptions.
    """
    try:
        # Decompressing the files
        decompressed_file_path = local_log_file_path.replace('.gz', '')
        # Opening the compressed file
        with gzip.open(local_log_file_path, 'rb') as f_in:
            # Opening the decompressed file
            with open(decompressed_file_path, 'wb') as f_out:
                # Copying the content of the compressed file into the decompressed file
                shutil.copyfileobj(f_in, f_out)
        logger.success(f'Decompressed {local_log_file_path} to {decompressed_file_path}')
        # Removing the compressed file
        os.remove(local_log_file_path)
    # Error handling if the file does not exist
    except FileNotFoundError:
        logger.error(f"Error: File {local_log_file_path} not found for decompression")
    # Error handling if the file is an invalid gzip file
    except gzip.BadGzipFile:
        logger.error(f"Error: Bad gzip file {local_log_file_path}")
    # Handling other errors
    except Exception as e:
        logger.error(f"Error while decompressing {local_log_file_path}: {e}")

if __name__ == '__main__':
    host = os.getenv('src_host')
    username = os.getenv('src_username')
    password = os.getenv('password')
    port = os.getenv('port')
    remote_log_path = os.getenv("remote_log_path")
    local_access_log_path = os.getenv("local_access_log_path")
    local_error_log_path = os.getenv("local_error_log_path")
    log_download(host, username, password, port, remote_log_path, local_access_log_path, local_error_log_path)
