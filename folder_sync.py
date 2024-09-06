import os
import shutil
import argparse
import time
import logging
import hashlib
import sys
from logging.handlers import RotatingFileHandler

# Import platform-specific modules
if sys.platform.startswith('win'):
    import msvcrt
else:
    import fcntl

def setup_logging(log_file):
    """
    Set up logging with file rotation and console output, supporting multiple processes.

    Args:
        log_file (str): The path to the log file.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger('FolderSync')
    logger.setLevel(logging.INFO)

    if not logger.handlers:  # Avoid adding handlers multiple times
        try:
            file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
            console_handler = logging.StreamHandler()

            formatter = logging.Formatter('%(asctime)s - %(levelname)s - PID: %(process)d - Thread: %(threadName)s - %(message)s')
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        except Exception as e:
            print(f"Error setting up logging: {e}")
            sys.exit(1)

    return logger

def calculate_md5(file_path):
    """
    Calculate the MD5 hash of a file.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: MD5 hash of the file or None if an error occurs.
    """
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except (OSError, IOError) as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None

def lock_file(file_obj):
    """
    Lock a file using platform-specific methods.

    Args:
        file_obj (file object): File object to be locked.

    Returns:
        bool: True if the file was locked successfully, False otherwise.
    """
    try:
        if sys.platform.startswith('win'):
            msvcrt.locking(file_obj.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(file_obj.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except (IOError, OSError):
        return False

def unlock_file(file_obj):
    """
    Unlock a file using platform-specific methods.

    Args:
        file_obj (file object): File object to be unlocked.
    """
    try:
        if sys.platform.startswith('win'):
            msvcrt.locking(file_obj.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            fcntl.flock(file_obj.fileno(), fcntl.LOCK_UN)
    except (IOError, OSError) as e:
        logging.error(f"Error unlocking file: {e}")

def lock_folder(folder_path, logger):
    """
    Lock a folder by creating a lock file and locking it.

    Args:
        folder_path (str): Path to the folder.
        logger (logging.Logger): Logger instance for logging events.

    Returns:
        file object: File object for the lock file or None if locking failed.
    """
    lock_file_path = os.path.join(folder_path, '.lock')
    try:
        lock_fd = open(lock_file_path, 'w')
        if lock_file(lock_fd):
            logger.info(f"Locked folder: {folder_path}")
            return lock_fd
        else:
            logger.error(f"Unable to lock folder: {folder_path}")
            lock_fd.close()
            return None
    except IOError as e:
        logger.error(f"Error locking folder {folder_path}: {e}")
        return None

def unlock_folder(lock_fd, folder_path, logger):
    """
    Unlock a folder by unlocking and removing the lock file.

    Args:
        lock_fd (file object): File object for the lock file.
        folder_path (str): Path to the folder.
        logger (logging.Logger): Logger instance for logging events.
    """
    try:
        if lock_fd:
            unlock_file(lock_fd)
            os.remove(os.path.join(folder_path, '.lock'))
            logger.info(f"Unlocked folder: {folder_path}")
    except Exception as e:
        logger.error(f"Error unlocking folder {folder_path}: {e}")
    finally:
        if lock_fd:
            lock_fd.close()


def lock_all_folders(folder, logger):
    """
    Lock all folders under a specified root folder.

    Args:
        folder (str): Root folder path.
        logger (logging.Logger): Logger instance for logging events.

    Returns:
        list of tuples: List of (folder path, file object) tuples.
    """
    locks = []
    for root, dirs, _ in os.walk(folder):
        # Lock only if there are subdirectories
        if dirs and root != folder:
            lock_fd = lock_folder(root, logger)
            if lock_fd:
                locks.append((root, lock_fd))
    return locks

def unlock_all_folders(locks, logger):
    """
    Unlock all folders given a list of (folder path, file object) tuples.

    Args:
        locks (list of tuples): List of (folder path, file object) tuples.
        logger (logging.Logger): Logger instance for logging events.
    """
    for folder_path, lock_fd in locks:
        unlock_folder(lock_fd, folder_path, logger)

def create_directories(source, replica, logger):
    """
    Create directories in the replica folder to match the source folder.

    Args:
        source (str): Source folder path.
        replica (str): Replica folder path.
        logger (logging.Logger): Logger instance for logging events.
    """
    for root, dirs, _ in os.walk(source):
        for dir_name in dirs:
            source_dir = os.path.join(root, dir_name)
            replica_dir = os.path.join(replica, os.path.relpath(source_dir, source))

            try:
                if not os.path.exists(replica_dir):
                    os.makedirs(replica_dir)
                    logger.info(f"Created directory: {replica_dir}")
            except OSError as e:
                logger.error(f"Error creating directory {replica_dir}: {e}")

def copy_files(source, replica, logger):
    """
    Copy files from the source folder to the replica folder, updating if necessary.

    Args:
        source (str): Source folder path.
        replica (str): Replica folder path.
        logger (logging.Logger): Logger instance for logging events.
    """
    for root, _, files in os.walk(source):
        for file_name in files:
            source_file = os.path.join(root, file_name)
            replica_file = os.path.join(replica, os.path.relpath(source_file, source))

            try:
                source_file_md5 = calculate_md5(source_file)
                replica_file_md5 = calculate_md5(replica_file)

                if not os.path.exists(replica_file) or (source_file_md5 and source_file_md5 != replica_file_md5):
                    shutil.copy2(source_file, replica_file)
                    logger.info(f"Copied file: {replica_file}")
            except Exception as e:
                logger.error(f"Error copying file {source_file} to {replica_file}: {e}")

def remove_extra_files(replica, source, logger):
    """
    Remove files from the replica folder that do not exist in the source folder.

    Args:
        replica (str): Replica folder path.
        source (str): Source folder path.
        logger (logging.Logger): Logger instance for logging events.
    """
    for root, _, files in os.walk(replica):
        for file_name in files:
            replica_file = os.path.join(root, file_name)
            source_file = os.path.join(source, os.path.relpath(replica_file, replica))

            try:
                if not os.path.exists(source_file):
                    os.remove(replica_file)
                    logger.info(f"Removed file: {replica_file}")
            except OSError as e:
                logger.error(f"Error removing file {replica_file}: {e}")

def remove_extra_directories(replica, source, logger):
    """
    Remove directories from the replica folder that do not exist in the source folder.

    Args:
        replica (str): Replica folder path.
        source (str): Source folder path.
        logger (logging.Logger): Logger instance for logging events.
    """
    for root, dirs, _ in os.walk(replica, topdown=False):
        for dir_name in dirs:
            replica_dir = os.path.join(root, dir_name)
            source_dir = os.path.join(source, os.path.relpath(replica_dir, replica))

            try:
                if not os.path.exists(source_dir):
                    shutil.rmtree(replica_dir)
                    logger.info(f"Removed directory: {replica_dir}")
            except OSError as e:
                logger.error(f"Error removing directory {replica_dir}: {e}")

def sync_folders(source, replica, logger):
    """
    Perform synchronization from the source folder to the replica folder.

    Args:
        source (str): Source folder path.
        replica (str): Replica folder path.
        logger (logging.Logger): Logger instance for logging events.
    """
    logger.info(f"Starting synchronization from {source} to {replica}")

    try:
        locks = lock_all_folders(source, logger)
        replica_locks = lock_all_folders(replica, logger)

        create_directories(source, replica, logger)
        copy_files(source, replica, logger)
        remove_extra_files(replica, source, logger)
        remove_extra_directories(replica, source, logger)

        unlock_all_folders(locks, logger)
        unlock_all_folders(replica_locks, logger)

    except Exception as e:
        logger.error(f"An error occurred during synchronization: {e}")
        sys.exit(1)

    logger.info("Synchronization completed")

def main():
    """
    Main function to parse command-line arguments, set up logging, and perform synchronization in a loop.
    """
    parser = argparse.ArgumentParser(description="Folder Synchronization Tool")
    parser.add_argument("source", help="Source folder path")
    parser.add_argument("replica", help="Replica folder path")
    parser.add_argument("interval", type=int, help="Synchronization interval in seconds")
    parser.add_argument("log_file", help="Log file path")
    
    args = parser.parse_args()
    logger = setup_logging(args.log_file)

    # Validate source and replica paths
    if not os.path.exists(args.source):
        logger.critical(f"Source folder '{args.source}' does not exist. Please provide a valid path.")
        sys.exit(1)
    
    if not os.path.exists(args.replica):
        logger.warning(f"Replica folder '{args.replica}' does not exist.")
        create_replica = input(f"Do you want to create the replica folder '{args.replica}'? (Y/n): ").strip().lower()
        if create_replica in ['y', 'yes', '']:
            try:
                os.makedirs(args.replica)
                logger.info(f"Created replica folder: {args.replica}")
            except OSError as e:
                logger.critical(f"Failed to create replica folder '{args.replica}': {e}")
                sys.exit(1)
        else:
            logger.critical(f"Replica folder '{args.replica}' does not exist and was not created. Exiting.")
            sys.exit(1)

    # Start synchronization loop
    while True:
        sync_folders(args.source, args.replica, logger)
        time.sleep(args.interval)

if __name__ == "__main__":
    main()