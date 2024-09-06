
import hashlib
import logging
import sys
import unittest
import os
import shutil
import tempfile
import time
if sys.platform.startswith('win'):
    import msvcrt
else:
    import fcntl

from unittest.mock import MagicMock, Mock, mock_open, patch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from folder_sync import (
    calculate_md5,
    lock_all_folders,
    lock_file,
    lock_folder,
    sync_folders,
    unlock_all_folders,
    unlock_file,
    create_directories,
    copy_files,
    remove_extra_files,
    remove_extra_directories,
    setup_logging,
    unlock_folder
)

class TestFolderSync(unittest.TestCase):
    def setUp(self):

        self.source_dir = tempfile.mkdtemp()
        self.replica_dir = tempfile.mkdtemp()
        self.log_file = tempfile.mktemp()
        self.logger = setup_logging(self.log_file)

    def tearDown(self):
        pass

    
    @patch('builtins.open', new_callable=mock_open, read_data=b'chunk1chunk2')
    def test_calculate_md5_success(self, mock_open_file):
        # Arrange
        file_path = '/mocked/path/file.txt'
        expected_md5 = hashlib.md5(b'chunk1chunk2').hexdigest()

        # Act
        result = calculate_md5(file_path)

        # Assert
        mock_open_file.assert_called_once_with(file_path, 'rb')  # Ensure the file is opened as binary
        self.assertEqual(result, expected_md5)  # Compare the result to the expected hash

    @patch('builtins.open', side_effect=OSError("File not found"))
    @patch('logging.error')  # Mock logging to capture errors
    def test_calculate_md5_file_error(self, mock_log_error, mock_open_file):
        # Arrange
        file_path = '/mocked/path/nonexistent.txt'

        # Act
        result = calculate_md5(file_path)

        # Assert
        mock_open_file.assert_called_once_with(file_path, 'rb')
        mock_log_error.assert_called_once_with(f"Error reading file {file_path}: File not found")
        self.assertIsNone(result)  # Expect None because of the file error

    @patch('builtins.open', new_callable=mock_open, read_data=b'')
    def test_calculate_md5_empty_file(self, mock_open_file):
        # Arrange
        file_path = '/mocked/path/empty.txt'
        expected_md5 = hashlib.md5(b'').hexdigest()

        # Act
        result = calculate_md5(file_path)

        # Assert
        mock_open_file.assert_called_once_with(file_path, 'rb')
        self.assertEqual(result, expected_md5)  # Even an empty file should have an MD5 hash

    @unittest.skipIf(not sys.platform.startswith('win'), "Requires Windows")
    @patch('msvcrt.locking')
    def test_lock_file_windows_success(self, mock_locking):
        # Arrange
        file_obj = MagicMock()
        file_obj.fileno.return_value = 1

        # Act
        result = lock_file(file_obj)

        # Assert
        mock_locking.assert_called_once_with(1, msvcrt.LK_NBLCK, 1)  # LK_NBLCK is 2
        self.assertTrue(result)  # File should be locked successfully

    @unittest.skipIf(sys.platform.startswith('win'), "Requires Unix-like system")
    @patch('fcntl.flock')
    def test_lock_file_unix_success(self, mock_flock):
        # Arrange
        file_obj = MagicMock()
        file_obj.fileno.return_value = 1

        # Act
        result = lock_file(file_obj)

        # Assert
        mock_flock.assert_called_once_with(1, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.assertTrue(result)  # File should be locked successfully

    @unittest.skipIf(not sys.platform.startswith('win'), "Requires Windows")
    @patch('msvcrt.locking', side_effect=OSError("Locking error"))
    def test_lock_file_windows_failure(self, mock_locking):
        # Arrange
        file_obj = MagicMock()
        file_obj.fileno.return_value = 1

        # Act
        result = lock_file(file_obj)

        # Assert
        mock_locking.assert_called_once_with(1, msvcrt.LK_NBLCK, 1)  # LK_NBLCK is 2
        self.assertFalse(result)  # File locking should fail

    @unittest.skipIf(sys.platform.startswith('win'), "Requires Unix-like system")
    @patch('fcntl.flock', side_effect=OSError("Flocking error"))
    def test_lock_file_unix_failure(self, mock_flock):
        # Arrange
        file_obj = MagicMock()
        file_obj.fileno.return_value = 1

        # Act
        result = lock_file(file_obj)

        # Assert
        mock_flock.assert_called_once_with(1, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.assertFalse(result)  # File locking should fail
    
    @unittest.skipIf(not sys.platform.startswith('win'), "Requires Windows")
    @patch('msvcrt.locking')
    def test_unlock_file_windows_success(self, mock_locking):
        # Arrange
        file_obj = MagicMock()
        file_obj.fileno.return_value = 1

        # Act
        unlock_file(file_obj)

        # Assert
        mock_locking.assert_called_once_with(1, msvcrt.LK_UNLCK, 1)  # LK_UNLCK is 3
        # No return value to check, just ensure method was called correctly

    @unittest.skipIf(sys.platform.startswith('win'), "Requires Unix-like system")
    @patch('fcntl.flock')
    def test_unlock_file_unix_success(self, mock_flock):
        # Arrange
        file_obj = MagicMock()
        file_obj.fileno.return_value = 1

        # Act
        unlock_file(file_obj)

        # Assert
        mock_flock.assert_called_once_with(1, fcntl.LOCK_UN)
        # No return value to check, just ensure method was called correctly

    @unittest.skipIf(not sys.platform.startswith('win'), "Requires Windows")
    @patch('msvcrt.locking', side_effect=OSError("Unlocking error"))
    @patch('logging.error')  # Mock logging to verify error handling
    def test_unlock_file_windows_failure(self, mock_logging, mock_locking):
        # Arrange
        file_obj = MagicMock()
        file_obj.fileno.return_value = 1

        # Act
        unlock_file(file_obj)

        # Assert
        mock_locking.assert_called_once_with(1, msvcrt.LK_UNLCK, 1)  # LK_UNLCK is 3
        mock_logging.assert_called_once_with("Error unlocking file: Unlocking error")

    @unittest.skipIf(sys.platform.startswith('win'), "Requires Unix-like system")
    @patch('fcntl.flock', side_effect=OSError("Unlocking error"))
    @patch('logging.error')  # Mock logging to verify error handling
    def test_unlock_file_unix_failure(self, mock_logging, mock_flock):
        # Arrange
        file_obj = MagicMock()
        file_obj.fileno.return_value = 1

        # Act
        unlock_file(file_obj)

        # Assert
        mock_flock.assert_called_once_with(1, fcntl.LOCK_UN)
        mock_logging.assert_called_once_with("Error unlocking file: Unlocking error")
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('folder_sync.lock_file')
    @patch('folder_sync.logging')
    def test_lock_folder_success(self, mock_logging, mock_lock_file, mock_open):
        # Arrange
        folder_path = '/path/to/folder'
        mock_logger = MagicMock()
        mock_open.return_value = MagicMock()
        mock_lock_file.return_value = True

        # Act
        result = lock_folder(folder_path, mock_logger)

        # Assert
        expected_lock_file_path = os.path.join(folder_path, '.lock')
        mock_open.assert_called_once_with(expected_lock_file_path, 'w')
        mock_lock_file.assert_called_once_with(mock_open.return_value)
        mock_logger.info.assert_called_once_with(f"Locked folder: {folder_path}")
        self.assertIsNotNone(result)  # Should return the file object

    @patch('builtins.open', new_callable=mock_open)
    @patch('folder_sync.lock_file')
    @patch('folder_sync.logging')
    def test_lock_folder_failure(self, mock_logging, mock_lock_file, mock_open):
        # Arrange
        folder_path = '/path/to/folder'
        mock_logger = MagicMock()
        mock_open.return_value = MagicMock()
        mock_lock_file.return_value = False

        # Act
        result = lock_folder(folder_path, mock_logger)

        # Assert
        expected_lock_file_path = os.path.join(folder_path, '.lock')
        mock_open.assert_called_once_with(expected_lock_file_path, 'w')
        mock_lock_file.assert_called_once_with(mock_open.return_value)
        mock_logger.error.assert_called_once_with(f"Unable to lock folder: {folder_path}")
        self.assertIsNone(result)  # Should return None

    @patch('builtins.open', new_callable=mock_open)
    @patch('folder_sync.lock_file')
    @patch('folder_sync.logging')
    def test_lock_folder_io_error(self, mock_logging, mock_lock_file, mock_open):
        # Arrange
        folder_path = '/path/to/folder'
        mock_logger = MagicMock()
        mock_open.side_effect = IOError("File error")

        # Act
        result = lock_folder(folder_path, mock_logger)

        # Assert
        mock_open.assert_called_once_with(os.path.join(folder_path, '.lock'), 'w')
        mock_lock_file.assert_not_called()
        mock_logger.error.assert_called_once_with(f"Error locking folder {folder_path}: File error")
        self.assertIsNone(result)  # Should return None
    
    @patch('folder_sync.unlock_file')
    @patch('os.remove')
    @patch('folder_sync.logging')
    def test_unlock_folder_success(self, mock_logging, mock_remove, mock_unlock_file):
        # Arrange
        folder_path = '/path/to/folder'
        lock_fd = MagicMock()
        lock_fd.close = MagicMock()  # Mock close method
        mock_logger = MagicMock()

        # Act
        unlock_folder(lock_fd, folder_path, mock_logger)

        # Assert
        mock_unlock_file.assert_called_once_with(lock_fd)
        lock_fd.close.assert_called_once()
        mock_remove.assert_called_once_with(os.path.join(folder_path, '.lock'))
        mock_logger.info.assert_called_once_with(f"Unlocked folder: {folder_path}")

    @patch('folder_sync.unlock_file')
    @patch('os.remove')
    @patch('folder_sync.logging')
    def test_unlock_folder_success(self, mock_logging, mock_remove, mock_unlock_file):
        # Arrange
        folder_path = '/path/to/folder'
        lock_fd = MagicMock()
        lock_fd.close = MagicMock()  # Mock the close method
        mock_logger = MagicMock()

        # Act
        unlock_folder(lock_fd, folder_path, mock_logger)

        # Assert
        mock_unlock_file.assert_called_once_with(lock_fd)
        lock_fd.close.assert_called_once()  # Ensure close() was called
        mock_remove.assert_called_once_with(os.path.join(folder_path, '.lock'))
        mock_logger.info.assert_called_once_with(f"Unlocked folder: {folder_path}")

    @patch('folder_sync.unlock_file', side_effect=Exception("Unlocking error"))
    @patch('os.remove')
    @patch('folder_sync.logging')
    def test_unlock_folder_unlock_exception(self, mock_logging, mock_remove, mock_unlock_file):
        # Arrange
        folder_path = '/path/to/folder'
        lock_fd = MagicMock()
        lock_fd.close = MagicMock()  # Mock the close method
        mock_logger = MagicMock()

        # Act
        unlock_folder(lock_fd, folder_path, mock_logger)

        # Assert
        mock_unlock_file.assert_called_once_with(lock_fd)
        lock_fd.close.assert_called_once()  # Ensure close() was called
        mock_remove.assert_not_called()  # Remove should not be called
        mock_logger.error.assert_called_once_with(f"Error unlocking folder {folder_path}: Unlocking error")

    @patch('folder_sync.unlock_file')
    @patch('os.remove', side_effect=OSError("Removal error"))
    @patch('folder_sync.logging')
    def test_unlock_folder_remove_exception(self, mock_logging, mock_remove, mock_unlock_file):
        # Arrange
        folder_path = '/path/to/folder'
        lock_fd = MagicMock()
        lock_fd.close = MagicMock()  # Mock the close method
        mock_logger = MagicMock()

        # Act
        unlock_folder(lock_fd, folder_path, mock_logger)

        # Assert
        mock_unlock_file.assert_called_once_with(lock_fd)
        lock_fd.close.assert_called_once()  # Ensure close() was called
        mock_remove.assert_called_once_with(os.path.join(folder_path, '.lock'))
        mock_logger.error.assert_called_once_with(f"Error unlocking folder {folder_path}: Removal error")
    
    @patch('os.walk')
    @patch('folder_sync.lock_folder')
    def test_lock_all_folders(self, mock_lock_folder, mock_os_walk):
        folder = '/path/to/root/folder'
        logger = MagicMock(spec=logging.Logger)
        mock_os_walk.return_value = [('/path/to/sub/folder', ['subdir1', 'subdir2'], [])]
        mock_lock_folder.return_value = 'lock_fd'
        locks = lock_all_folders(folder, logger)
        self.assertEqual(locks, [('/path/to/sub/folder', 'lock_fd')])
        mock_lock_folder.assert_called_once_with('/path/to/sub/folder', logger)

    @patch('os.walk')
    def test_lock_all_folders_no_subdirectories(self, mock_os_walk):
        folder = '/path/to/root/folder'
        logger = MagicMock(spec=logging.Logger)
        mock_os_walk.return_value = [('/path/to/root/folder', [], [])]
        locks = lock_all_folders(folder, logger)
        self.assertEqual(locks, [])

    @patch('os.walk')
    @patch('folder_sync.lock_folder')
    def test_lock_all_folders_lock_folder_fails(self, mock_lock_folder, mock_os_walk):
        folder = '/path/to/root/folder'
        logger = MagicMock(spec=logging.Logger)
        mock_os_walk.return_value = [('/path/to/sub/folder', ['subdir1', 'subdir2'], [])]
        mock_lock_folder.return_value = None
        locks = lock_all_folders(folder, logger)
        self.assertEqual(locks, [])
        mock_lock_folder.assert_called_once_with('/path/to/sub/folder', logger)

    
    @patch('folder_sync.unlock_folder')
    def test_unlock_all_folders(self, mock_unlock_folder):

        locks = [('/path/to/folder1', 'lock_fd1'), ('/path/to/folder2', 'lock_fd2')]

        logger = MagicMock(spec=logging.Logger)

        unlock_all_folders(locks, logger)

        mock_unlock_folder.assert_has_calls([

            unittest.mock.call('lock_fd1', '/path/to/folder1', logger),

            unittest.mock.call('lock_fd2', '/path/to/folder2', logger)

        ])


    @patch('folder_sync.unlock_folder')
    def test_unlock_all_folders_empty_list(self, mock_unlock_folder):

        locks = []

        logger = MagicMock(spec=logging.Logger)

        unlock_all_folders(locks, logger)

        mock_unlock_folder.assert_not_called()


    @patch('folder_sync.unlock_folder')
    def test_unlock_all_folders_unlock_folder_fails(self, mock_unlock_folder):
        locks = [('/path/to/folder1', 'lock_fd1')]
        logger = MagicMock(spec=logging.Logger)
        mock_unlock_folder.side_effect = Exception('Mocked exception')
        with self.assertRaises(Exception):
            unlock_all_folders(locks, logger)
        mock_unlock_folder.assert_called_once_with('lock_fd1', '/path/to/folder1', logger)

    @patch('os.walk')
    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch('os.path.relpath')
    @patch('os.path.join')

    def test_create_directories(self, mock_join, mock_relpath, mock_exists, mock_makedirs, mock_walk):

        source = '/path/to/source'
        replica = '/path/to/replica'
        logger = MagicMock(spec=logging.Logger)
        mock_walk.return_value = [('/path/to/source', ['dir1', 'dir2'], [])]
        mock_relpath.return_value = 'dir1'
        mock_join.return_value = '/path/to/replica/dir1'
        mock_exists.return_value = False
        create_directories(source, replica, logger)
        mock_makedirs.assert_any_call('/path/to/replica/dir1')
        logger.info.assert_any_call(f"Created directory: /path/to/replica/dir1")


    @patch('os.walk')
    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch('os.path.relpath')
    @patch('os.path.join')

    def test_create_directories_directory_already_exists(self, mock_join, mock_relpath, mock_exists, mock_makedirs, mock_walk):

        source = '/path/to/source'
        replica = '/path/to/replica'
        logger = MagicMock(spec=logging.Logger)
        mock_walk.return_value = [('/path/to/source', ['dir1', 'dir2'], [])]
        mock_relpath.return_value = 'dir1'
        mock_join.return_value = '/path/to/replica/dir1'
        mock_exists.return_value = True
        create_directories(source, replica, logger)
        mock_makedirs.assert_not_called()
        logger.info.assert_not_called()


    @patch('os.walk')
    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch('os.path.relpath')
    @patch('os.path.join')

    def test_create_directories_os_error(self, mock_join, mock_relpath, mock_exists, mock_makedirs, mock_walk):

        source = '/path/to/source'
        replica = '/path/to/replica'
        logger = MagicMock(spec=logging.Logger)
        mock_walk.return_value = [('/path/to/source', ['dir1', 'dir2'], [])]
        mock_relpath.return_value = 'dir1'
        mock_join.return_value = '/path/to/replica/dir1'
        mock_exists.return_value = False
        mock_makedirs.side_effect = OSError('Mocked OS error')

        create_directories(source, replica, logger)
        mock_makedirs.assert_any_call('/path/to/replica/dir1')
        logger.error.assert_any_call(f"Error creating directory /path/to/replica/dir1: Mocked OS error")


    @patch('os.walk')
    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch('os.path.relpath')
    @patch('os.path.join')
    def test_create_directories_empty_source(self, mock_join, mock_relpath, mock_exists, mock_makedirs, mock_walk):
        source = '/path/to/source'
        replica = '/path/to/replica'
        logger = MagicMock(spec=logging.Logger)
        mock_walk.return_value = []
        create_directories(source, replica, logger)
        mock_makedirs.assert_not_called()
        logger.info.assert_not_called()

    
    @patch('os.walk')
    @patch('folder_sync.calculate_md5')
    @patch('shutil.copy2')
    def test_copy_files(self, mock_copy2, mock_calculate_md5, mock_os_walk):
        # Setup
        source = 'source_folder'
        replica = 'replica_folder'
        logger = logging.getLogger('test_logger')

        # Mocking os.walk to return a specific structure
        mock_os_walk.return_value = [
            (source, [], ['file1.txt', 'file2.txt'])
        ]

        # Mocking calculate_md5 to return different values for source and replica files
        mock_calculate_md5.side_effect = lambda x: 'md5_source' if 'source' in x else 'md5_replica'

        # Call the function
        copy_files(source, replica, logger)

        # Assertions
        mock_copy2.assert_any_call(os.path.join(source, 'file1.txt'), os.path.join(replica, 'file1.txt'))
        mock_copy2.assert_any_call(os.path.join(source, 'file2.txt'), os.path.join(replica, 'file2.txt'))
        self.assertEqual(mock_copy2.call_count, 2)
    
    
    @patch('os.walk')
    @patch('os.path.exists')
    @patch('os.remove')
    def test_remove_extra_files(self, mock_remove, mock_path_exists, mock_os_walk):
        # Setup
        replica = 'replica_folder'
        source = 'source_folder'
        logger = logging.getLogger('test_logger')

        # Mocking os.walk to return a specific structure
        mock_os_walk.return_value = [
            (replica, [], ['file1.txt', 'file2.txt'])
        ]

        # Mocking os.path.exists to return False for files not in source
        mock_path_exists.side_effect = lambda x: False if 'source' in x else True

        # Call the function
        remove_extra_files(replica, source, logger)

        # Assertions
        mock_remove.assert_any_call(os.path.join(replica, 'file1.txt'))
        mock_remove.assert_any_call(os.path.join(replica, 'file2.txt'))
        self.assertEqual(mock_remove.call_count, 2)


    @patch('os.walk')
    @patch('os.path.exists')
    @patch('shutil.rmtree')
    def test_remove_extra_directories(self, mock_rmtree, mock_path_exists, mock_os_walk):
        # Setup
        replica = 'replica_folder'
        source = 'source_folder'
        logger = logging.getLogger('test_logger')

        # Mocking os.walk to return a specific structure
        mock_os_walk.return_value = [
            (replica, ['dir1', 'dir2'], [])
        ]

        # Mocking os.path.exists to return False for directories not in source
        mock_path_exists.side_effect = lambda x: False if 'source' in x else True

        # Call the function
        remove_extra_directories(replica, source, logger)

        # Assertions
        mock_rmtree.assert_any_call(os.path.join(replica, 'dir1'))
        mock_rmtree.assert_any_call(os.path.join(replica, 'dir2'))
        self.assertEqual(mock_rmtree.call_count, 2)

    @patch('folder_sync.lock_all_folders')
    @patch('folder_sync.create_directories')
    @patch('folder_sync.copy_files')
    @patch('folder_sync.remove_extra_files')
    @patch('folder_sync.remove_extra_directories')
    @patch('folder_sync.unlock_all_folders')
    @patch('sys.exit')
    def test_sync_folders_exception(self, mock_exit, mock_unlock_all_folders, mock_remove_extra_directories, 
                                    mock_remove_extra_files, mock_copy_files, mock_create_directories, 
                                    mock_lock_all_folders):
        # Setup
        source = 'source_folder'
        replica = 'replica_folder'
        logger = logging.getLogger('test_logger')

        # Mocking lock_all_folders to raise an exception
        mock_lock_all_folders.side_effect = Exception('Locking error')

        # Call the function
        with self.assertLogs(logger, level='ERROR') as log:
            sync_folders(source, replica, logger)

        # Assertions
        mock_lock_all_folders.assert_called_once_with(source, logger)
        mock_unlock_all_folders.assert_not_called()
        mock_exit.assert_called_once_with(1)
        self.assertIn('An error occurred during synchronization: Locking error', log.output[0])
if __name__ == '__main__':
    unittest.main()