import unittest
import os
import zipfile
import tempfile
from unittest.mock import patch
from file_manager import FileManager  # Adjust this import if the file has a different name

class TestFileManager(unittest.TestCase):
    
    def setUp(self):
        # Create temporary directories for testing
        self.temp_input = tempfile.TemporaryDirectory()
        self.temp_output = tempfile.TemporaryDirectory()
        
        # Patch the class attributes to use temporary directories
        patcher_input = patch.object(FileManager, 'INPUT_DIR', self.temp_input.name)
        patcher_output = patch.object(FileManager, 'OUTPUT_DIR', self.temp_output.name)
        patcher_required = patch.object(FileManager, 'REQUIRED_FILES', ['test.xlsx', 'data.xlsx'])
        
        self.addCleanup(patcher_input.stop)
        self.addCleanup(patcher_output.stop)
        self.addCleanup(patcher_required.stop)
        
        self.mock_input = patcher_input.start()
        self.mock_output = patcher_output.start()
        self.mock_required = patcher_required.start()
    
    def tearDown(self):
        self.temp_input.cleanup()
        self.temp_output.cleanup()

    def test_setup_directories(self):
        """Test that directories are created successfully."""
        FileManager.setup_directories()
        self.assertTrue(os.path.exists(FileManager.INPUT_DIR))
        self.assertTrue(os.path.exists(FileManager.OUTPUT_DIR))

    def test_setup_files_from_zip_success(self):
        """Test extracting files from a zip works correctly."""
        # Create a temporary zip file with one Excel file
        temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        with zipfile.ZipFile(temp_zip.name, 'w') as zf:
            zf.writestr('test.xlsx', 'dummy content')
        
        result = FileManager.setup_files_from_zip(temp_zip.name)
        self.assertTrue(result)
        self.assertTrue(os.path.exists(os.path.join(FileManager.INPUT_DIR, 'test.xlsx')))
        
        os.unlink(temp_zip.name)

    def test_setup_files_from_zip_fail(self):
        """Test extraction fails gracefully with an invalid zip."""
        result = FileManager.setup_files_from_zip('non_existent.zip')
        self.assertFalse(result)

    def test_check_input_files_exist_all_present(self):
        """Test that all required files being present returns True."""
        for filename in FileManager.REQUIRED_FILES:
            open(os.path.join(FileManager.INPUT_DIR, filename), 'w').close()
        
        result = FileManager.check_input_files_exist()
        self.assertTrue(result)

    def test_check_input_files_exist_missing_files(self):
        """Test that missing required files returns False."""
        # Create only one of the required files
        open(os.path.join(FileManager.INPUT_DIR, FileManager.REQUIRED_FILES[0]), 'w').close()
        result = FileManager.check_input_files_exist()
        self.assertFalse(result)

    def test_get_output_path(self):
        """Test that the output path is generated correctly."""
        filename = 'output.xlsx'
        expected_path = os.path.join(FileManager.OUTPUT_DIR, filename)
        self.assertEqual(FileManager.get_output_path(filename), expected_path)

    def test_list_input_files(self):
        """Test listing files in input directory."""
        filenames = ['file1.xlsx', 'file2.xlsx']
        for f in filenames:
            open(os.path.join(FileManager.INPUT_DIR, f), 'w').close()
        
        files = FileManager.list_input_files()
        self.assertEqual(set(files), set(filenames))

if __name__ == '__main__':
    unittest.main()
