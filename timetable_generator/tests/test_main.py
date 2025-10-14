import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import TimetableGenerator, main

import unittest
from unittest.mock import patch, MagicMock
from main import TimetableGenerator, main

from io import StringIO
import sys

class TestMainModule(unittest.TestCase):
    """Unit tests for main.py timetable generator."""

    @patch("main.FileManager")
    @patch("main.ExcelLoader")
    @patch("main.ScheduleGenerator")
    @patch("main.ExcelExporter")
    def test_setup_environment_success(self, mock_exporter, mock_schedule, mock_loader, mock_file_manager):
        """✅ Test setup_environment() runs correctly when files exist"""
        # Mock the file manager behavior
        mock_file_manager.check_input_files_exist.return_value = True
        mock_loader.load_all_data.return_value = {"course": []}
        mock_schedule.return_value = MagicMock()
        mock_exporter.return_value = MagicMock()

        gen = TimetableGenerator()
        gen.setup_environment()

        self.assertIsNotNone(gen.data_frames)
        self.assertIsNotNone(gen.schedule_generator)
        self.assertIsNotNone(gen.excel_exporter)

    @patch("main.ExcelExporter")
    def test_generate_timetables(self, mock_exporter):
        """✅ Test generate_timetables() count logic"""
        mock_exporter.return_value.export_semester_branch_timetable.return_value = True

        gen = TimetableGenerator()
        gen.excel_exporter = mock_exporter.return_value
        result = gen.generate_timetables(semesters=["Sem1"], branches=["CSE"])

        self.assertEqual(result, 1, "Should return 1 successful timetable export")

    def test_print_summary_complete(self):
        """✅ Test print_summary() output for full completion"""
        gen = TimetableGenerator()
        captured_output = StringIO()
        sys.stdout = captured_output  # Redirect stdout

        gen.print_summary(2, 2)  # success == total

        sys.stdout = sys.__stdout__  # Reset redirect
        output = captured_output.getvalue()
        self.assertIn("EXPORT COMPLETE!", output)
        self.assertIn("Generated 2 / 2", output)
    def test_print_summary_partial(self):
        """✅ Test print_summary() output for partial completion"""
        gen = TimetableGenerator()
        captured_output = StringIO()
        sys.stdout = captured_output

        gen.print_summary(1, 2)  # success < total

        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()
        self.assertIn("EXPORT PARTIALLY COMPLETE!", output)
        self.assertIn("Generated 1 / 2", output)
    @patch("main.TimetableGenerator.setup_environment")
    @patch("main.TimetableGenerator.get_data_summary")
    @patch("main.TimetableGenerator.generate_timetables")
    @patch("main.TimetableGenerator.print_summary")
    @patch("main.export_all_html", create=True)
    @patch("main.generate_index_html", create=True)
    def test_main_execution(self, mock_index, mock_html, mock_summary, mock_generate, mock_data, mock_setup):
        """✅ Test main() end-to-end flow"""
        mock_generate.return_value = 3
        result = main()
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
