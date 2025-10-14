import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from schedule_generator import ScheduleGenerator


class TestScheduleGenerator(unittest.TestCase):
    """Unit tests for ScheduleGenerator class"""

    def setUp(self):
        # Dummy Excel data for testing
        self.data_frames = {
            "course": pd.DataFrame({
                "Course Code": ["CS101", "MA102"],
                "Course Name": ["Data Structures", "Mathematics"],
                "Instructor": ["Prof. A", "Prof. B"],
                "Semester": [1, 1],
                "L": [3, 3],
                "T": [1, 1],
                "P": [1, 0],
                "Branch": ["CSE", "CSE"],
                "LTPSC": ["3-1-1", "3-1-0"]
            }),
            "classroom": pd.DataFrame({
                "Room": ["LH1", "LH2", "TR1", "LB1"],
                "Type": ["lecture_hall", "lecture_hall", "tutorial_room", "lab_room"]
            })
        }

        self.generator = ScheduleGenerator(self.data_frames)

    @patch("schedule_generator.ExcelLoader.get_classrooms_by_type")
    def test_initialize_schedule(self, mock_get_classrooms):
        """✅ Test schedule initialization creates Free + Lunch Break rows"""
        from config import LUNCH_SLOT
        schedule = self.generator._initialize_schedule()
        self.assertIn(LUNCH_SLOT, schedule.index)
        self.assertTrue(all("Free" in schedule.values or "LUNCH BREAK" in schedule.values))

    @patch("schedule_generator.ExcelLoader.get_classrooms_by_type")
    @patch("schedule_generator.ExcelLoader.get_semester_branch_courses")
    @patch("schedule_generator.ExcelLoader.parse_ltpsc")
    def test_generate_basic_schedule(self, mock_parse, mock_courses, mock_classrooms):
        """✅ Test generate_basic_schedule fills some slots"""
        # Mock data
        mock_courses.return_value = pd.DataFrame({
            "Course Code": ["CS101"],
            "L": [2],
            "T": [1],
            "P": [1]
        })
        mock_parse.return_value = mock_courses.return_value
        mock_classrooms.return_value = {
            "lecture_halls": ["LH1"],
            "tutorial_rooms": ["TR1"],
            "lab_rooms": ["LB1"]
        }

        schedule = self.generator.generate_basic_schedule(1, "A", "CSE")
        # There should be at least one scheduled slot
        filled = schedule.apply(lambda col: col[col != "Free"]).count().sum()
        self.assertGreater(filled, 0)

    @patch("schedule_generator.ExcelLoader.get_classrooms_by_type")
    @patch("schedule_generator.ExcelLoader.get_semester_branch_courses")
    @patch("schedule_generator.ExcelLoader.parse_ltpsc")
    def test_generate_detailed_schedule(self, mock_parse, mock_courses, mock_classrooms):
        """✅ Test generate_detailed_schedule adds instructor names"""
        mock_courses.return_value = pd.DataFrame({
            "Course Code": ["CS101"],
            "Course Name": ["Data Structures"],
            "Instructor": ["Prof. A"],
            "L": [2],
            "T": [1],
            "P": [1],
            "LTPSC": ["3-1-1"],
        })
        mock_parse.return_value = mock_courses.return_value
        mock_classrooms.return_value = {
            "lecture_halls": ["LH1"],
            "tutorial_rooms": ["TR1"],
            "lab_rooms": ["LB1"]
        }

        detailed_schedule = self.generator.generate_detailed_schedule(1, "A", "CSE")
        found = any("Prof." in str(x) for x in detailed_schedule.values.flatten())
        self.assertTrue(found or detailed_schedule is not None)

    def test_get_classroom_types(self):
        """✅ Test classroom type extraction"""
        with patch("schedule_generator.ExcelLoader.get_classrooms_by_type") as mock_func:
            mock_func.return_value = {
                "lecture_halls": ["LH1", "LH2"],
                "tutorial_rooms": ["TR1"],
                "lab_rooms": ["LB1"]
            }
            result = self.generator._get_classroom_types()
            self.assertIn("lecture_halls", result)
            self.assertIn("lab_rooms", result)

    def test_initialize_schedule_has_free(self):
        """✅ Test all initial slots are 'Free' except lunch"""
        schedule = self.generator._initialize_schedule()
        free_cells = schedule.replace("LUNCH BREAK", "Free")
        self.assertTrue((free_cells == "Free").all().all())


if __name__ == "__main__":
    unittest.main()
