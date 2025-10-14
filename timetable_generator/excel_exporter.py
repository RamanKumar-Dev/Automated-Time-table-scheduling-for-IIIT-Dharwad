"""Excel export utilities for timetable generation."""
import pandas as pd
import os
from file_manager import FileManager
from excel_loader import ExcelLoader

class ExcelExporter:
    """Handles exporting timetables to Excel files."""

    def __init__(self, data_frames, schedule_generator):
        self.dfs = data_frames
        self.schedule_gen = schedule_generator

    def export_semester_branch_timetable(self, semester, branch):
        """Export timetable for a specific semester, branch, and sections A and B."""
        print(f"Generating timetable for Semester {semester}, Branch {branch}")

        section_a_schedule = self.schedule_gen.generate_basic_schedule(semester, 'A', branch)
        section_b_schedule = self.schedule_gen.generate_basic_schedule(semester, 'B', branch)

        section_a_detailed = self.schedule_gen.generate_detailed_schedule(semester, 'A', branch)
        section_b_detailed = self.schedule_gen.generate_detailed_schedule(semester, 'B', branch)

        if section_a_schedule is None or section_a_schedule.empty:
            print(f"No schedule generated for semester {semester}, branch {branch}")
            return False

        filename = f"sem{semester}_{branch}_timetable.xlsx"
        filepath = FileManager.get_output_path(filename)

        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                section_a_schedule.to_excel(writer, sheet_name=f'Section_A_Student', index=True)
                section_a_detailed.to_excel(writer, sheet_name=f'Section_A_Faculty', index=True)
                section_b_schedule.to_excel(writer, sheet_name=f'Section_B_Student', index=True)
                section_b_detailed.to_excel(writer, sheet_name=f'Section_B_Faculty', index=True)

                sem_courses = ExcelLoader.get_semester_branch_courses(self.dfs, semester, branch)
                if not sem_courses.empty:
                    course_summary = sem_courses[['Course Code', 'Course Name', 'Instructor', 'LTPSC', 'Credits', 'Registered Students']]
                    course_summary.to_excel(writer, sheet_name=f'Course_Info', index=False)

            print("SUCCESS: Created", filename)
            self._print_schedule_preview(semester, section_a_schedule, section_b_schedule)
            return True

        except Exception as e:
            print("ERROR creating", filename, ":", e)
            return False

    def _print_schedule_preview(self, semester, section_a, section_b):
        """Print schedule previews."""
        print("Semester", semester, "- Section A Preview:")
        print(section_a)
        print("Semester", semester, "- Section B Preview:")
        print(section_b)
        print("-" * 50)