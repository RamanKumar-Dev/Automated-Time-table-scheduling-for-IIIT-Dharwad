"""Main execution module for Excel-based timetable generation."""
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from file_manager import FileManager
from excel_loader import ExcelLoader
from schedule_generator import ScheduleGenerator
from excel_exporter import ExcelExporter
from config import TARGET_SEMESTERS, REQUIRED_FILES

BRANCHES = ['CSE', 'DSAI', 'ECE']

class TimetableGenerator:
    """Main class to coordinate timetable generation from Excel files."""
    
    def _init_(self):
        self.data_frames = None
        self.schedule_generator = None
        self.excel_exporter = None
    
    def setup_environment(self, zip_file_path=None):
        """Set up the environment and load Excel data."""
        try:
            import openpyxl
            print("openpyxl is available")
        except ImportError:
            print("Installing openpyxl...")
            os.system('pip install openpyxl -q')
            import openpyxl
        
        FileManager.setup_directories()
        
        if zip_file_path and os.path.exists(zip_file_path):
            print("Using zip file input...")
            if FileManager.setup_files_from_zip(zip_file_path):
                print("Zip extraction successful")
            else:
                print("Zip extraction failed")
        
        if not FileManager.check_input_files_exist():
            print("ERROR: Required Excel files are missing.")
            print("Please ensure the following Excel files are in the input directory:")
            for file in REQUIRED_FILES:
                print("  -", file)
            print("Input directory:", FileManager.INPUT_DIR)
            FileManager.list_input_files()
            raise Exception("Missing required Excel files")
        
        self.data_frames = ExcelLoader.load_all_data()
        if self.data_frames is None:
            raise Exception("Failed to load data from Excel files")
        
        self.schedule_generator = ScheduleGenerator(self.data_frames)
        self.excel_exporter = ExcelExporter(self.data_frames, self.schedule_generator)
    
    
    def generate_timetables(self, semesters=None, branches=None):
        """Generate timetables for specified semesters and branches from Excel data."""
        if semesters is None:
            semesters = TARGET_SEMESTERS
        if branches is None:
            branches = BRANCHES

        print("\n" + "="*80)
        print("GENERATING TIMETABLES FROM EXCEL FILES")
        print("INPUT: Excel files | OUTPUT: Excel timetables")
        print("FEATURES:")
        print("  - Direct Excel file reading")
        print("  - Same course appears multiple times in a week")
        print("  - No course appears multiple times in same day (except labs/tutorials)")
        print("  - Lectures: Common for both sections")
        print("  - Tutorials/Labs: Separate for each section")
        print("TIMING: Lectures=1.5h, Tutorials=1h, Labs=2h")
        print("LUNCH BREAK: 12:30-14:00 (No classes)")
        print("="*80)

        success_count = 0
        for semester in semesters:
            for branch in branches:
                if self.excel_exporter.export_semester_branch_timetable(semester, branch):
                    success_count += 1

        return success_count
    
    def print_summary(self, success_count, total_semesters):
        """Print generation summary."""
        print("\n" + "="*80)
        if success_count == total_semesters:
            print("EXPORT COMPLETE!")
        else:
            print("EXPORT PARTIALLY COMPLETE!")
        
        print("Generated", success_count, "/", total_semesters, "timetable files")
        print("Each Excel file contains:")
        print("  - Section A Student View")
        print("  - Section A Faculty View")
        print("  - Section B Student View")
        print("  - Section B Faculty View")
        print("  - Course Information sheet")
        print("Files saved in:", FileManager.OUTPUT_DIR)
        print("="*80)
    
    def get_data_summary(self):
        """Print summary of loaded Excel data."""
        if self.data_frames:
            print("\nEXCEL DATA SUMMARY:")
            for key, df in self.data_frames.items():
                print("  ", key, ":", len(df), "records")
            
            if 'course' in self.data_frames:
                print("\nCOURSES BY SEMESTER:")
                course_df = self.data_frames['course']
                for semester in sorted(course_df['Semester'].unique()):
                    sem_courses = course_df[course_df['Semester'] == semester]
                    print("  Semester", semester, ":", len(sem_courses), "courses")

def main(zip_file_path=None):
    """Main function to generate timetables from Excel files."""
    generator = TimetableGenerator()
    
    try:
        generator.setup_environment(zip_file_path)
        generator.get_data_summary()
        success_count = generator.generate_timetables()
        generator.print_summary(success_count, len(TARGET_SEMESTERS))

        # Generate HTML files from the output Excel timetables
        try:
            from export_html import export_all_html
            export_all_html()
            print("HTML files generated for all sections.")
        except Exception as html_err:
            print("WARNING: HTML timetable generation failed:", html_err)

        # Generate index.html for selection
        try:
            from _generate_index_html import generate_index_html
            generate_index_html()
            print("index.html generated for timetable selection.")
        except Exception as idx_err:
            print("WARNING: index.html generation failed:", idx_err)

    except Exception as e:
        print("ERROR:", e)
        return False
    
    return True

if __name__ == "__main__":
    main()