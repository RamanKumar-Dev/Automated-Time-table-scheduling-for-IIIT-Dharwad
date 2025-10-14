"""Excel file loader for timetable generator."""
import pandas as pd
import os
from config import INPUT_DIR, REQUIRED_FILES, DEFAULT_LECTURE_HALLS, DEFAULT_TUTORIAL_ROOMS, DEFAULT_LAB_ROOMS

class ExcelLoader:
    """Handles loading of Excel files for all data inputs."""
    
    @staticmethod
    def load_all_data():
        """
        Loads all required Excel files from the input directory into a dictionary of DataFrames.
        Returns:
            dict: Keys are file base names (without _data.xlsx or .xlsx), values are DataFrames.
            None: If any file fails to load.
        """
        data_frames = {}

        for filename in REQUIRED_FILES:
            filepath = os.path.join(INPUT_DIR, filename)
            try:
                df = pd.read_excel(filepath)
                # Remove '_data.xlsx' or '.xlsx' to get the key name
                key = filename.replace('_data.xlsx', '').replace('.xlsx', '')
                data_frames[key] = df
                print(f"Loaded: {filename} ({len(df)} records)")
            except Exception as e:
                print(f"ERROR: Could not read {filename}. Error: {e}")
                return None

        if len(data_frames) != len(REQUIRED_FILES):
            print("ERROR: Not all required files were loaded.")
            return None

        print("SUCCESS: All Excel files loaded successfully.")
        return data_frames
    
    
    @staticmethod
    def get_semester_branch_courses(dfs, semester_id, branch):
        """Get courses for a specific semester and branch."""
        if 'course' not in dfs:
            return pd.DataFrame()
        course_df = dfs['course']
        if 'Semester' not in course_df.columns or 'Branch' not in course_df.columns:
            return pd.DataFrame()
        course_df = course_df.copy()
        course_df['Semester'] = pd.to_numeric(course_df['Semester'], errors='coerce')
        course_df = course_df.dropna(subset=['Semester'])
        course_df['Semester'] = course_df['Semester'].astype(int)
        return course_df[(course_df['Semester'] == semester_id) & (course_df['Branch'] == branch)].copy()
    
    @staticmethod
    def parse_ltpsc(courses_df):
        """Parse LTPSC format into separate columns."""
        if courses_df.empty or 'LTPSC' not in courses_df.columns:
            return courses_df
            
        df = courses_df.copy()
        df[['L', 'T', 'P', 'S', 'C']] = df['LTPSC'].str.split('-', expand=True)
        
        for col in ['L', 'T', 'P']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        return df
    
    @staticmethod
    def get_classrooms_by_type(classroom_df):
        """Get classrooms categorized by type from Excel data."""
        lecture_halls = []
        tutorial_rooms = []
        lab_rooms = []
        
        if classroom_df is not None and not classroom_df.empty:
            if 'Type' in classroom_df.columns and 'Room Number' in classroom_df.columns:
                lecture_mask = classroom_df['Type'].str.contains('Lecture|Auditorium', na=False, case=False)
                if lecture_mask.any():
                    lecture_halls = classroom_df[lecture_mask]['Room Number'].tolist()
                
                tutorial_mask = classroom_df['Type'].str.contains('Tutorial', na=False, case=False)
                if tutorial_mask.any():
                    tutorial_rooms = classroom_df[tutorial_mask]['Room Number'].tolist()
                
                lab_mask = classroom_df['Type'].str.contains('Lab', na=False, case=False)
                if lab_mask.any():
                    lab_rooms = classroom_df[lab_mask]['Room Number'].tolist()
        
        if not lecture_halls:
            lecture_halls = DEFAULT_LECTURE_HALLS
            print("INFO: Using default lecture halls")
        if not tutorial_rooms:
            tutorial_rooms = DEFAULT_TUTORIAL_ROOMS
            print("INFO: Using default tutorial rooms")
        if not lab_rooms:
            lab_rooms = DEFAULT_LAB_ROOMS
            print("INFO: Using default lab rooms")
            
        return {
            'lecture_halls': lecture_halls,
            'tutorial_rooms': tutorial_rooms,
            'lab_rooms': lab_rooms
        }