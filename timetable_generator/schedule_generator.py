"""Core scheduling logic for generating timetables from Excel data."""
import pandas as pd
import random
from config import DAYS, TIME_SLOTS, LUNCH_SLOT, AVAILABLE_TIMES, LAB_SLOTS_2HR
from excel_loader import ExcelLoader

class ScheduleGenerator:
    """Generates weekly class schedules for semesters and sections from Excel data."""
    
    def __init__(self, data_frames):
        """Initialize ScheduleGenerator with data frames."""
        self.dfs = data_frames
        self.classroom_types = None
        
    def _initialize_schedule(self):
        """Initialize an empty schedule with lunch break."""
        schedule = pd.DataFrame(index=TIME_SLOTS, columns=DAYS, dtype=object)
        schedule = schedule.fillna('Free')
        schedule.loc[LUNCH_SLOT] = 'LUNCH BREAK'
        return schedule
    
    def _get_classroom_types(self):
        """Get classroom types from Excel data."""
        if self.classroom_types is None:
            classroom_df = self.dfs.get('classroom', pd.DataFrame())
            self.classroom_types = ExcelLoader.get_classrooms_by_type(classroom_df)
        return self.classroom_types
    
    def generate_basic_schedule(self, semester_id, section, branch):
        """Generates a weekly class schedule for a specific semester, section, and branch."""
        sem_courses = ExcelLoader.get_semester_branch_courses(self.dfs, semester_id, branch)
        if sem_courses.empty:
            print(f"WARNING: No courses found for semester {semester_id}, branch {branch}")
            return self._initialize_schedule()
        
        sem_courses = ExcelLoader.parse_ltpsc(sem_courses)
        classroom_types = self._get_classroom_types()
        
        schedule = self._initialize_schedule()
        used_slots = set()
        course_day_usage = {}
        
        self._schedule_lectures(sem_courses, schedule, used_slots, course_day_usage, classroom_types)
        self._schedule_tutorials(sem_courses, schedule, used_slots, section, classroom_types)
        self._schedule_labs(sem_courses, schedule, used_slots, section, classroom_types)
        
        return schedule
    
    def _schedule_lectures(self, sem_courses, schedule, used_slots, course_day_usage, classroom_types):
        """Schedule lecture sessions from Excel data."""
        for _, course in sem_courses.iterrows():
            if course['L'] > 0:
                course_code = course['Course Code']
                lectures_needed = course['L']
                
                if course_code not in course_day_usage:
                    course_day_usage[course_code] = set()
                
                lectures_scheduled = 0
                max_attempts = 50
                
                while lectures_scheduled < lectures_needed and max_attempts > 0:
                    max_attempts -= 1
                    
                    available_days = [day for day in DAYS if day not in course_day_usage[course_code]]
                    if not available_days:
                        course_day_usage[course_code] = set()
                        available_days = DAYS.copy()
                    
                    day = random.choice(available_days)
                    time_slot = random.choice(AVAILABLE_TIMES)
                    slot_key = (day, time_slot, 'lecture')
                    
                    if slot_key not in used_slots and schedule.loc[time_slot, day] == 'Free':
                        room = random.choice(classroom_types['lecture_halls'])
                        schedule.loc[time_slot, day] = f"{course_code}"
                        used_slots.add(slot_key)
                        course_day_usage[course_code].add(day)
                        lectures_scheduled += 1
    
    def _schedule_tutorials(self, sem_courses, schedule, used_slots, section, classroom_types):
        """Schedule tutorial sessions from Excel data."""
        for _, course in sem_courses.iterrows():
            if course['T'] > 0:
                course_code = course['Course Code']
                tutorials_needed = course['T']
                
                tutorials_scheduled = 0
                max_attempts = 50
                
                while tutorials_scheduled < tutorials_needed and max_attempts > 0:
                    max_attempts -= 1
                    
                    day = random.choice(DAYS)
                    time_slot = random.choice(AVAILABLE_TIMES)
                    slot_key = (day, time_slot, f'tutorial_{section}')
                    
                    if slot_key not in used_slots and schedule.loc[time_slot, day] == 'Free':
                        room = random.choice(classroom_types['tutorial_rooms'])
                        schedule.loc[time_slot, day] = f"{course_code}(T-{section})"
                        used_slots.add(slot_key)
                        tutorials_scheduled += 1
    
    def _schedule_labs(self, sem_courses, schedule, used_slots, section, classroom_types):
        """Schedule lab sessions from Excel data."""
        for _, course in sem_courses.iterrows():
            if course['P'] > 0:
                course_code = course['Course Code']
                labs_needed = course['P']
                
                labs_scheduled = 0
                max_attempts = 30
                
                while labs_scheduled < labs_needed and max_attempts > 0:
                    max_attempts -= 1
                    
                    day = random.choice(DAYS)
                    lab_slot_pair = random.choice(LAB_SLOTS_2HR)
                    slot1, slot2 = lab_slot_pair
                    slot1_key = (day, slot1, f'lab_{section}')
                    slot2_key = (day, slot2, f'lab_{section}')
                    
                    if (slot1_key not in used_slots and slot2_key not in used_slots and
                        schedule.loc[slot1, day] == 'Free' and schedule.loc[slot2, day] == 'Free'):
                        
                        room = random.choice(classroom_types['lab_rooms'])
                        schedule.loc[slot1, day] = f"{course_code}(P-{section})"
                        schedule.loc[slot2, day] = f"{course_code}(P-{section})"
                        used_slots.add(slot1_key)
                        used_slots.add(slot2_key)
                        labs_scheduled += 1
                
                while labs_scheduled < labs_needed and max_attempts > 0:
                    max_attempts -= 1
                    
                    day = random.choice(DAYS)
                    time_slot = random.choice(AVAILABLE_TIMES)
                    slot_key = (day, time_slot, f'lab_{section}')
                    
                    if slot_key not in used_slots and schedule.loc[time_slot, day] == 'Free':
                        room = random.choice(classroom_types['lab_rooms'])
                        schedule.loc[time_slot, day] = f"{course_code}(P-{section})"
                        used_slots.add(slot_key)
                        labs_scheduled += 1
    
    def generate_detailed_schedule(self, semester_id, section, branch):
        """Generate a more detailed schedule with instructor information from Excel data."""
        basic_schedule = self.generate_basic_schedule(semester_id, section,branch)
        if basic_schedule is None or basic_schedule.empty:
            return basic_schedule
        
        course_info = ExcelLoader.get_semester_branch_courses(self.dfs, semester_id, branch)
        if course_info.empty:
            return basic_schedule
            
        course_map = course_info.set_index('Course Code')[['Course Name', 'Instructor', 'LTPSC']].to_dict('index')
        
        detailed_schedule = basic_schedule.copy()
        
        for day in detailed_schedule.columns:
            for time_slot in detailed_schedule.index:
                cell_content = detailed_schedule.loc[time_slot, day]
                if cell_content not in ['Free', 'LUNCH BREAK'] and '(' not in str(cell_content):
                    course_code = cell_content
                    if course_code in course_map:
                        instructor = course_map[course_code]['Instructor']
                        detailed_schedule.loc[time_slot, day] = f"{course_code} - {instructor}"
        
        return detailed_schedule 