"""Configuration settings for the timetable generator."""

# Directory paths
INPUT_DIR = '/home/raman-kumar/Desktop/program/Python/project/Automated-Time-Table-IIIT-DHARWAD-main/timetable_generator/sdtt_inputs_zip/sdtt_inputs'
OUTPUT_DIR = '/home/raman-kumar/Desktop/program/Python/project/Automated-Time-Table-IIIT-DHARWAD-main/timetable_generator/output_timetables'

# Required Excel input files
REQUIRED_FILES = [
    'course_data.xlsx',
    'faculty_availability.xlsx', 
    'classroom_data.xlsx',
    'student_data.xlsx',
    'exams_data.xlsx'
]

# Time scheduling configuration
DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
TIME_SLOTS = [
    '9:00-10:30', '10:30-12:00',
    '12:30-14:00', '14:00-15:30', '15:30-17:00', '17:00-18:30'
]
LUNCH_SLOT = '12:30-14:00'
AVAILABLE_TIMES = [t for t in TIME_SLOTS if t != LUNCH_SLOT]

# Lab scheduling configuration
LAB_SLOTS_2HR = [
    ('14:00-15:30', '15:30-17:00'),
    ('15:30-17:00', '17:00-18:30')
]

# Target semesters
TARGET_SEMESTERS = [1, 3, 5, 7]

# Default room fallbacks
DEFAULT_LECTURE_HALLS = ['C002', 'C003', 'C101', 'C102', 'C104']
DEFAULT_TUTORIAL_ROOMS = ['C408', 'C202', 'C203']
DEFAULT_LAB_ROOMS = ['L106', 'L107']