import os
import pandas as pd
from config import OUTPUT_DIR, TARGET_SEMESTERS, INPUT_DIR

def generate_index_html():
    # Load course data
    course_path = os.path.join(INPUT_DIR, 'course_data.xlsx')
    courses_df = pd.read_excel(course_path)

    # Map semester number to available courses
    sem_course_map = {}
    for sem in TARGET_SEMESTERS:
        sem_courses = courses_df[courses_df['Semester'] == sem]['Course Name'].tolist()
        sem_course_map[sem] = sem_courses

    # HTML generation
    html = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Timetable Selector</title>
    <link href="https://fonts.googleapis.com/css?family=Roboto:400,700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Roboto', sans-serif; background: #f5f7fa; margin: 0; padding: 0; }
        .container { background: #fff; margin: 40px auto; padding: 32px; border-radius: 16px; box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.15); max-width: 700px; }
        h1 { color: #2d3a4b; text-align: center; margin-bottom: 24px; }
        select { padding: 10px 16px; border-radius: 8px; border: 1px solid #bfc9d1; font-size: 1rem; margin-bottom: 24px; outline: none; }
        button { background: #4f8cff; color: #fff; border: none; border-radius: 8px; padding: 10px 24px; font-size: 1rem; cursor: pointer; transition: background 0.2s; }
        button:hover { background: #2563eb; }
        .select-row { margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Select a Timetable</h1>
        <form id="timetableForm">
            <div class="select-row">
                <select id="semesterSelect" required>
                    <option value="" disabled selected>Choose a semester</option>
'''
    for sem in TARGET_SEMESTERS:
        html += f'                    <option value="{sem}">Semester {sem}</option>\n'
    html += '''                </select>
                <select id="branchSelect" required style="margin-left: 10px;">
                    <option value="" disabled selected>Choose a branch</option>
                    <option value="CSE">CSE</option>
                    <option value="DSAI">DSAI</option>
                    <option value="ECE">ECE</option>
                </select>
                <select id="sectionSelect" required style="margin-left: 10px;">
                    <option value="" disabled selected>Choose a section</option>
                    <option value="Section_A_Student">Section A (Student)</option>
                    <option value="Section_A_Faculty">Section A (Faculty)</option>
                    <option value="Section_B_Student">Section B (Student)</option>
                    <option value="Section_B_Faculty">Section B (Faculty)</option>
                </select>
            </div>
            <button type="submit">View Timetable</button>
        </form>
    </div>
    <script>
        document.getElementById('timetableForm').addEventListener('submit', function(e) {
            e.preventDefault();
            const sem = document.getElementById('semesterSelect').value;
            const branch = document.getElementById('branchSelect').value;
            const section = document.getElementById('sectionSelect').value;
            if (sem && branch && section) {
                // Adjust file path to html_output
                const url = `html_output/sem${sem}_${branch}_${section}.html`;
                window.location.href = url;
            }
        });
    </script>
</body>
</html>
'''

    # Save index.html in the parent directory of OUTPUT_DIR
    index_path = os.path.join(os.path.dirname(OUTPUT_DIR), 'index.html')
    with open(index_path, 'w', encoding='utf-8') as f:
        f.write(html)
