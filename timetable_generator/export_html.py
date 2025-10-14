import os
import pandas as pd

OUTPUT_DIR = '/home/raman-kumar/Desktop/program/Python/project/Automated-Time-Table-IIIT-DHARWAD-main/timetable_generator/output_timetables'
HTML_DIR = '/home/raman-kumar/Desktop/program/Python/project/Automated-Time-Table-IIIT-DHARWAD-main/timetable_generator/html_output'
BRANCHES = ['CSE', 'DSAI', 'ECE']
SEMESTERS = [1, 3, 5, 7]

os.makedirs(HTML_DIR, exist_ok=True)

def excel_sheet_to_html(excel_path, sheet_name, html_path):
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    df.to_html(html_path, index=False, border=1)

for sem in SEMESTERS:
    for branch in BRANCHES:
        excel_file = f"sem{sem}_{branch}_timetable.xlsx"
        excel_path = os.path.join(OUTPUT_DIR, excel_file)
        if not os.path.exists(excel_path):
            continue

        # For CSE: Section A, Section B, Faculty
        if branch == "CSE":
            for sheet in ["Section_A_Student", "Section_B_Student", "Section_A_Faculty", "Section_B_Faculty"]:
                html_file = f"sem{sem}_{branch}_{sheet}.html"
                html_path = os.path.join(HTML_DIR, html_file)
                try:
                    excel_sheet_to_html(excel_path, sheet, html_path)
                    print(f"Generated: {html_file}")
                except Exception as e:
                    print(f"Error generating {html_file}: {e}")

        # For DSAI and ECE: Only Section A and Faculty
        else:
            for sheet in ["Section_A_Student", "Section_A_Faculty"]:
                html_file = f"sem{sem}_{branch}_{sheet}.html"
                html_path = os.path.join(HTML_DIR, html_file)
                try:
                    excel_sheet_to_html(excel_path, sheet, html_path)
                    print(f"Generated: {html_file}")
                except Exception as e:
                    print(f"Error generating {html_file}: {e}")