# simple_scheduler.py
import pandas as pd
import yaml
from collections import defaultdict
import os

# ---- utilities ----
def read_config(path="configs/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)

def parse_list_cell(cell):
    if pd.isna(cell) or str(cell).strip() == "":
        return []
    # support comma or semicolon separated
    s = str(cell)
    return [x.strip() for x in s.replace(";", ",").split(",") if x.strip()]

def ensure_dirs(path):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d)

# ---- scheduling (greedy) ----
def schedule(cfg):
    # load CSVs
    courses = pd.read_csv(cfg["input"]["course"])
    faculty = pd.read_csv(cfg["input"]["faculty"])
    rooms = pd.read_csv(cfg["input"]["rooms"])
    students = pd.read_csv(cfg["input"]["students"])

    # normalize column names lower-case (simple)
    # parse students' enrollments
    student_enroll = {}
    for _, r in students.iterrows():
        enrolled = parse_list_cell(r.get("Enrolled Courses", r.get("Enrolled Courses".lower(), "")))
        student_enroll[r["Student Roll Number"]] = set(enrolled)

    # build mapping student -> occupied (day,slot) during assignment
    student_occupied = defaultdict(set)
    # track room occupancy (room, day, slot)
    room_occupied = set()
    # track faculty availability: map name -> set(days)
    fac_days = {}
    for _, r in faculty.iterrows():
        name = r["Faculty Name"]
        fac_days[name] = set(parse_list_cell(r.get("Available Days", "")) or cfg.get("days", []))

    # Sort courses by registered students desc (large first)
    if "Registered Students" in courses.columns:
        courses = courses.sort_values("Registered Students", ascending=False)
    else:
        courses = courses

    assignments = []
    unassigned = []

    days = cfg["days"]
    slots = [s["name"] for s in cfg["slot_definitions"]]
    # For each course try to find day,slot,room
    for _, c in courses.iterrows():
        cc = c["Course Code"]
        cname = c.get("Course Name", "")
        instr = c.get("Instructor", "")
        num_students = int(c.get("Registered Students", 0))
        assigned = False

        for day in days:
            # check faculty available on day
            if instr in fac_days and day not in fac_days[instr]:
                continue
            for slot in slots:
                # quick conflict check for students
                conflict_found = False
                # find students enrolled in this course
                enrolled_students = [s for s, enroll in student_enroll.items() if cc in enroll]
                for s in enrolled_students:
                    if (day, slot) in student_occupied[s]:
                        conflict_found = True
                        break
                if conflict_found:
                    continue

                # find a room with capacity
                chosen_room = None
                for _, r in rooms.sort_values("Capacity", ascending=False).iterrows():
                    room_id = r["Room Number"]
                    cap = int(r.get("Capacity", 0))
                    if cap >= num_students and (room_id, day, slot) not in room_occupied:
                        chosen_room = room_id
                        break
                if not chosen_room:
                    continue

                # assign
                assignments.append({
                    "Course Code": cc,
                    "Course Name": cname,
                    "Instructor": instr,
                    "Registered Students": num_students,
                    "Day": day,
                    "Slot": slot,
                    "Room": chosen_room
                })
                # update occupancies
                for s in enrolled_students:
                    student_occupied[s].add((day, slot))
                room_occupied.add((chosen_room, day, slot))
                assigned = True
                break
            if assigned:
                break

        if not assigned:
            unassigned.append(cc)

    # write CSV
    out_path = cfg["output"]["timetable"]
    ensure_dirs(out_path)
    df_out = pd.DataFrame(assignments)
    df_out.to_csv(out_path, index=False)

    print(f"Assigned {len(assignments)} courses. Unassigned: {len(unassigned)}")
    if unassigned:
        print("Unassigned course codes:", unassigned)
    print("Output written to:", out_path)

if __name__ == "__main__":
    cfg = read_config("configs/config.yaml")
    schedule(cfg)
