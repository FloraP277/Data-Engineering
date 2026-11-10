"""Generate SYNTHETIC sample files shaped exactly like the MSHA downloads.

Use them to run and test the pipeline without downloading the real data.
They have the real file layout (pipe-delimited, full MSHA header, zipped) and
deliberately include the problems real data has, so the quality rules have
something to catch:

- blank and missing IDs, malformed dates, dates in the future
- negative penalties, non-numeric numbers, duplicate rows
- mine IDs missing their leading zeros
- records that point to mines not in the Mines file

Two weekly batches are written. The second repeats most of the first, changes
some records (e.g. a mine changes operator) and adds new ones, which exercises
the incremental MERGE logic.

    python scripts/make_sample_data.py --out sample_data
"""
import argparse
import datetime as dt
import io
import os
import random
import zipfile

MINES_COLS = """MINE_ID CURRENT_MINE_NAME COAL_METAL_IND CURRENT_MINE_TYPE CURRENT_MINE_STATUS CURRENT_STATUS_DT
CURRENT_CONTROLLER_ID CURRENT_CONTROLLER_NAME CURRENT_OPERATOR_ID CURRENT_OPERATOR_NAME STATE BOM_STATE_CD
FIPS_CNTY_CD FIPS_CNTY_NM CONG_DIST_CD COMPANY_TYPE CURRENT_CONTROLLER_BEGIN_DT DISTRICT OFFICE_CD OFFICE_NAME
ASSESS_CTRL_NO PRIMARY_SIC_CD PRIMARY_SIC PRIMARY_SIC_CD_1 PRIMARY_SIC_CD_SFX SECONDARY_SIC_CD SECONDARY_SIC
SECONDARY_SIC_CD_1 SECONDARY_SIC_CD_SFX PRIMARY_CANVASS_CD PRIMARY_CANVASS SECONDARY_CANVASS_CD SECONDARY_CANVASS
CURRENT_103I CURRENT_103I_DT PORTABLE_OPERATION PORTABLE_FIPS_ST_CD DAYS_PER_WEEK HOURS_PER_SHIFT
PROD_SHIFTS_PER_DAY MAINT_SHIFTS_PER_DAY NO_EMPLOYEES PART48_TRAINING LONGITUDE LATITUDE AVG_MINE_HEIGHT
MINE_GAS_CATEGORY_CD METHANE_LIBERATION NO_PRODUCING_PITS NO_NONPRODUCING_PITS NO_TAILING_PONDS
PILLAR_RECOVERY_USED HIGHWALL_MINER_USED MULTIPLE_PITS MINERS_REP_IND SAFETY_COMMITTEE_IND MILES_FROM_OFFICE
DIRECTIONS_TO_MINE NEAREST_TOWN""".split()

INSPECTIONS_COLS = """EVENT_NO MINE_ID INSPECTION_BEGIN_DT INSPECTION_END_DT CONTROLLER_ID CONTROLLER_NAME OPERATOR_ID
OPERATOR_NAME CAL_YR CAL_QTR FISCAL_YR FISCAL_QTR INSPECT_OFFICE_CD ACTIVITY_CODE ACTIVITY ACTIVE_SECTIONS
IDLE_SECTIONS SHAFT_SLOPE_SINK IMPOUND_CONSTR BLDG_CONSTR_SITES DRAGLINES UNCLASSIFIED_CONSTR CO_RECORDS
SURF_UG_MINE SURF_FACILITY_MINE REFUSE_PILES EXPLOSIVE_STORAGE OUTBY_AREAS MAJOR_CONSTR SHAFTS_SLOPES IMPOUNDMENTS
MISC_AREA PROGRAM_AREA SAMPLE_CNT_AIR SAMPLE_CNT_DUSTSPOT SAMPLE_CNT_DUSTSURVEY SAMPLE_CNT_RESPDUST
SAMPLE_CNT_NOISE SAMPLE_CNT_OTHER NBR_INSPECTORS TOTAL_ON_SITE_HOURS TOTAL_INSP_HOURS COAL_METAL_IND
TOTAL_ON_SITE_HRS_SPVR_TRAINEE TOTAL_INSP_HRS_SPVR_TRAINEE""".split()

VIOLATIONS_COLS = """EVENT_NO INSPECTION_BEGIN_DT INSPECTION_END_DT VIOLATION_NO CONTROLLER_ID CONTROLLER_NAME
VIOLATOR_ID VIOLATOR_NAME VIOLATOR_TYPE_CD MINE_ID MINE_NAME MINE_TYPE COAL_METAL_IND CONTRACTOR_ID
VIOLATION_ISSUE_DT VIOLATION_OCCUR_DT CAL_YR CAL_QTR FISCAL_YR FISCAL_QTR VIOLATION_ISSUE_TIME SIG_SUB
SECTION_OF_ACT PART_SECTION SECTION_OF_ACT_1 SECTION_OF_ACT_2 CIT_ORD_SAFE ORIG_TERM_DUE_DT ORIG_TERM_DUE_TIME
LATEST_TERM_DUE_DT LATEST_TERM_DUE_TIME TERMINATION_DT TERMINATION_TIME TERMINATION_TYPE VACATE_DT VACATE_TIME
INITIAL_VIOL_NO REPLACED_BY_ORDER_NO LIKELIHOOD INJ_ILLNESS NO_AFFECTED NEGLIGENCE WRITTEN_NOTICE
ENFORCEMENT_AREA SPECIAL_ASSESS PRIMARY_OR_MILL RIGHT_TO_CONF_DT ASMT_GENERATED_IND FINAL_ORDER_ISSUE_DT
PROPOSED_PENALTY AMOUNT_DUE AMOUNT_PAID BILL_PRINT_DT LAST_ACTION_CD LAST_ACTION_DT DOCKET_NO
DOCKET_STATUS_CD CONTESTED_IND CONTESTED_DT VIOLATOR_VIOLATION_CNT VIOLATOR_INSPECTION_DAY_CNT""".split()

ACCIDENTS_COLS = """MINE_ID CONTROLLER_ID CONTROLLER_NAME OPERATOR_ID OPERATOR_NAME CONTRACTOR_ID DOCUMENT_NO
SUBUNIT_CD SUBUNIT ACCIDENT_DT CAL_YR CAL_QTR FISCAL_YR FISCAL_QTR ACCIDENT_TIME DEGREE_INJURY_CD DEGREE_INJURY
FIPS_STATE_CD UG_LOCATION_CD UG_LOCATION UG_MINING_METHOD_CD UG_MINING_METHOD MINING_EQUIP_CD MINING_EQUIP
EQUIP_MFR_CD EQUIP_MFR_NAME EQUIP_MODEL_NO SHIFT_BEGIN_TIME CLASSIFICATION_CD CLASSIFICATION ACCIDENT_TYPE_CD
ACCIDENT_TYPE NO_INJURIES TOT_EXPER MINE_EXPER JOB_EXPER OCCUPATION_CD OCCUPATION ACTIVITY_CD ACTIVITY
INJURY_SOURCE_CD INJURY_SOURCE NATURE_INJURY_CD NATURE_INJURY INJ_BODY_PART_CD INJ_BODY_PART SCHEDULE_CHARGE
DAYS_RESTRICT DAYS_LOST TRANS_TERM RETURN_TO_WORK_DT IMMED_NOTIFY_CD IMMED_NOTIFY INVEST_BEGIN_DT NARRATIVE
CLOSED_DOC_NO COAL_METAL_IND""".split()

STATES = ["WV", "KY", "PA", "WY", "NV", "AZ", "UT", "AL", "VA", "IL", "TX", "MN"]
MINE_TYPES = ["Surface", "Underground", "Facility"]
CANVASS = ["Coal", "Metal", "Nonmetal", "Stone", "Sand & gravel"]
OPERATORS = [f"Operator {c} Mining LLC" for c in "ABCDEFGHIJKLMNOP"]
EQUIPMENT = ["Front-end loader, tractor-shovel", "Truck, haulage", "Conveyor, belt", "Roof bolter",
             "Continuous miner", "Hand tools", "Scoop", "Not on list"]
ACC_TYPES = ["Struck by", "Caught in, under or between", "Fall to walkway or working surface",
             "Over-exertion in lifting objects", "Fall from machine", "Contact with hot objects"]
DEGREES = (["DAYS AWAY FROM WORK ONLY"] * 5 + ["DAYS RESTRICTED ACTIVITY ONLY"] * 3
           + ["NO DYS AWY FRM WRK,NO RSTR ACT"] * 3 + ["PERM TOT OR PERM PRTL DISABLTY", "FATALITY"])
BODY = ["Hand", "Back", "Knee", "Shoulder", "Fingers", "Ankle", "Eye"]
ACTIVITIES = [("E01", "Regular Inspection"), ("E02", "Spot Inspection"), ("E03", "Hazard Complaint")]


def fmt(d: dt.date) -> str:
    return d.strftime("%m/%d/%Y")


def rand_date(rng, start: dt.date, end: dt.date) -> dt.date:
    return start + dt.timedelta(days=rng.randint(0, (end - start).days))


def row(cols, values: dict) -> str:
    return "|".join(str(values.get(c, "")) for c in cols)


def write_zip(path: str, txt_name: str, cols, rows):
    buf = io.StringIO()
    buf.write("|".join(cols) + "\n")
    for r in rows:
        buf.write(row(cols, r) + "\n")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(txt_name, buf.getvalue().encode("latin-1"))


def build(seed: int, n_mines: int, as_of: dt.date):
    rng = random.Random(seed)
    start = dt.date(2022, 1, 1)
    mines = []
    for i in range(n_mines):
        mid = f"{rng.randint(100000, 4699999):07d}"
        canvass = rng.choice(CANVASS)
        mines.append({
            "MINE_ID": mid, "CURRENT_MINE_NAME": f"Sample Mine {i + 1}",
            "COAL_METAL_IND": "C" if canvass == "Coal" else "M",
            "CURRENT_MINE_TYPE": rng.choice(MINE_TYPES), "CURRENT_MINE_STATUS": rng.choice(["Active"] * 4 + ["Intermittent"]),
            "CURRENT_STATUS_DT": fmt(rand_date(rng, dt.date(2010, 1, 1), start)),
            "CURRENT_OPERATOR_ID": f"{rng.randint(1, 99999):07d}", "CURRENT_OPERATOR_NAME": rng.choice(OPERATORS),
            "STATE": rng.choice(STATES), "PRIMARY_CANVASS": canvass, "NO_EMPLOYEES": rng.randint(3, 600),
            "LATITUDE": f"{rng.uniform(30, 47):.5f}", "LONGITUDE": f"-{rng.uniform(80, 118):.5f}",
            "FIPS_CNTY_NM": f"County {rng.randint(1, 50)}",
        })
    inspections, violations, accidents = [], [], []
    for m in mines:
        risk = rng.random()  # some mines are much riskier than others
        for _ in range(rng.randint(2, 14)):
            begin = rand_date(rng, start, as_of - dt.timedelta(days=10))
            end = begin + dt.timedelta(days=rng.randint(0, 9))
            ev = f"{len(inspections) + 4000000}"
            code, act = rng.choice(ACTIVITIES)
            inspections.append({"EVENT_NO": ev, "MINE_ID": m["MINE_ID"], "INSPECTION_BEGIN_DT": fmt(begin),
                                "INSPECTION_END_DT": fmt(end), "CAL_YR": end.year, "CAL_QTR": (end.month - 1) // 3 + 1,
                                "ACTIVITY_CODE": code, "ACTIVITY": act, "PROGRAM_AREA": "Coal" if m["COAL_METAL_IND"] == "C" else "Metal/Non-Metal",
                                "NBR_INSPECTORS": rng.randint(1, 4), "TOTAL_ON_SITE_HOURS": rng.randint(2, 60),
                                "COAL_METAL_IND": m["COAL_METAL_IND"]})
            for _ in range(int(rng.expovariate(1 / (1 + 6 * risk)))):
                occ = begin + dt.timedelta(days=rng.randint(0, (end - begin).days))
                sig = "Y" if rng.random() < 0.15 + 0.4 * risk else "N"
                violations.append({"EVENT_NO": ev, "VIOLATION_NO": f"{len(violations) + 9000000}",
                                   "MINE_ID": m["MINE_ID"], "VIOLATION_OCCUR_DT": fmt(occ), "VIOLATION_ISSUE_DT": fmt(occ),
                                   "CAL_YR": occ.year, "CAL_QTR": (occ.month - 1) // 3 + 1, "SIG_SUB": sig,
                                   "PART_SECTION": rng.choice(["75.400", "75.202(a)", "56.14107(a)", "77.1605(b)", "56.12004"]),
                                   "LIKELIHOOD": rng.choice(["Unlikely", "Reasonably", "Highly"]),
                                   "INJ_ILLNESS": rng.choice(["NoLostDays", "LostDays", "Permanent"]),
                                   "NO_AFFECTED": rng.randint(1, 5), "NEGLIGENCE": rng.choice(["Low", "Moderate", "High"]),
                                   "PROPOSED_PENALTY": f"{rng.choice([138, 250, 400, 1500, 4800, 12000]) * (2 if sig == 'Y' else 1):.2f}",
                                   "AMOUNT_PAID": "", "CONTESTED_IND": rng.choice(["N"] * 6 + ["Y"]),
                                   "COAL_METAL_IND": m["COAL_METAL_IND"]})
        for _ in range(int(rng.expovariate(1 / (0.5 + 4 * risk)))):
            d = rand_date(rng, start, as_of - dt.timedelta(days=10))
            equip, atype, part = rng.choice(EQUIPMENT), rng.choice(ACC_TYPES), rng.choice(BODY)
            degree = rng.choice(DEGREES)
            accidents.append({"MINE_ID": m["MINE_ID"], "DOCUMENT_NO": f"2{d.strftime('%y%j')}{len(accidents):05d}",
                              "SUBUNIT": rng.choice(["UNDERGROUND", "STRIP, QUARY, OPEN PIT", "MILL OPERATION/PREPARATION PLANT"]),
                              "ACCIDENT_DT": fmt(d), "CAL_YR": d.year, "CAL_QTR": (d.month - 1) // 3 + 1,
                              "DEGREE_INJURY": degree, "MINING_EQUIP": equip, "ACCIDENT_TYPE": atype,
                              "CLASSIFICATION": rng.choice(["HANDLING OF MATERIALS", "POWERED HAULAGE", "MACHINERY", "SLIP OR FALL OF PERSON"]),
                              "NO_INJURIES": 1, "DAYS_LOST": 0 if "NO DYS" in degree else rng.randint(1, 120),
                              "DAYS_RESTRICT": rng.randint(0, 30), "INJ_BODY_PART": part,
                              "INJURY_SOURCE": rng.choice(["Machine", "Rock", "Hand tool", "Conveyor"]),
                              "NATURE_INJURY": rng.choice(["Sprain, strain", "Fracture", "Cut, laceration", "Contusion"]),
                              "OCCUPATION": rng.choice(["Loader operator", "Haul truck driver", "Mechanic", "Laborer"]),
                              "NARRATIVE": f"Employee was operating near the {equip.lower()} when {atype.lower()} occurred, injuring {part.lower()}.",
                              "COAL_METAL_IND": m["COAL_METAL_IND"]})
    return rng, mines, inspections, violations, accidents


def inject_defects(rng, mines, inspections, violations, accidents, as_of):
    """Add the kinds of problems real data has."""
    future = fmt(as_of + dt.timedelta(days=200))
    mines.append({**mines[0]})                                    # exact duplicate row
    mines.append({"MINE_ID": "", "CURRENT_MINE_NAME": "No ID Mine", "STATE": "WV"})   # missing key
    mines[1]["MINE_ID"] = mines[1]["MINE_ID"].lstrip("0") or "0"  # leading zeros lost (should be repaired)
    mines[2]["NO_EMPLOYEES"] = "unknown"                         # non-numeric number
    for i in range(5):
        inspections[i]["INSPECTION_END_DT"] = "01/01/2000"       # end before begin
    inspections[5]["INSPECTION_BEGIN_DT"] = "2023-13-45"         # malformed date
    for i in range(4):
        violations[i]["PROPOSED_PENALTY"] = "-250.00"            # negative penalty
    violations[4]["VIOLATION_OCCUR_DT"] = future                 # future date
    violations[5]["SIG_SUB"] = "?"                                # unknown flag (warning only)
    violations.append({**violations[6]})                          # duplicate
    for i in range(3):
        violations[10 + i]["MINE_ID"] = "9999999"                 # orphan mine (warning only)
    accidents[0]["ACCIDENT_DT"] = ""                              # missing date
    accidents[1]["DAYS_LOST"] = "-3"                              # negative days
    accidents[2]["NARRATIVE"] = ""                                # missing narrative (warning only)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="sample_data")
    p.add_argument("--mines", type=int, default=400)
    args = p.parse_args()

    week1, week2 = dt.date(2026, 10, 30), dt.date(2026, 11, 6)
    rng, mines, insp, viol, acc = build(seed=42, n_mines=args.mines, as_of=week1)
    batches = {}
    m1, i1, v1, a1 = [dict(x) for x in mines], [dict(x) for x in insp], [dict(x) for x in viol], [dict(x) for x in acc]
    inject_defects(rng, m1, i1, v1, a1, week1)
    batches[week1] = (m1, i1, v1, a1)

    # Week 2: same history, plus changes and new records.
    m2, i2, v2, a2 = [dict(x) for x in mines], [dict(x) for x in insp], [dict(x) for x in viol], [dict(x) for x in acc]
    for m in m2[:12]:                                             # 12 mines change operator
        m["CURRENT_OPERATOR_NAME"] = "Newco Resources Inc"
    for v in v2[:30]:                                             # 30 penalties get assessed/paid
        v["AMOUNT_PAID"] = v["PROPOSED_PENALTY"]
    _, new_m, new_i, new_v, new_a = build(seed=7, n_mines=15, as_of=week2)
    for m in new_m:
        m["CURRENT_MINE_NAME"] = m["CURRENT_MINE_NAME"].replace("Sample", "New")
    for x in new_i:
        x["EVENT_NO"] = str(int(x["EVENT_NO"]) + 500000)
    for x in new_v:
        x["VIOLATION_NO"] = str(int(x["VIOLATION_NO"]) + 500000)
        x["EVENT_NO"] = str(int(x["EVENT_NO"]) + 500000)
    for x in new_a:
        x["DOCUMENT_NO"] = "3" + x["DOCUMENT_NO"][1:]
    m2 += new_m; i2 += new_i; v2 += new_v; a2 += new_a
    inject_defects(rng, m2, i2, v2, a2, week2)
    batches[week2] = (m2, i2, v2, a2)

    for day, (m, i, v, a) in batches.items():
        out = os.path.join(args.out, day.isoformat())
        os.makedirs(out, exist_ok=True)
        write_zip(os.path.join(out, "Mines.zip"), "Mines.txt", MINES_COLS, m)
        write_zip(os.path.join(out, "Inspections.zip"), "Inspections.txt", INSPECTIONS_COLS, i)
        write_zip(os.path.join(out, "Violations.zip"), "Violations.txt", VIOLATIONS_COLS, v)
        write_zip(os.path.join(out, "Accidents.zip"), "Accidents.txt", ACCIDENTS_COLS, a)
        print(f"{out}: {len(m)} mines, {len(i)} inspections, {len(v)} violations, {len(a)} accidents")


if __name__ == "__main__":
    main()
