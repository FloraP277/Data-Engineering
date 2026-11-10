"""SYNTHETIC hospital files with CMS-style headers, for tests and offline demos.

Three releases. Between releases some hospitals gain or lose a star, change
ownership or type, a few close and a few open, so the SCD Type 2 history has
real changes to track. Names and facility IDs are invented.

    python scripts/make_sample_data.py --out sample_data
"""
import argparse
import csv
import os
import random

HEADER = ["Facility ID", "Facility Name", "Address", "City/Town", "State", "ZIP Code", "County/Parish",
          "Telephone Number", "Hospital Type", "Hospital Ownership", "Emergency Services",
          "Hospital overall rating", "Hospital overall rating footnote"]
STATES = ["MD", "VA", "DC", "PA", "NY", "TX", "CA", "OH"]
TYPES = ["Acute Care Hospitals", "Critical Access Hospitals", "Childrens", "Psychiatric"]
OWNERS = ["Voluntary non-profit - Private", "Proprietary", "Government - Local", "Government - State"]


def releases(n=120, seed=11):
    rng = random.Random(seed)
    base = []
    for i in range(n):
        st = rng.choice(STATES)
        base.append({
            "Facility ID": f"{rng.randint(10, 99)}{i:04d}", "Facility Name": f"Sample General Hospital {i + 1}",
            "Address": f"{rng.randint(1, 999)} Main St", "City/Town": f"City {rng.randint(1, 30)}", "State": st,
            "ZIP Code": f"{rng.randint(10000, 99999)}", "County/Parish": f"County {rng.randint(1, 20)}",
            "Telephone Number": "(555) 555-0100", "Hospital Type": rng.choice(TYPES),
            "Hospital Ownership": rng.choice(OWNERS), "Emergency Services": rng.choice(["Yes", "No"]),
            "Hospital overall rating": rng.choice(["1", "2", "3", "3", "4", "4", "5", "Not Available"]),
            "Hospital overall rating footnote": "",
        })
    r1 = [dict(h) for h in base]
    r2 = [dict(h) for h in base]
    for h in rng.sample(r2, 15):              # ratings move
        if h["Hospital overall rating"].isdigit():
            h["Hospital overall rating"] = str(min(5, max(1, int(h["Hospital overall rating"]) + rng.choice([-1, 1]))))
    for h in rng.sample(r2, 4):               # ownership changes
        h["Hospital Ownership"] = "Proprietary"
    r3 = [dict(h) for h in r2][:-3]          # three hospitals close
    for h in rng.sample(r3, 10):
        if h["Hospital overall rating"].isdigit():
            h["Hospital overall rating"] = str(min(5, max(1, int(h["Hospital overall rating"]) + rng.choice([-1, 1]))))
    r3 += [{**dict(base[0]), "Facility ID": f"99{900 + k}", "Facility Name": f"New Community Hospital {k}"}
           for k in range(2)]                # two open
    return {"2026-01-28": r1, "2026-04-29": r2, "2026-07-30": r3}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="sample_data")
    args = p.parse_args()
    os.makedirs(args.out, exist_ok=True)
    for day, rows in releases().items():
        path = os.path.join(args.out, f"Hospital_General_Information_{day}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, HEADER)
            w.writeheader()
            w.writerows(rows)
        print(f"{path}: {len(rows)} hospitals")


if __name__ == "__main__":
    main()
