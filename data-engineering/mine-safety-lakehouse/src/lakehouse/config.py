"""Dataset definitions for the MSHA Open Government Data files.

Every MSHA file is a pipe-delimited text file inside a zip, with a header row.
Field names come from MSHA's published definition files. The pipeline checks
that each `required` column exists in the file header before loading, so if
MSHA renames a field, the run fails with a clear message instead of loading
bad data.
"""
from dataclasses import dataclass, field

MSHA_BASE_URL = "https://arlweb.msha.gov/OpenGovernmentData/DataSets"


@dataclass(frozen=True)
class Dataset:
    name: str                      # short name used for table names
    zip_name: str                  # file name on the MSHA site
    keys: tuple                    # columns that uniquely identify a record
    required: tuple                # columns the pipeline depends on
    date_cols: tuple = ()          # parsed to DATE in silver
    int_cols: tuple = ()           # cast to INT in silver
    decimal_cols: tuple = ()       # cast to DECIMAL(12,2) in silver
    keep: tuple = field(default_factory=tuple)  # columns carried into silver

    @property
    def url(self) -> str:
        return f"{MSHA_BASE_URL}/{self.zip_name}"


MINES = Dataset(
    name="mines",
    zip_name="Mines.zip",
    keys=("MINE_ID",),
    required=("MINE_ID", "CURRENT_MINE_NAME", "COAL_METAL_IND", "CURRENT_MINE_TYPE",
              "CURRENT_MINE_STATUS", "STATE", "CURRENT_OPERATOR_ID", "CURRENT_OPERATOR_NAME",
              "NO_EMPLOYEES", "PRIMARY_CANVASS", "LATITUDE", "LONGITUDE"),
    date_cols=("CURRENT_STATUS_DT",),
    int_cols=("NO_EMPLOYEES",),
    keep=("MINE_ID", "CURRENT_MINE_NAME", "COAL_METAL_IND", "CURRENT_MINE_TYPE",
          "CURRENT_MINE_STATUS", "CURRENT_STATUS_DT", "CURRENT_CONTROLLER_ID",
          "CURRENT_CONTROLLER_NAME", "CURRENT_OPERATOR_ID", "CURRENT_OPERATOR_NAME",
          "STATE", "FIPS_CNTY_NM", "PRIMARY_SIC", "PRIMARY_CANVASS", "NO_EMPLOYEES",
          "LATITUDE", "LONGITUDE"),
)

INSPECTIONS = Dataset(
    name="inspections",
    zip_name="Inspections.zip",
    keys=("EVENT_NO",),
    required=("EVENT_NO", "MINE_ID", "INSPECTION_BEGIN_DT", "INSPECTION_END_DT",
              "ACTIVITY_CODE", "ACTIVITY"),
    date_cols=("INSPECTION_BEGIN_DT", "INSPECTION_END_DT"),
    int_cols=("CAL_YR", "CAL_QTR", "NBR_INSPECTORS", "TOTAL_ON_SITE_HOURS"),
    keep=("EVENT_NO", "MINE_ID", "INSPECTION_BEGIN_DT", "INSPECTION_END_DT", "CAL_YR",
          "CAL_QTR", "ACTIVITY_CODE", "ACTIVITY", "PROGRAM_AREA", "NBR_INSPECTORS",
          "TOTAL_ON_SITE_HOURS", "COAL_METAL_IND"),
)

VIOLATIONS = Dataset(
    name="violations",
    zip_name="Violations.zip",
    keys=("VIOLATION_NO",),
    required=("VIOLATION_NO", "EVENT_NO", "MINE_ID", "VIOLATION_OCCUR_DT", "SIG_SUB",
              "PART_SECTION", "NEGLIGENCE", "PROPOSED_PENALTY"),
    date_cols=("VIOLATION_OCCUR_DT", "VIOLATION_ISSUE_DT"),
    int_cols=("CAL_YR", "CAL_QTR", "NO_AFFECTED"),
    decimal_cols=("PROPOSED_PENALTY", "AMOUNT_PAID"),
    keep=("VIOLATION_NO", "EVENT_NO", "MINE_ID", "VIOLATION_OCCUR_DT", "VIOLATION_ISSUE_DT",
          "CAL_YR", "CAL_QTR", "SIG_SUB", "PART_SECTION", "LIKELIHOOD", "INJ_ILLNESS",
          "NO_AFFECTED", "NEGLIGENCE", "PROPOSED_PENALTY", "AMOUNT_PAID", "CONTESTED_IND",
          "COAL_METAL_IND"),
)

ACCIDENTS = Dataset(
    name="accidents",
    zip_name="Accidents.zip",
    keys=("DOCUMENT_NO",),
    required=("DOCUMENT_NO", "MINE_ID", "ACCIDENT_DT", "ACCIDENT_TYPE", "DEGREE_INJURY",
              "NO_INJURIES", "DAYS_LOST", "MINING_EQUIP", "NARRATIVE"),
    date_cols=("ACCIDENT_DT", "RETURN_TO_WORK_DT"),
    int_cols=("CAL_YR", "CAL_QTR", "NO_INJURIES", "DAYS_LOST", "DAYS_RESTRICT"),
    keep=("DOCUMENT_NO", "MINE_ID", "ACCIDENT_DT", "CAL_YR", "CAL_QTR", "SUBUNIT",
          "DEGREE_INJURY", "CLASSIFICATION", "ACCIDENT_TYPE", "MINING_EQUIP",
          "INJURY_SOURCE", "NATURE_INJURY", "INJ_BODY_PART", "OCCUPATION", "NO_INJURIES",
          "DAYS_LOST", "DAYS_RESTRICT", "NARRATIVE", "COAL_METAL_IND"),
)

DATASETS = {d.name: d for d in (MINES, INSPECTIONS, VIOLATIONS, ACCIDENTS)}
