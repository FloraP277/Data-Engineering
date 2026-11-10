"""Data quality rules.

Each rule is a SQL condition that is TRUE when a row FAILS the rule.

- `error` rules: the row is moved to a quarantine table and kept out of silver.
- `warn` rules: the row is loaded, and the failure is counted in dq_results.

Keeping failed rows in quarantine (instead of dropping them) means nothing is
lost silently: every rejected record can be inspected and reprocessed.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Rule:
    name: str
    fails_when: str
    severity: str  # "error" or "warn"


_FUTURE = "> current_date()"

RULES = {
    "mines": [
        Rule("mine_id_missing_or_malformed", "MINE_ID IS NULL OR NOT MINE_ID RLIKE '^[0-9]{7}$'", "error"),
        Rule("state_malformed", "STATE IS NULL OR NOT STATE RLIKE '^[A-Z]{2}$'", "warn"),
        Rule("employees_negative", "NO_EMPLOYEES < 0", "error"),
    ],
    "inspections": [
        Rule("event_no_missing", "EVENT_NO IS NULL", "error"),
        Rule("mine_id_missing", "MINE_ID IS NULL", "error"),
        Rule("begin_date_missing", "INSPECTION_BEGIN_DT IS NULL", "error"),
        Rule("end_before_begin", "INSPECTION_END_DT < INSPECTION_BEGIN_DT", "error"),
        Rule("begin_date_in_future", f"INSPECTION_BEGIN_DT {_FUTURE}", "error"),
    ],
    "violations": [
        Rule("violation_no_missing", "VIOLATION_NO IS NULL", "error"),
        Rule("mine_id_missing", "MINE_ID IS NULL", "error"),
        Rule("occur_date_missing", "VIOLATION_OCCUR_DT IS NULL", "error"),
        Rule("occur_date_in_future", f"VIOLATION_OCCUR_DT {_FUTURE}", "error"),
        Rule("penalty_negative", "PROPOSED_PENALTY < 0", "error"),
        Rule("sig_sub_not_y_n", "SIG_SUB IS NULL OR SIG_SUB NOT IN ('Y', 'N')", "warn"),
    ],
    "accidents": [
        Rule("document_no_missing", "DOCUMENT_NO IS NULL", "error"),
        Rule("mine_id_missing", "MINE_ID IS NULL", "error"),
        Rule("accident_date_missing", "ACCIDENT_DT IS NULL", "error"),
        Rule("accident_date_in_future", f"ACCIDENT_DT {_FUTURE}", "error"),
        Rule("injuries_negative", "NO_INJURIES < 0", "error"),
        Rule("days_lost_negative", "DAYS_LOST < 0", "error"),
        Rule("narrative_missing", "NARRATIVE IS NULL", "warn"),
    ],
}

# Records that point to a mine not in silver_mines. Loaded (MSHA data can
# legitimately reference mines outside the current extract) but counted.
ORPHAN_RULE = Rule("mine_not_in_mines_table", "_mine_found IS NULL", "warn")
