-- Queries for the Databricks dashboard. Replace workspace.mine_safety if you
-- used a different catalog or schema.

-- 1. Injuries and S&S violations per year, coal vs metal/non-metal
SELECT year, coal_metal,
       SUM(injuries)       AS injuries,
       SUM(ss_violations)  AS ss_violations,
       SUM(days_lost)      AS days_lost
FROM workspace.mine_safety.gold_state_safety_trends
GROUP BY year, coal_metal
ORDER BY year, coal_metal;

-- 2. States ranked by injuries in the latest full year
SELECT state, SUM(injuries) AS injuries, SUM(mines_with_activity) AS mines,
       ROUND(SUM(injuries) * 100.0 / NULLIF(SUM(employees_current), 0), 2) AS injuries_per_100_employees_approx
FROM workspace.mine_safety.gold_state_safety_trends
WHERE year = (SELECT MAX(year) - 1 FROM workspace.mine_safety.gold_state_safety_trends)
GROUP BY state
ORDER BY injuries DESC
LIMIT 15;

-- 3. Repeat violators: mines with S&S violations in 3 of the last 4 quarters
SELECT rank, MINE_ID, mine_name, state, mine_type, operator_name,
       quarters_with_ss, ss_violations, proposed_penalty, injuries
FROM workspace.mine_safety.gold_repeat_violators
ORDER BY rank;

-- 4. Which equipment is involved in the most serious accidents
SELECT equipment,
       SUM(accidents)          AS accidents,
       SUM(serious_accidents)  AS serious_accidents,
       ROUND(SUM(serious_accidents) * 100.0 / SUM(accidents), 1) AS serious_pct
FROM workspace.mine_safety.gold_equipment_accidents
GROUP BY equipment
HAVING SUM(accidents) >= 20
ORDER BY serious_accidents DESC;

-- 5. Data quality: failure rate per rule in the latest load
SELECT dataset, rule, severity, failed, total, failed_pct
FROM workspace.mine_safety.dq_results
WHERE load_date = (SELECT MAX(load_date) FROM workspace.mine_safety.dq_results)
  AND failed > 0
ORDER BY severity, failed DESC;
