#!/usr/bin/env python3
"""
Debug pattern matching
"""

import re

sql = """declare @DropDb boolean = 1;
-- Set this to 0 to skip DROP statements, 1 to include them
if @DropDb = 1
begin
drop index if exists IDX_QRTZ_T_J;
drop index if exists IDX_QRTZ_T_JG;
drop index if exists IDX_QRTZ_T_C;
drop index if exists IDX_QRTZ_T_G;

ALTER TABLE qrtz_triggers DROP CONSTRAINT IF EXISTS fk_qrtz_triggers_qrtz_job_details;
ALTER TABLE qrtz_cron_triggers DROP CONSTRAINT IF EXISTS fk_qrtz_cron_triggers_qrtz_triggers;

drop table QRTZ_CALENDARS;
drop table QRTZ_CRON_TRIGGERS;
drop table QRTZ_BLOB_TRIGGERS;
drop table QRTZ_FIRED_TRIGGERS;
end"""

# My pattern
pattern = r'(declare\s+@(\w+)\s+(\w+)(?:\(\d+\))?\s*=\s*([^;]+);)\s*(?:--[^\n]*)?\s*(if\s+@\w+\s*[=<>]+\s*\d+\s*begin\s*(.*?)\s*end)(?!\s+if)'

matches = list(re.finditer(pattern, sql, flags=re.DOTALL | re.IGNORECASE | re.MULTILINE))
print(f"Matches found: {len(matches)}")

for i, match in enumerate(matches):
    print(f"\n=== MATCH {i+1} ===")
    print(f"Full match: {match.group(0)[:100]}...")
    print(f"Group 1 (declare): {match.group(1)}")
    print(f"Group 2 (var_name): {match.group(2)}")
    print(f"Group 3 (var_type): {match.group(3)}")
    print(f"Group 4 (var_value): {match.group(4)}")
    print(f"Group 5 (if_stmt): {match.group(5)[:50]}...")
    body = match.group(6)
    print(f"\nFull Body:")
    print(body)
    print(f"Body len: {len(body)}")
