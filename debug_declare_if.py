#!/usr/bin/env python3
"""Debug the declare if pattern"""
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

# Pattern from the code
pattern = r'declare\s+@(\w+)\s+boolean\s*=\s*1;.*?if\s+@\w+\s*=\s*1\s*begin\s+((?:(?:drop|alter|create)\s+(?:table|index|constraint).*?;(?:\s|$))+)\s*end'

matches = list(re.finditer(pattern, sql, flags=re.DOTALL | re.IGNORECASE))
print(f"Matches found: {len(matches)}")

for i, m in enumerate(matches):
    print(f"\nMatch {i+1}:")
    print(f"Body (first 200 chars): {m.group(1)[:200]}")
    print(f"Body length: {len(m.group(1))}")
