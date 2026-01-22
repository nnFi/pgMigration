#!/usr/bin/env python3
"""
Debug pattern matching - test word boundaries
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

# Test different patterns
print("=== TESTING PATTERNS ===\n")

# Pattern 1: Original
pattern1 = r'(declare\s+@(\w+)\s+(\w+)(?:\(\d+\))?\s*=\s*([^;]+);)\s*(?:--[^\n]*)?\s*(if\s+@\w+\s*[=<>]+\s*\d+\s*begin\s*(.*?)\s*end)(?!\s+IF)'
print("Pattern 1 (original):")
matches1 = list(re.finditer(pattern1, sql, flags=re.DOTALL | re.IGNORECASE | re.MULTILINE))
if matches1:
    body1 = matches1[0].group(6)
    print(f"Body ends with: ...{body1[-30:]}")
    print(f"Last 5 chars: {repr(body1[-5:])}")
    print(f"Length: {len(body1)}\n")

# Pattern 2: With word boundary
pattern2 = r'(declare\s+@(\w+)\s+(\w+)(?:\(\d+\))?\s*=\s*([^;]+);)\s*(?:--[^\n]*)?\s*(if\s+@\w+\s*[=<>]+\s*\d+\s*begin\s*(.*?)\s*\bend\b)(?!\s+IF)'
print("Pattern 2 (with word boundary on end):")
matches2 = list(re.finditer(pattern2, sql, flags=re.DOTALL | re.IGNORECASE | re.MULTILINE))
if matches2:
    body2 = matches2[0].group(6)
    print(f"Body ends with: ...{body2[-30:]}")
    print(f"Last 5 chars: {repr(body2[-5:])}")
    print(f"Length: {len(body2)}\n")

# Show the actual character that's at position 343 in the original
print(f"Character at position 343 in sql: {repr(sql[343:350])}")
