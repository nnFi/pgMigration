#!/usr/bin/env python3
"""
Test für die große Quartz-Migration
"""

from flyway_converter import FlywayConverter

# Das ECHTE große Script vom User
big_script = """
declare @DropDb boolean = 1;
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
end

CREATE TABLE QRTZ_CALENDARS (
    SCHED_NAME varchar(120) not null,
    CALENDAR_NAME varchar(200) not null,
    CALENDAR bytea(max) not null
);

CREATE TABLE QRTZ_CRON_TRIGGERS (
    SCHED_NAME varchar(120) not null,
    TRIGGER_NAME varchar(150) not null,
    TRIGGER_GROUP varchar(150) not null,
    CRON_EXPRESSION varchar(120) not null,
    TIME_ZONE_ID varchar(80)
);
"""

print("="*70)
print("BIG QUARTZ SCHEDULER MIGRATION TEST")
print("="*70)

converter = FlywayConverter()
result, log = converter.convert_file(big_script)

print("\nCONVERTED OUTPUT:")
print("="*70)
print(result)

print("\n" + "="*70)
print("CHECKS:")
print("="*70)

checks = [
    ("-- Drop indexes" in result or "-- drop indexes" in result, "Has index comment"),
    ("-- Drop constraints" in result or "-- drop constraints" in result, "Has constraint comment"),
    ("-- Drop tables" in result or "-- drop tables" in result, "Has table comment"),
    ("DROP TABLE IF EXISTS" in result or "drop table if exists" in result, "DROP TABLE has IF EXISTS"),
    ("cascade" in result.lower(), "DROP TABLE has CASCADE"),
    ("BYTEA NOT NULL" not in result, "BYTEA(max) converted"),
    ("qrtz_calendars" in result.lower(), "Table names lowercase"),
    ("DO $$" in result, "Has DO block"),
    ("IF dropdb THEN" in result or "if dropdb then" in result.lower(), "IF var THEN syntax"),
]

for passed, msg in checks:
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {msg}")

print("\n" + "="*70)
print("LOG:")
print("="*70)
import sys
for entry in log:
    # Handle unicode safely for Windows console
    try:
        sys.stdout.write(f"  {entry}\n")
    except:
        # If unicode fails, use ASCII representation
        sys.stdout.write(f"  {repr(entry)}\n")
