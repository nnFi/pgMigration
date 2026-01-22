"""
Schritt 2: Spalten verifizieren
Prüft ob alle Spalten aus MSSQL in PostgreSQL erstellt wurden
"""

import pyodbc
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime
import sys
import json
import re

# Logging Setup
class Logger:
    # Log level hierarchy
    LOG_LEVELS = {'DEBUG': 0, 'INFO': 1, 'WARNING': 2, 'ERROR': 3}
    
    def __init__(self, filename):
        from pathlib import Path
        self.terminal = sys.stdout
        self.filename = filename
        # Erstelle logs Verzeichnis
        Path(filename).parent.mkdir(exist_ok=True)
        self.log_file = open(filename, 'w', encoding='utf-8')
        
        # Lese LOG_LEVEL aus Umgebung (Standard: INFO)
        log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
        self.log_level = self.LOG_LEVELS.get(log_level_str, 1)
    
    def _ensure_open(self):
        """Stelle sicher dass die Datei offen ist"""
        if self.log_file.closed:
            self.log_file = open(self.filename, 'a', encoding='utf-8')
    
    def write(self, message):
        self._ensure_open()
        self.log_file.write(message)
        self.log_file.flush()
    
    def flush(self):
        if not self.log_file.closed:
            self.log_file.flush()
    
    def close(self):
        if not self.log_file.closed:
            self.log_file.close()
    
    def detail(self, message, level='INFO'):
        """Schreibe ins Log wenn Level passt"""
        message_level = self.LOG_LEVELS.get(level.upper(), 1)
        if message_level >= self.log_level:
            self._ensure_open()
            self.log_file.write(message + '\n')
            self.log_file.flush()
    
    def summary(self, message):
        """Schreibe ins Terminal und Log"""
        self.terminal.write(message + '\n')
        self.terminal.flush()
        self._ensure_open()
        self.log_file.write(message + '\n')
        self.log_file.flush()

# Aktiviere Logging mit Timestamp
run_dir = os.getenv('MIGRATION_RUN_DIR', 'logs')
logger = Logger(f'{run_dir}/step2_debug.log')

# Globale print-Funktionen
def print_detail(msg, level='DEBUG'):
    logger.detail(str(msg), level=level)

def print_summary(msg):
    logger.summary(str(msg))

# Lade Umgebungsvariablen
load_dotenv()

# MSSQL Konfiguration
MSSQL_SERVER = os.getenv('MSSQL_SERVER')
MSSQL_PORT = os.getenv('MSSQL_PORT', '')
MSSQL_DATABASE = os.getenv('MSSQL_DATABASE')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')

# PostgreSQL Konfiguration
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# Optionen - keine mehr nötig

def normalize_name(name):
    """Normalisiere Namen: Ersetze - durch _ und konvertiere zu lowercase wenn NORMALIZE_COLUMNS aktiviert"""
    # Prüfe ob Normalisierung aktiviert war (dann sind Tabellen lowercase in PostgreSQL)
    normalize_enabled = os.getenv('NORMALIZE_COLUMNS', '').lower() == 'true'
    
    normalized = name.replace('-', '_')
    
    if normalize_enabled:
        # Wenn Normalisierung aktiv war, sind Tabellen lowercase in PostgreSQL
        return normalized.lower()
    else:
        # Ohne Normalisierung: Case beibehalten
        return normalized

def connect_mssql():
    """Verbindung zu MSSQL herstellen"""
    try:
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={MSSQL_SERVER}'
        )
        if MSSQL_PORT:
            conn_str += f',{MSSQL_PORT}'
        conn_str += (
            f';DATABASE={MSSQL_DATABASE};'
            f'UID={MSSQL_USER};'
            f'PWD={MSSQL_PASSWORD}'
        )
        conn = pyodbc.connect(conn_str)
        print_summary(f"Verbindung zu MSSQL erfolgreich: {MSSQL_DATABASE}")
        return conn
    except Exception as e:
        print_summary(f"FEHLER: Fehler bei MSSQL-Verbindung: {e}")
        sys.exit(1)

def connect_postgresql():
    """Verbindung zu PostgreSQL herstellen"""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        print_summary(f"Verbindung zu PostgreSQL erfolgreich: {PG_DATABASE}")
        return conn
    except Exception as e:
        print_summary(f"FEHLER: Fehler bei PostgreSQL-Verbindung: {e}")
        sys.exit(1)

def load_column_mapping(filename='logs/column_mapping.json'):
    """Lade Spalten-Mapping aus JSON-Datei"""
    try:
        if os.path.exists(filename):
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print_detail(f"  Warnung: Konnte Mapping nicht laden: {e}", level='WARNING')
    return {}

def map_schema_name(mssql_schema):
    """Mappe MSSQL Schema zu PostgreSQL Schema"""
    # dbo wird zu public
    if mssql_schema.lower() == 'dbo':
        return 'public'
    return mssql_schema

def get_mssql_tables(mssql_conn):
    """Hole alle Tabellen aus MSSQL"""
    cursor = mssql_conn.cursor()
    query = """
        SELECT TABLE_SCHEMA, TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    cursor.execute(query)
    tables = [(row.TABLE_SCHEMA, row.TABLE_NAME) for row in cursor.fetchall()]
    cursor.close()
    return tables

def get_mssql_columns(mssql_conn, schema, table):
    """Hole alle Spalten einer MSSQL Tabelle"""
    cursor = mssql_conn.cursor()
    query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query, (schema, table))
    columns = {}
    
    for row in cursor.fetchall():
        col_name = row.COLUMN_NAME
        
        # Spalten sind in PostgreSQL IMMER lowercase (werden während CREATE TABLE konvertiert)
        col_name = col_name.lower()
        
        columns[col_name] = {
            'data_type': row.DATA_TYPE,
            'max_length': row.CHARACTER_MAXIMUM_LENGTH,
            'precision': row.NUMERIC_PRECISION,
            'scale': row.NUMERIC_SCALE,
            'is_nullable': row.IS_NULLABLE
        }
    cursor.close()
    return columns

def get_postgres_columns(pg_conn, schema, table):
    """Hole alle Spalten einer PostgreSQL Tabelle"""
    cursor = pg_conn.cursor()
    
    # Mappe Schema (dbo -> public) und dann normalisiere
    pg_schema = map_schema_name(schema)
    normalized_schema = normalize_name(pg_schema)
    normalized_table = normalize_name(table)
    
    query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    cursor.execute(query, (normalized_schema, normalized_table))
    columns = {}
    for row in cursor.fetchall():
        col_name = row[0]
        
        # Wenn Spalten-Normalisierung aktiviert ist, behalte die normalisierten Namen
        # (Diese sollten bereits in PostgreSQL klein geschrieben sein)
        
        columns[col_name] = {
            'data_type': row[1],
            'max_length': row[2],
            'precision': row[3],
            'scale': row[4],
            'is_nullable': row[5]
        }
    cursor.close()
    return columns

def get_mssql_row_count(mssql_conn, schema, table):
    """Hole Zeilenanzahl aus MSSQL"""
    cursor = mssql_conn.cursor()
    cursor.execute(f'SELECT COUNT(*) FROM [{schema}].[{table}]')
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def get_postgres_row_count(pg_conn, schema, table):
    """Hole Zeilenanzahl aus PostgreSQL"""
    cursor = pg_conn.cursor()
    
    # Mappe Schema (dbo -> public) und dann normalisiere
    pg_schema = map_schema_name(schema)
    normalized_schema = normalize_name(pg_schema)
    normalized_table = normalize_name(table)
    
    # SQL mit Quotes
    query = f'SELECT COUNT(*) FROM "{normalized_schema}"."{normalized_table}"'
    
    cursor.execute(query)
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def check_table_exists(pg_conn, schema, table):
    """Prüfe ob Tabelle in PostgreSQL existiert"""
    cursor = pg_conn.cursor()
    
    # Mappe Schema (dbo -> public) und dann normalisiere
    pg_schema = map_schema_name(schema)
    normalized_schema = normalize_name(pg_schema)
    normalized_table = normalize_name(table)
    
    query = """
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        )
    """
    cursor.execute(query, (normalized_schema, normalized_table))
    exists = cursor.fetchone()[0]
    
    # Wenn nicht gefunden und es eine Quartz-Tabelle ist, prüfe auch kleingeschriebene Version
    if not exists and table.upper().startswith('QRTZ_'):
        lowercase_table = table.lower()
        cursor.execute(query, (normalized_schema, lowercase_table))
        exists = cursor.fetchone()[0]
    
    cursor.close()
    return exists

def verify_table(mssql_conn, pg_conn, schema, table):
    """Verifiziere eine einzelne Tabelle"""
    print_detail(f"\nVerifiziere: {schema}.{table}", level='INFO')
    
    # Lade Spalten-Mapping
    column_mapping = load_column_mapping()
    table_key = f"{schema}.{table}"
    table_mapping = column_mapping.get(table_key, {})
    
    issues = []
    
    # Prüfe ob Tabelle existiert (Funktionen machen Schema-Mapping intern)
    try:
        if not check_table_exists(pg_conn, schema, table):
            pg_schema = map_schema_name(schema)
            print_summary(f"  FEHLER: Tabelle existiert nicht in PostgreSQL (Schema: {pg_schema})")
            return ['Tabelle fehlt']
    except Exception as e:
        pg_conn.rollback()  # Rollback bei Fehler
        print_summary(f"  FEHLER: Fehler beim Prüfen der Tabelle: {e}")
        return [f'Fehler beim Prüfen: {e}']
    
    print_detail(f"  Tabelle existiert", level='INFO')
    
    # Hole Spalten (Funktionen machen Schema-Mapping intern)
    mssql_cols = get_mssql_columns(mssql_conn, schema, table)
    pg_cols = get_postgres_columns(pg_conn, schema, table)
    
    # Prüfe Spaltenanzahl
    if len(mssql_cols) != len(pg_cols):
        msg = f"Spaltenanzahl unterschiedlich: MSSQL={len(mssql_cols)}, PostgreSQL={len(pg_cols)}"
        print_detail(f"  Warnung: {msg}", level='WARNING')
        issues.append(msg)
    else:
        print_detail(f"  Spaltenanzahl korrekt: {len(mssql_cols)}", level='INFO')
    
    # Prüfe jede Spalte
    missing_cols = []
    for col_name, col_info in mssql_cols.items():
        # col_name ist bereits lowercase (aus get_mssql_columns)
        # Aber das Mapping hat Original-Namen (mit CamelCase) als Keys
        # Wir müssen das Mapping mit Original-Namen durchsuchen
        
        # Finde Original-Namen im Mapping (case-insensitive Suche)
        original_col_name = None
        for mapping_key in table_mapping.keys():
            if mapping_key.lower() == col_name:
                original_col_name = mapping_key
                break
        
        # Wenn Mapping gefunden, nutze den gekürzten Namen
        if original_col_name and original_col_name in table_mapping:
            mapped_name = table_mapping[original_col_name]
            # Konvertiere gekürzten Namen auch zu lowercase
            pg_col_name = normalize_name(mapped_name.lower())
        else:
            # Kein Mapping, nutze col_name direkt (bereits lowercase)
            pg_col_name = normalize_name(col_name)
        
        if pg_col_name not in pg_cols:
            missing_cols.append(col_name)
    
    if missing_cols:
        msg = f"Fehlende Spalten: {', '.join(missing_cols)}"
        print_detail(f"  Fehler: {msg}", level='ERROR')
        issues.append(msg)
    else:
        if table_mapping:
            print_detail(f"  Alle Spalten vorhanden ({len(table_mapping)} gekürzt)", level='INFO')
        else:
            print_detail(f"  Alle Spalten vorhanden", level='INFO')
    
    # Prüfe Zeilenanzahl (nur wenn MIGRATE_DATA aktiviert ist)
    migrate_data = os.getenv('MIGRATE_DATA', 'true').lower() == 'true'
    
    if migrate_data:
        try:
            mssql_count = get_mssql_row_count(mssql_conn, schema, table)
            pg_count = get_postgres_row_count(pg_conn, schema, table)
            
            if mssql_count != pg_count:
                msg = f"Zeilenanzahl unterschiedlich: MSSQL={mssql_count}, PostgreSQL={pg_count}"
                print_detail(f"  Warnung: {msg}", level='WARNING')
                issues.append(msg)
            else:
                print_detail(f"  Zeilenanzahl korrekt: {pg_count}", level='INFO')
        except Exception as e:
            pg_conn.rollback()  # Rollback bei Fehler
            msg = f"Fehler beim Zählen der Zeilen: {e}"
            print_detail(f"  Fehler: {msg}", level='ERROR')
            issues.append(msg)
    else:
        print_detail(f"  Zeilenanzahl-Prüfung übersprungen (MIGRATE_DATA=false)", level='INFO')
    
    return issues

def main():
    """Hauptfunktion"""
    print_summary("=" * 60)
    print_summary("SCHRITT 2: SPALTEN VERIFIZIEREN")
    print_summary("=" * 60)
    print_summary("")
    
    start_time = datetime.now()
    
    # Verbindungen herstellen
    mssql_conn = connect_mssql()
    pg_conn = connect_postgresql()
    
    try:
        # Hole alle Tabellen
        print_summary("\nHole Tabellenliste aus MSSQL...")
        tables = get_mssql_tables(mssql_conn)
        print_summary(f"Zu prüfende Tabellen: {len(tables)}")
        
        # Verifiziere jede Tabelle
        all_issues = {}
        tables_ok = 0
        tables_with_issues = 0
        
        for schema, table in tables:
            issues = verify_table(mssql_conn, pg_conn, schema, table)
            if issues:
                all_issues[f"{schema}.{table}"] = issues
                tables_with_issues += 1
            else:
                tables_ok += 1
        
        # Zusammenfassung
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print_detail("")
        print_detail("=" * 60, level='INFO')
        print_summary("ZUSAMMENFASSUNG")
        print_detail("=" * 60, level='INFO')
        print_summary(f"Tabellen geprüft: {len(tables)}")
        print_summary(f"  Ohne Probleme: {tables_ok}")
        print_summary(f"  Mit Problemen: {tables_with_issues}")
        
        if all_issues:
            print_detail("")
            print_summary("PROBLEME GEFUNDEN:")
            print_detail("-" * 60, level='INFO')
            for table, issues in all_issues.items():
                print_detail(f"\n{table}:", level='WARNING')
                for issue in issues:
                    print_detail(f"  - {issue}", level='DEBUG')
        else:
            print_detail("")
            print_summary("Alle Tabellen erfolgreich verifiziert!")
        
        print_detail("", level='DEBUG')
        print_summary(f"Dauer: {duration:.2f} Sekunden")
        print_detail("", level='DEBUG')
        
        # Merke ob Fehler aufgetreten sind
        has_errors = tables_with_issues > 0
        
    finally:
        mssql_conn.close()
        pg_conn.close()
        print_summary("Verbindungen geschlossen.")
        logger.close()
    
    # Exit mit entsprechendem Code (außerhalb try/finally)
    if has_errors:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
