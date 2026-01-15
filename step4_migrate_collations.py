"""
Schritt 4: Collations migrieren (Optional)
Migriert Collations von MSSQL nach PostgreSQL
"""

import pyodbc
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime
import sys
import json

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
logger = Logger(f'{run_dir}/step4_debug.log')

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
    """Normalisiere Namen: Ersetze - durch _"""
    # Ersetze - durch _
    return name.replace('-', '_')

# Mapping von MSSQL Collations zu PostgreSQL Collations
# Mehrere Optionen pro MSSQL-Collation, erste verfügbare wird verwendet
COLLATION_MAPPING = {
    'SQL_Latin1_General_CP1_CI_AS': ['de-DE-x-icu', 'de_DE.utf8', 'de_DE', 'en-US-x-icu', 'en_US.utf8', 'C.UTF-8', 'default'],
    'Latin1_General_CI_AS': ['de-DE-x-icu', 'de_DE.utf8', 'de_DE', 'en-US-x-icu', 'en_US.utf8', 'C.UTF-8', 'default'],
    'SQL_Latin1_General_CP1_CS_AS': ['C'],
    'Latin1_General_CS_AS': ['C'],
    'German_PhoneBook_CI_AS': ['de-DE-x-icu', 'de_DE.utf8', 'de_DE', 'default'],
    'SQL_Latin1_General_CP850_CI_AS': ['de-DE-x-icu', 'de_DE.utf8', 'de_DE', 'en-US-x-icu', 'en_US.utf8', 'default'],
}

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
        print_detail(f"Fehler bei MSSQL-Verbindung: {e}", level='ERROR')
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
        print_detail(f"Fehler bei PostgreSQL-Verbindung: {e}", level='ERROR')
        sys.exit(1)

def get_column_collations(mssql_conn):
    """Hole alle Spalten mit Collations aus MSSQL"""
    cursor = mssql_conn.cursor()
    query = """
        SELECT 
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.COLLATION_NAME,
            c.DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.COLLATION_NAME IS NOT NULL
        ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
    """
    cursor.execute(query)
    columns = []
    for row in cursor.fetchall():
        columns.append({
            'schema': row.TABLE_SCHEMA,
            'table': row.TABLE_NAME,
            'column': row.COLUMN_NAME,
            'collation': row.COLLATION_NAME,
            'data_type': row.DATA_TYPE
        })
    cursor.close()
    return columns

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
def map_collation(mssql_collation, available_collations):
    """Mappe MSSQL Collation zu PostgreSQL Collation - wähle erste verfügbare"""
    # Versuche direktes Mapping mit Fallback-Liste
    if mssql_collation in COLLATION_MAPPING:
        candidates = COLLATION_MAPPING[mssql_collation]
        # Wenn es eine Liste ist, prüfe welche verfügbar ist
        if isinstance(candidates, list):
            for candidate in candidates:
                if candidate == 'default' or candidate in available_collations:
                    return candidate
            # Fallback wenn nichts gefunden
            return 'default'
        else:
            # Einzelner Wert (z.B. 'C')
            return candidates if candidates in available_collations else 'default'
    
    # Fallback für unbekannte Collations
    if 'CI' in mssql_collation:
        # Case-insensitive: bevorzuge deutsche Locales, dann andere
        for candidate in ['de-DE-x-icu', 'de_DE.utf8', 'de_DE', 'en-US-x-icu', 'en_US.utf8', 'C.UTF-8']:
            if candidate in available_collations:
                return candidate
        return 'default'
    else:
        # Case-sensitive
        return 'C' if 'C' in available_collations else 'default'

def get_available_collations(pg_conn):
    """Hole verfügbare Collations aus PostgreSQL"""
    cursor = pg_conn.cursor()
    # Hole alle verfügbaren Collations (nicht nur ICU)
    cursor.execute("SELECT collname FROM pg_collation WHERE collname != '' ORDER BY collname")
    collations = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return collations

def alter_column_collation(pg_conn, col_info, pg_collation):
    """Ändere Collation einer Spalte in PostgreSQL"""
    cursor = pg_conn.cursor()
    
    schema = col_info['schema']
    table = col_info['table']
    column = col_info['column']
    
    # Mappe Schema (dbo -> public)
    pg_schema = map_schema_name(schema)
    
    # Lade Spalten-Mapping
    column_mapping = load_column_mapping()
    table_key = f"{schema}.{table}"
    table_mapping = column_mapping.get(table_key, {})
    
    # Mappe Spaltennamen (gekürzte Namen)
    pg_column = table_mapping.get(column, column)
    
    # Normalisiere Namen (NICHT die Collation - die hat eigene Naming-Konventionen)
    norm_schema = normalize_name(pg_schema)
    norm_table = normalize_name(table)
    norm_column = normalize_name(pg_column)
    
    try:
        # Skip wenn 'default' - dann verwendet PostgreSQL automatisch die DB-Standard-Collation
        if pg_collation == 'default':
            print_detail(f"  → DB-Standard: {schema}.{table}.{column}", level='INFO')
            return True
        
        # In PostgreSQL müssen wir den gesamten Spaltentyp neu definieren mit der COLLATE Klausel - nur für explizite Collations wie 'C'
        sql = f'ALTER TABLE "{norm_schema}"."{norm_table}" ALTER COLUMN "{norm_column}" TYPE varchar COLLATE "{pg_collation}"'
        
        cursor.execute(sql)
        pg_conn.commit()
        print_detail(f"  Collation gesetzt: {schema}.{table}.{column} -> {pg_collation}", level='INFO')
        return True
    except Exception as e:
        pg_conn.rollback()
        print_detail(f"  Collation nicht gesetzt für {schema}.{table}.{column}: {e}", level='ERROR')
        return False

def main():
    """Hauptfunktion"""
    print_summary("=" * 60)
    print_summary("SCHRITT 4: COLLATIONS MIGRIEREN")
    print_summary("=" * 60)
    print_summary("")
    print_summary("HINWEIS: Collations in PostgreSQL funktionieren anders als in MSSQL.")
    print_detail("Dieses Script versucht eine Näherungslösung, aber manuelle", level='INFO')
    print_detail("Anpassungen können erforderlich sein.", level='INFO')
    print_detail("", level='DEBUG')
    
    start_time = datetime.now()
    
    # Verbindungen herstellen
    mssql_conn = connect_mssql()
    pg_conn = connect_postgresql()
    
    try:
        # Hole verfügbare PostgreSQL Collations
        print_detail("Hole verfügbare PostgreSQL Collations...", level='INFO')
        available_collations = get_available_collations(pg_conn)
        print_detail(f"Verfügbare Collations: {len(available_collations)}", level='DEBUG')
        print_detail("", level='DEBUG')
        
        # Hole Spalten mit Collations
        print_detail("Hole Spalten mit Collations aus MSSQL...", level='INFO')
        columns = get_column_collations(mssql_conn)
        print_summary(f"Gefundene Spalten mit Collations: {len(columns)}")
        print_detail("", level='DEBUG')
        
        # Gruppiere nach Collation
        collation_groups = {}
        for col in columns:
            collation = col['collation']
            if collation not in collation_groups:
                collation_groups[collation] = []
            collation_groups[collation].append(col)
        
        print_detail(f"Verschiedene Collations: {len(collation_groups)}")
        print_detail("")
        
        # Zeige Mapping
        print_detail("--- COLLATION MAPPING ---")
        for mssql_coll in sorted(collation_groups.keys()):
            pg_coll = map_collation(mssql_coll, available_collations)
            count = len(collation_groups[mssql_coll])
            available = "✓" if pg_coll in available_collations or pg_coll == 'C' or pg_coll == 'default' else "✗"
            print_detail(f"{available} {mssql_coll} -> {pg_coll} ({count} Spalten)")
        print_detail("")
        
        print_detail("--- COLLATIONS SETZEN ---")
        
        # Setze Collations
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for col in columns:
            pg_collation = map_collation(col['collation'], available_collations)
            
            # Überspringe wenn Collation 'default' ist (DB-Standard)
            if pg_collation == 'default':
                skipped_count += 1
                continue
            
            if alter_column_collation(pg_conn, col, pg_collation):
                success_count += 1
            else:
                failed_count += 1
        
        # Zusammenfassung
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print_detail("")
        print_detail("=" * 60)
        print_summary("ZUSAMMENFASSUNG")
        print_detail("=" * 60)
        print_summary(f"Spalten mit Collations: {len(columns)}")
        print_summary(f"  Erfolgreich gesetzt: {success_count}")
        print_summary(f"  Fehlgeschlagen: {failed_count}")
        print_summary(f"  Übersprungen: {skipped_count}")
        print_summary(f"Dauer: {duration:.2f} Sekunden")
        print_detail("")
        
        if skipped_count > 0 or failed_count > 0:
            print_summary("HINWEIS: Einige Collations konnten nicht gesetzt werden.")
            print_detail("Bitte prüfen Sie manuell, ob dies kritisch für Ihre Anwendung ist.")
        
        # Merke ob Fehler aufgetreten sind (optional bei Collations)
        has_errors = False
        
    finally:
        mssql_conn.close()
        pg_conn.close()
        print_summary("Verbindungen geschlossen.")
        logger.close()
    
    # Exit mit entsprechendem Code (außerhalb try/finally)
    sys.exit(0)

if __name__ == "__main__":
    main()
