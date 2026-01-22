"""
Schritt 1: Tabellen und Daten migrieren
Migriert Tabellenstrukturen (Spalten, Datentypen) und Daten von MSSQL nach PostgreSQL
"""

import pyodbc
import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
from datetime import datetime
import sys
import json
import re
from pathlib import Path
from type_mappings_manager import load_type_mappings_with_fallback

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
        self.log_level = self.LOG_LEVELS.get(log_level_str, 1)  # Default to INFO
    
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
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
# Prüfe ob run_dir gesetzt ist (von run_all.py)
run_dir = os.getenv('MIGRATION_RUN_DIR', 'logs')
print(f"[DEBUG] MIGRATION_RUN_DIR={run_dir}")
logger = Logger(f'{run_dir}/step1_debug.log')
print(f"[DEBUG] Log-Datei erstellt: {run_dir}/step1_debug.log")

# Globale print-Funktionen
def print_detail(msg, level='DEBUG'):
    logger.detail(str(msg), level=level)

def print_summary(msg):
    logger.summary(str(msg))

# Lade Umgebungsvariablen
load_dotenv()

# MSSQL Konfiguration
MSSQL_SERVER = os.getenv('MSSQL_SERVER')
MSSQL_PORT = os.getenv('MSSQL_PORT', '')  # Optional, Standard 1433
MSSQL_DATABASE = os.getenv('MSSQL_DATABASE')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')

# PostgreSQL Konfiguration
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# TYPE_MAPPING wird lazy geladen (nach Logger-Initialisierung)
TYPE_MAPPING = None

def get_type_mapping():
    """Lade TYPE_MAPPING aus JSON-Konfiguration (lazy)"""
    global TYPE_MAPPING
    if TYPE_MAPPING is None:
        TYPE_MAPPING = load_type_mappings_with_fallback()
    return TYPE_MAPPING

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
        print_summary(f"Fehler bei MSSQL-Verbindung: {e}")
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

def normalize_name(name):
    """Normalisiere Namen: Ersetze - durch _"""
    return name.replace('-', '_')

def shorten_column_name(column_name, max_length=63):
    """Kürze Spaltennamen auf PostgreSQL-Limit (63 Zeichen)"""
    if len(column_name) <= max_length:
        return column_name
    
    # Intelligente Kürzung: Behalte Anfang und Ende, kürze Mitte
    # Ende ist oft wichtig (_fk, _id, etc.)
    chars_to_remove = len(column_name) - max_length
    
    # Behalte erste 40 Zeichen und letzte 20 Zeichen
    if len(column_name) > 60:
        prefix_length = 40
        suffix_length = 20
        shortened = column_name[:prefix_length] + column_name[-(suffix_length):]
    else:
        # Für kürzere Namen: einfach am Ende abschneiden
        shortened = column_name[:max_length]
    
    return shortened

def save_column_mapping(mapping, filename=None):
    """Speichere Spalten-Mapping in JSON-Datei"""
    run_dir = os.getenv('MIGRATION_RUN_DIR', 'logs')
    
    # Speichere im run-Verzeichnis
    run_file = f'{run_dir}/column_mapping.json'
    try:
        with open(run_file, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
        print_summary(f"Column-Mapping gespeichert: {run_file}")
    except Exception as e:
        print_detail(f"  Warnung: Konnte Mapping nicht speichern: {e}", level='WARNING')
    
    # ZUSÄTZLICH: Speichere als aktuellste Version im logs/ root
    # für andere Scripts die das Mapping lesen müssen
    current_file = 'logs/column_mapping.json'
    try:
        with open(current_file, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print_detail(f"  Warnung: Konnte aktuelle Mapping-Datei nicht speichern: {e}", level='WARNING')

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

def get_table_columns(mssql_conn, schema, table):
    """Hole Spalteninformationen aus MSSQL inklusive IDENTITY und DEFAULT"""
    cursor = mssql_conn.cursor()
    query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY,
            IDENT_SEED(c.TABLE_SCHEMA + '.' + c.TABLE_NAME) as IDENTITY_SEED,
            IDENT_INCR(c.TABLE_SCHEMA + '.' + c.TABLE_NAME) as IDENTITY_INCREMENT
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
    """
    cursor.execute(query, (schema, table))
    columns = cursor.fetchall()
    cursor.close()
    return columns

def map_column_type(data_type, max_length, precision, scale):
    """Mappe MSSQL Datentyp zu PostgreSQL"""
    type_mapping = get_type_mapping()
    base_type = type_mapping.get(data_type.lower(), 'TEXT')
    
    if data_type.lower() in ['char', 'varchar', 'nchar', 'nvarchar']:
        if max_length and max_length > 0:
            return f"{base_type}({max_length})"
        else:
            return 'TEXT'
    elif data_type.lower() in ['decimal', 'numeric']:
        if precision and scale is not None:
            return f"{base_type}({precision},{scale})"
        else:
            return base_type
    
    return base_type

def create_postgres_table(pg_conn, schema, table, columns, column_mapping):
    """Erstelle Tabelle in PostgreSQL"""
    cursor = pg_conn.cursor()
    
    # Mappe Schema (dbo -> public)
    pg_schema = map_schema_name(schema)
    
    # Schema erstellen falls nicht vorhanden (außer public)
    if pg_schema.lower() != 'public':
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{pg_schema}"')
    
    # Tabelle löschen falls vorhanden
    cursor.execute(f'DROP TABLE IF EXISTS "{pg_schema}"."{table}" CASCADE')
    
    # CREATE TABLE Statement erstellen
    col_definitions = []
    table_key = f"{schema}.{table}"
    
    if table_key not in column_mapping:
        column_mapping[table_key] = {}
    
    has_long_columns = False
    
    for col in columns:
        col_name = col.COLUMN_NAME
        data_type = col.DATA_TYPE
        max_length = col.CHARACTER_MAXIMUM_LENGTH
        precision = col.NUMERIC_PRECISION
        scale = col.NUMERIC_SCALE
        is_nullable = col.IS_NULLABLE
        column_default = col.COLUMN_DEFAULT
        is_identity = col.IS_IDENTITY
        
        # Kürze Spaltennamen falls nötig (VOR Normalisierung)
        shortened_name = shorten_column_name(col_name)
        
        # Speichere Mapping nur wenn Name GEKÜRZT wurde (nicht normalisiert)
        if shortened_name != col_name:
            column_mapping[table_key][col_name] = shortened_name
            has_long_columns = True
        
        # Normalisiere Namen (Kleinbuchstaben, - zu _) - wird IMMER angewendet
        pg_col_name = normalize_name(shortened_name)
        
        # IDENTITY Spalten werden zu GENERATED BY DEFAULT/ALWAYS AS IDENTITY
        if is_identity == 1:
            identity_always = os.getenv('IDENTITY_ALWAYS', 'false').lower() == 'true'
            if identity_always:
                pg_type = 'BIGINT GENERATED ALWAYS AS IDENTITY'
                nullable = 'NOT NULL'
            else:
                pg_type = 'BIGINT GENERATED BY DEFAULT AS IDENTITY'
                nullable = 'NOT NULL'  # IDENTITY ist immer NOT NULL
        else:
            pg_type = map_column_type(data_type, max_length, precision, scale)
            nullable = '' if is_nullable == 'YES' else 'NOT NULL'
        
        # DEFAULT-Werte hinzufügen
        default_clause = ''
        if column_default and not is_identity:
            # Bereinige DEFAULT-Wert (MSSQL fügt oft Klammern hinzu)
            default_val = column_default.strip()
            # Entferne äußere Klammern
            while default_val.startswith('(') and default_val.endswith(')'):
                default_val = default_val[1:-1].strip()
            
            # Mappe spezielle MSSQL-Funktionen zu PostgreSQL
            if default_val.lower() == 'getdate()':
                default_val = 'CURRENT_TIMESTAMP'
            elif default_val.lower() == 'newid()':
                default_val = 'gen_random_uuid()'
            
            default_clause = f' DEFAULT {default_val}'
        
        # Spaltenname mit Anführungszeichen
        # Reihenfolge: Typ, NOT NULL, DEFAULT
        col_def = f'"{pg_col_name}" {pg_type} {nullable}{default_clause}'.strip()
        col_definitions.append(col_def)
    
    # Normalisiere Tabellennamen
    pg_table = normalize_name(table)
    
    # CREATE TABLE mit Anführungszeichen
    create_statement = f'CREATE TABLE "{pg_schema}"."{pg_table}" (\n    ' + ',\n    '.join(col_definitions) + '\n)'
    
    try:
        cursor.execute(create_statement)
        pg_conn.commit()
        
        # NICHT mehr hier speichern - wird in main() gesammelt
        if has_long_columns:
            long_cols = [k for k in column_mapping[table_key].keys()]
            print_detail(f"  Tabelle erstellt: {schema}.{table} -> {pg_schema}.{pg_table}", level='INFO')
            print_detail(f"    {len(long_cols)} Spalte(n) umbenannt", level='DEBUG')
        else:
            print_detail(f"  Tabelle erstellt: {schema}.{table} -> {pg_schema}.{pg_table}", level='INFO')
        
        return True, column_mapping
    except Exception as e:
        pg_conn.rollback()
        print_detail(f"  Fehler beim Erstellen der Tabelle {schema}.{table}: {e}", level='ERROR')
        return False, column_mapping

def migrate_table_data(mssql_conn, pg_conn, schema, table, columns_info, batch_size=1000):
    """Migriere Daten von MSSQL zu PostgreSQL"""
    mssql_cursor = mssql_conn.cursor()
    pg_cursor = pg_conn.cursor()
    
    # Mappe Schema (dbo -> public)
    pg_schema = map_schema_name(schema)
    
    # Lade Spalten-Mapping
    column_mapping = load_column_mapping()
    table_key = f"{schema}.{table}"
    table_mapping = column_mapping.get(table_key, {})
    
    # Finde IDENTITY-Spalten und prüfe auf GENERATED ALWAYS
    identity_always = os.getenv('IDENTITY_ALWAYS', 'false').lower() == 'true'
    
    try:
        # Hole alle Daten aus MSSQL
        mssql_cursor.execute(f'SELECT * FROM [{schema}].[{table}]')
        columns = [column[0] for column in mssql_cursor.description]
        
        # Mappe Spaltennamen zu PostgreSQL (gekürzte Namen)
        pg_columns = [table_mapping.get(col, col) for col in columns]
        
        # Erstelle INSERT Statement (mit oder ohne OVERRIDING SYSTEM VALUE)
        placeholders = ','.join(['%s'] * len(pg_columns))
        column_names = ','.join([f'"{col}"' for col in pg_columns])
        
        # Mit GENERATED ALWAYS brauchen wir OVERRIDING SYSTEM VALUE
        if identity_always:
            insert_query = f'INSERT INTO "{pg_schema}"."{table}" ({column_names}) OVERRIDING SYSTEM VALUE VALUES ({placeholders})'
        else:
            insert_query = f'INSERT INTO "{pg_schema}"."{table}" ({column_names}) VALUES ({placeholders})'
        
        # Batch-Insert
        rows = []
        total_rows = 0
        
        for row in mssql_cursor:
            rows.append(tuple(row))
            if len(rows) >= batch_size:
                execute_batch(pg_cursor, insert_query, rows)
                total_rows += len(rows)
                rows = []
        
        # Restliche Zeilen einfügen
        if rows:
            execute_batch(pg_cursor, insert_query, rows)
            total_rows += len(rows)
        
        # Setze Sequenzen für IDENTITY-Spalten zurück (nur wenn nicht GENERATED ALWAYS)
        if not identity_always:
            # Finde IDENTITY-Spalten
            identity_columns = []
            for col in columns_info:
                if col.IS_IDENTITY == 1:
                    pg_col_name = table_mapping.get(col.COLUMN_NAME, col.COLUMN_NAME)
                    identity_columns.append(pg_col_name)
            
            if identity_columns:
                for id_col in identity_columns:
                    try:
                        # Hole aktuellen Maximalwert
                        pg_cursor.execute(f'SELECT MAX("{id_col}") FROM "{pg_schema}"."{table}"')
                        max_val = pg_cursor.fetchone()[0]
                        if max_val is not None:
                            # Finde Sequenznamen automatisch (funktioniert für GENERATED AS IDENTITY)
                            pg_cursor.execute(f"SELECT pg_get_serial_sequence('\"{pg_schema}\".\"{table}\"', '{id_col}')")
                            sequence_name = pg_cursor.fetchone()[0]
                            if sequence_name:
                                pg_cursor.execute(f"SELECT setval('{sequence_name}', {max_val})")
                                print_detail(f"    Sequenz zurückgesetzt: {sequence_name} -> {max_val}", level='DEBUG')
                            else:
                                print_detail(f"    Warnung: Keine Sequenz gefunden für {id_col}", level='WARNING')
                    except Exception as seq_err:
                        print_detail(f"    Warnung: Konnte Sequenz nicht zurücksetzen für {id_col}: {seq_err}", level='WARNING')
        
        pg_conn.commit()
        print_detail(f"  Daten migriert: {total_rows} Zeilen")
        return total_rows
        
    except Exception as e:
        pg_conn.rollback()
        print_detail(f"  Fehler beim Migrieren der Daten: {e}")
        return 0
    finally:
        mssql_cursor.close()

def normalize_column_names(pg_conn):
    """
    Normalisiert alle Spaltennamen zu lowercase/snake_case
    Konvertiert CamelCase Spalten um sie mit Hibernate kompatibel zu machen
    z.B. firstName → first_name
    """
    cursor = pg_conn.cursor()
    
    # Prüfe ob Spalten-Normalisierung aktiviert ist
    normalize = os.getenv('NORMALIZE_COLUMNS', 'false').lower() == 'true'
    if not normalize:
        print_detail("Spalten-Normalisierung übersprungen (NORMALIZE_COLUMNS=false)", level='DEBUG')
        return True
    
    try:
        print_summary("")
        print_summary("=" * 60)
        print_summary("SPALTEN NORMALISIEREN (CamelCase → lowercase)")
        print_summary("=" * 60)
        
        # Finde alle Tabellen im public Schema
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            print_summary("Keine Tabellen gefunden.")
            return True
        
        total_renamed = 0
        
        for table_name in tables:
            # Finde alle Spalten mit Großbuchstaben
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                AND column_name ~ '[A-Z]'
                ORDER BY column_name
            """, (table_name,))
            
            columns_to_rename = [row[0] for row in cursor.fetchall()]
            
            if not columns_to_rename:
                continue
            
            print_detail(f"Normalisiere Tabelle: {table_name}", level='INFO')
            
            for old_col_name in columns_to_rename:
                # Konvertiere nur zu Kleinbuchstaben (keine Unterstriche hinzufügen)
                new_col_name = old_col_name.lower()
                
                if new_col_name == old_col_name:
                    continue
                
                try:
                    cursor.execute(f'ALTER TABLE "{table_name}" RENAME COLUMN "{old_col_name}" TO "{new_col_name}"')
                    print_detail(f"  {old_col_name} → {new_col_name}", level='DEBUG')
                    total_renamed += 1
                except Exception as col_err:
                    print_detail(f"  Warnung: '{old_col_name}' konnte nicht umbenannt werden: {col_err}", level='WARNING')
        
        # Commit der Änderungen
        pg_conn.commit()
        
        print_summary("")
        print_summary(f"Spalten normalisiert: {total_renamed}")
        print_summary("")
        
        return True
        
    except Exception as e:
        print_detail(f"Fehler bei Spalten-Normalisierung: {e}", level='ERROR')
        import traceback
        print_detail(traceback.format_exc(), level='ERROR')
        return False
    finally:
        cursor.close()

def normalize_table_names(pg_conn):
    """
    Normalisiert alle Tabellennamen zu lowercase
    """
    cursor = pg_conn.cursor()
    
    # Prüfe ob Normalisierung aktiviert ist
    normalize = os.getenv('NORMALIZE_COLUMNS', 'false').lower() == 'true'
    if not normalize:
        print_detail("Tabellen-Normalisierung übersprungen (NORMALIZE_COLUMNS=false)", level='DEBUG')
        return True
    
    try:
        print_summary("")
        print_summary("=" * 60)
        print_summary("TABELLENNAMEN NORMALISIEREN")
        print_summary("=" * 60)
        
        # Finde alle Tabellen mit Großbuchstaben
        cursor.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            AND table_type = 'BASE TABLE'
            AND table_name ~ '[A-Z]'
            ORDER BY table_schema, table_name
        """)
        
        tables_to_rename = cursor.fetchall()
        
        if not tables_to_rename:
            print_summary("Keine Tabellennamen mit Großbuchstaben gefunden.")
            print_summary("")
            return True
        
        renamed_count = 0
        
        for schema, table_name in tables_to_rename:
            new_table_name = table_name.lower()
            
            if new_table_name == table_name:
                continue
            
            try:
                print_detail(f"Benennen um: {schema}.{table_name} -> {schema}.{new_table_name}", level='INFO')
                cursor.execute(f'ALTER TABLE "{schema}"."{table_name}" RENAME TO "{new_table_name}"')
                renamed_count += 1
            except Exception as err:
                print_detail(f"  Warnung: Tabelle '{schema}.{table_name}' konnte nicht umbenannt werden: {err}", level='WARNING')
        
        # Commit der Änderungen
        pg_conn.commit()
        
        print_summary("")
        print_summary(f"Tabellennamen normalisiert: {renamed_count}")
        print_summary("")
        
        return True
        
    except Exception as e:
        print_detail(f"Fehler bei Tabellen-Normalisierung: {e}", level='ERROR')
        import traceback
        print_detail(traceback.format_exc(), level='ERROR')
        return False
    finally:
        cursor.close()

def main():
    """Hauptfunktion"""
    print_summary("=" * 60)
    print_summary("SCHRITT 1: TABELLEN UND DATEN MIGRIEREN")
    print_summary("=" * 60)
    print_summary("")
    
    start_time = datetime.now()
    
    # Verbindungen herstellen
    mssql_conn = connect_mssql()
    pg_conn = connect_postgresql()
    
    # Variable für Exit-Code
    has_errors = False
    
    # Initialisiere column_mapping
    column_mapping = {}
    
    try:
        # Hole alle Tabellen
        print_summary("\nHole Tabellenliste aus MSSQL...")
        tables = get_mssql_tables(mssql_conn)
        print_summary(f"Gefundene Tabellen: {len(tables)}")
        print_summary("")
        
        # Migriere jede Tabelle
        success_count = 0
        failed_tables = []
        
        for schema, table in tables:
            print_detail(f"Migriere: {schema}.{table}", level='INFO')
            
            # Hole Spalteninformationen
            columns = get_table_columns(mssql_conn, schema, table)
            
            # Erstelle Tabelle und sammle Mapping (übergebe aktuelles Mapping)
            success, column_mapping = create_postgres_table(pg_conn, schema, table, columns, column_mapping)
            
            if success:
                # Migriere Daten (wenn aktiviert)
                migrate_data = os.getenv('MIGRATE_DATA', 'true').lower() == 'true'
                if migrate_data:
                    row_count = migrate_table_data(mssql_conn, pg_conn, schema, table, columns)
                    if row_count >= 0:
                        success_count += 1
                    else:
                        failed_tables.append(f"{schema}.{table}")
                else:
                    print_detail(f"  Datenmigrationen übersprungen (MIGRATE_DATA=false)", level='INFO')
                    success_count += 1
            else:
                failed_tables.append(f"{schema}.{table}")
            
            print_detail("", level='DEBUG')
        
        # Zusammenfassung
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Speichere Column-Mapping EINMAL am Ende (falls vorhanden)
        if column_mapping:
            save_column_mapping(column_mapping)
            print_summary(f"Column-Mappings gespeichert: {len(column_mapping)} Tabelle(n)")
        
        # Das finale Mapping ist bereits gespeichert, keine zusätzliche Speicherung nötig
        
        print_summary("=" * 60)
        print_summary("ZUSAMMENFASSUNG")
        print_summary("=" * 60)
        print_summary(f"Erfolgreich migriert: {success_count}/{len(tables)} Tabellen")
        if failed_tables:
            print_summary(f"Fehlgeschlagen: {len(failed_tables)}")
            for table in failed_tables:
                print_summary(f"  - {table}")
        print_summary(f"Dauer: {duration:.2f} Sekunden")
        print_summary("")
        
        # Tabellennamen normalisieren (wenn NORMALIZE_COLUMNS aktiviert)
        print_detail("Starten Sie Tabellen-Normalisierung...", level='INFO')
        normalize_table_names(pg_conn)
        
        # Spalten normalisieren (wenn NORMALIZE_COLUMNS aktiviert)
        print_detail("Starten Sie Spalten-Normalisierung...", level='INFO')
        normalize_column_names(pg_conn)
        
        # Merke ob Fehler aufgetreten sind
        has_errors = len(failed_tables) > 0
        
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
    try:
        main()
    except SystemExit:
        raise  # Lasse sys.exit() durch
    except Exception as e:
        print_summary(f"\nFEHLER: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
