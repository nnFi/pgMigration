"""
Schritt 3: Constraints, Keys und Indexes migrieren
Fügt Primary Keys, Unique Constraints, Foreign Keys und Filtered Indexes hinzu
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
run_dir = os.getenv('MIGRATION_RUN_DIR', 'logs')
logger = Logger(f'{run_dir}/step3_debug.log')
# NICHT: sys.stdout = logger  # Dies führt zu Problemen beim Beenden!
# NICHT: sys.stderr = logger

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

# Optionen - keine mehr nötig

def normalize_name(name):
    """Normalisiere Namen: Ersetze - durch _ und konvertiere zu lowercase wenn NORMALIZE_COLUMNS aktiviert"""
    # Prüfe ob Normalisierung aktiviert ist
    normalize_enabled = os.getenv('NORMALIZE_COLUMNS', '').lower() == 'true'
    
    normalized = name.replace('-', '_')
    
    if normalize_enabled:
        # Wenn Normalisierung aktiv, sind Tabellen und Spalten lowercase in PostgreSQL
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

def get_primary_keys(mssql_conn):
    """Hole alle Primary Keys aus MSSQL"""
    cursor = mssql_conn.cursor()
    query = """
        SELECT 
            tc.TABLE_SCHEMA,
            tc.TABLE_NAME,
            tc.CONSTRAINT_NAME,
            STRING_AGG(kcu.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION) as COLUMNS
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME
        ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME
    """
    cursor.execute(query)
    pks = []
    for row in cursor.fetchall():
        pks.append({
            'schema': row.TABLE_SCHEMA,
            'table': row.TABLE_NAME,
            'constraint_name': row.CONSTRAINT_NAME,
            'columns': row.COLUMNS.split(',')
        })
    cursor.close()
    return pks

def get_unique_constraints(mssql_conn):
    """Hole alle Unique Constraints aus MSSQL"""
    cursor = mssql_conn.cursor()
    query = """
        SELECT 
            tc.TABLE_SCHEMA,
            tc.TABLE_NAME,
            tc.CONSTRAINT_NAME,
            STRING_AGG(kcu.COLUMN_NAME, ',') WITHIN GROUP (ORDER BY kcu.ORDINAL_POSITION) as COLUMNS
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            AND tc.TABLE_NAME = kcu.TABLE_NAME
        WHERE tc.CONSTRAINT_TYPE = 'UNIQUE'
        GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME
        ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME
    """
    cursor.execute(query)
    uqs = []
    for row in cursor.fetchall():
        uqs.append({
            'schema': row.TABLE_SCHEMA,
            'table': row.TABLE_NAME,
            'constraint_name': row.CONSTRAINT_NAME,
            'columns': row.COLUMNS.split(',')
        })
    cursor.close()
    return uqs

def get_foreign_keys(mssql_conn):
    """Hole alle Foreign Keys aus MSSQL"""
    cursor = mssql_conn.cursor()
    query = """
        SELECT 
            fk.name AS CONSTRAINT_NAME,
            OBJECT_SCHEMA_NAME(fk.parent_object_id) AS TABLE_SCHEMA,
            OBJECT_NAME(fk.parent_object_id) AS TABLE_NAME,
            STRING_AGG(COL_NAME(fkc.parent_object_id, fkc.parent_column_id), ',') 
                WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS COLUMNS,
            OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS REFERENCED_SCHEMA,
            OBJECT_NAME(fk.referenced_object_id) AS REFERENCED_TABLE,
            STRING_AGG(COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id), ',') 
                WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS REFERENCED_COLUMNS,
            fk.delete_referential_action_desc AS DELETE_RULE,
            fk.update_referential_action_desc AS UPDATE_RULE
        FROM sys.foreign_keys fk
        INNER JOIN sys.foreign_key_columns fkc 
            ON fk.object_id = fkc.constraint_object_id
        GROUP BY 
            fk.name,
            fk.parent_object_id,
            fk.referenced_object_id,
            fk.delete_referential_action_desc,
            fk.update_referential_action_desc
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    cursor.execute(query)
    fks = []
    for row in cursor.fetchall():
        fks.append({
            'constraint_name': row.CONSTRAINT_NAME,
            'schema': row.TABLE_SCHEMA,
            'table': row.TABLE_NAME,
            'columns': row.COLUMNS.split(','),
            'referenced_schema': row.REFERENCED_SCHEMA,
            'referenced_table': row.REFERENCED_TABLE,
            'referenced_columns': row.REFERENCED_COLUMNS.split(','),
            'delete_rule': row.DELETE_RULE,
            'update_rule': row.UPDATE_RULE
        })
    cursor.close()
    return fks

def get_filtered_indexes(mssql_conn):
    """Hole alle Filtered/Partial Indexes aus MSSQL (Indexes mit WHERE Klauseln)"""
    cursor = mssql_conn.cursor()
    query = """
        SELECT 
            OBJECT_SCHEMA_NAME(i.object_id) AS TABLE_SCHEMA,
            OBJECT_NAME(i.object_id) AS TABLE_NAME,
            i.name AS INDEX_NAME,
            i.is_unique AS IS_UNIQUE,
            i.filter_definition AS WHERE_CLAUSE,
            STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS COLUMNS
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.filter_definition IS NOT NULL  -- Nur filtered indexes
            AND i.is_primary_key = 0  -- Keine Primary Keys
            AND i.is_unique_constraint = 0  -- Keine Unique Constraints
        GROUP BY 
            i.object_id,
            i.name,
            i.is_unique,
            i.filter_definition
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """
    cursor.execute(query)
    indexes = []
    for row in cursor.fetchall():
        indexes.append({
            'schema': row.TABLE_SCHEMA,
            'table': row.TABLE_NAME,
            'index_name': row.INDEX_NAME,
            'is_unique': row.IS_UNIQUE,
            'where_clause': row.WHERE_CLAUSE,
            'columns': row.COLUMNS.split(',')
        })
    cursor.close()
    return indexes

def add_primary_key(pg_conn, pk):
    """Füge Primary Key zu PostgreSQL Tabelle hinzu"""
    cursor = pg_conn.cursor()
    
    schema = pk['schema']
    table = pk['table']
    pg_schema = map_schema_name(schema)
    constraint_name = pk['constraint_name']
    
    # Lade Spalten-Mapping
    column_mapping = load_column_mapping()
    table_key = f"{schema}.{table}"
    table_mapping = column_mapping.get(table_key, {})
    
    # Mappe Spaltennamen (gekürzte Namen)
    pg_columns = [table_mapping.get(col, col) for col in pk['columns']]
    
    # Normalisiere Namen
    norm_schema = normalize_name(pg_schema)
    norm_table = normalize_name(table)
    norm_constraint = normalize_name(constraint_name)
    norm_columns = [normalize_name(col) for col in pg_columns]
    
    # SQL mit Quotes
    columns = ', '.join([f'"{col}"' for col in norm_columns])
    drop_sql = f'ALTER TABLE "{norm_schema}"."{norm_table}" DROP CONSTRAINT IF EXISTS "{norm_constraint}" CASCADE'
    add_sql = f'ALTER TABLE "{norm_schema}"."{norm_table}" ADD CONSTRAINT "{norm_constraint}" PRIMARY KEY ({columns})'
    
    try:
        # Entferne vorhandenen Constraint falls vorhanden
        cursor.execute(drop_sql)
        
        # Füge neuen Constraint hinzu
        sql = add_sql
        cursor.execute(sql)
        pg_conn.commit()
        print_detail(f"   Primary Key: {schema}.{table} ({', '.join(pk['columns'])})", level='INFO')
        return True
    except Exception as e:
        pg_conn.rollback()
        print_detail(f"   Fehler bei Primary Key {schema}.{table}: {e}", level='ERROR')
        return False

def add_unique_constraint(pg_conn, uq):
    """Füge Unique Constraint zu PostgreSQL Tabelle hinzu"""
    cursor = pg_conn.cursor()
    
    schema = uq['schema']
    table = uq['table']
    pg_schema = map_schema_name(schema)
    constraint_name = uq['constraint_name']
    
    # Lade Spalten-Mapping
    column_mapping = load_column_mapping()
    table_key = f"{schema}.{table}"
    table_mapping = column_mapping.get(table_key, {})
    
    # Mappe Spaltennamen (gekürzte Namen)
    pg_columns = [table_mapping.get(col, col) for col in uq['columns']]
    
    # Normalisiere Namen
    norm_schema = normalize_name(pg_schema)
    norm_table = normalize_name(table)
    norm_constraint = normalize_name(constraint_name)
    norm_columns = [normalize_name(col) for col in pg_columns]
    
    # SQL mit Quotes
    columns = ', '.join([f'"{col}"' for col in norm_columns])
    drop_sql = f'ALTER TABLE "{norm_schema}"."{norm_table}" DROP CONSTRAINT IF EXISTS "{norm_constraint}" CASCADE'
    add_sql = f'ALTER TABLE "{norm_schema}"."{norm_table}" ADD CONSTRAINT "{norm_constraint}" UNIQUE ({columns})'
    
    try:
        # Entferne vorhandenen Constraint falls vorhanden
        cursor.execute(drop_sql)
        
        # Füge neuen Constraint hinzu
        sql = add_sql
        cursor.execute(sql)
        pg_conn.commit()
        print_detail(f"   Unique Constraint: {schema}.{table} ({', '.join(uq['columns'])})", level='INFO')
        return True
    except Exception as e:
        pg_conn.rollback()
        print_detail(f"   Fehler bei Unique Constraint {schema}.{table}: {e}", level='ERROR')
        return False

def add_filtered_index(pg_conn, idx):
    """Erstelle Filtered/Partial Index in PostgreSQL"""
    cursor = pg_conn.cursor()
    
    schema = idx['schema']
    table = idx['table']
    pg_schema = map_schema_name(schema)
    index_name = idx['index_name']
    
    # Lade Spalten-Mapping
    column_mapping = load_column_mapping()
    table_key = f"{schema}.{table}"
    table_mapping = column_mapping.get(table_key, {})
    
    # Prüfe ob Normalisierung aktiviert ist
    normalize_enabled = os.getenv('NORMALIZE_COLUMNS', '').lower() == 'true'
    
    # Mappe Spaltennamen (gekürzte Namen)
    pg_columns = []
    for col in idx['columns']:
        mapped_col = col
        if normalize_enabled:
            # Case-insensitive Suche im Mapping
            for mapping_key in table_mapping.keys():
                if mapping_key.lower() == col.lower():
                    mapped_col = table_mapping[mapping_key]
                    break
        else:
            # Normaler case-sensitive Lookup
            mapped_col = table_mapping.get(col, col)
        pg_columns.append(mapped_col)
    
    # Spalten zu lowercase wenn Normalisierung aktiv
    if normalize_enabled:
        pg_columns = [col.lower() for col in pg_columns]
    
    # Normalisiere Namen
    norm_schema = normalize_name(pg_schema)
    norm_table = normalize_name(table)
    norm_index = normalize_name(index_name)
    norm_columns = [normalize_name(col) for col in pg_columns]
    
    # Konvertiere MSSQL WHERE Klausel zu PostgreSQL
    where_clause = idx['where_clause']
    # Entferne eckige Klammern um Spaltennamen
    where_clause = where_clause.replace('[', '"').replace(']', '"')
    
    # Ersetze Spaltennamen in WHERE Klausel
    if normalize_enabled:
        # Bei aktivierter Normalisierung: Alle Spaltennamen zu lowercase
        # Regex um Spaltennamen in Anführungszeichen zu finden und zu lowercase zu konvertieren
        import re
        def lowercase_column(match):
            return f'"{match.group(1).lower()}"'
        where_clause = re.sub(r'"([^"]+)"', lowercase_column, where_clause)
    
    # Zusätzlich: Mappe gekürzte Spaltennamen
    for orig_col in table_mapping.keys():
        mapped_col = table_mapping[orig_col]
        if normalize_enabled:
            norm_orig = orig_col.lower()
            norm_mapped = mapped_col.lower()
        else:
            norm_orig = orig_col
            norm_mapped = mapped_col
        norm_mapped = normalize_name(norm_mapped)
        where_clause = where_clause.replace(f'"{norm_orig}"', f'"{norm_mapped}"')
    
    unique_keyword = 'UNIQUE' if idx['is_unique'] else ''
    
    # SQL mit Quotes
    columns = ', '.join([f'"{col}"' for col in norm_columns])
    drop_sql = f'DROP INDEX IF EXISTS "{norm_schema}"."{norm_index}"'
    create_sql = f'CREATE {unique_keyword} INDEX "{norm_index}" ON "{norm_schema}"."{norm_table}" ({columns}) WHERE {where_clause}'
    
    try:
        # Lösche Index falls vorhanden
        cursor.execute(drop_sql)
        
        # Erstelle Index mit WHERE Klausel
        sql = create_sql
        cursor.execute(sql)
        pg_conn.commit()
        print_detail(f"   Filtered Index: {schema}.{table}.{index_name}", level='DEBUG')
        return True
    except Exception as e:
        pg_conn.rollback()
        print_detail(f"   Fehler bei Filtered Index {schema}.{table}.{index_name}: {e}", level='ERROR')
        return False

def add_foreign_key(pg_conn, fk):
    """Füge Foreign Key zu PostgreSQL Tabelle hinzu"""
    cursor = pg_conn.cursor()
    
    schema = fk['schema']
    table = fk['table']
    pg_schema = map_schema_name(schema)
    constraint_name = fk['constraint_name']
    
    # Lade Spalten-Mapping für Quelltabelle
    column_mapping = load_column_mapping()
    table_key = f"{schema}.{table}"
    table_mapping = column_mapping.get(table_key, {})
    
    # Mappe Spaltennamen (gekürzte Namen)
    # Bei aktivierter Normalisierung: case-insensitive Mapping-Lookup
    normalize_enabled = os.getenv('NORMALIZE_COLUMNS', '').lower() == 'true'
    
    pg_columns = []
    for col in fk['columns']:
        mapped_col = col
        if normalize_enabled:
            # Case-insensitive Suche im Mapping
            for mapping_key in table_mapping.keys():
                if mapping_key.lower() == col.lower():
                    mapped_col = table_mapping[mapping_key]
                    break
        else:
            # Normaler case-sensitive Lookup
            mapped_col = table_mapping.get(col, col)
        pg_columns.append(mapped_col)
    
    ref_schema = fk['referenced_schema']
    pg_ref_schema = map_schema_name(ref_schema)
    ref_table = fk['referenced_table']
    
    # Lade Spalten-Mapping für Zieltabelle
    ref_table_key = f"{ref_schema}.{ref_table}"
    ref_table_mapping = column_mapping.get(ref_table_key, {})
    
    # Mappe referenzierte Spaltennamen (gekürzte Namen)
    pg_ref_columns = []
    for col in fk['referenced_columns']:
        mapped_col = col
        if normalize_enabled:
            # Case-insensitive Suche im Mapping
            for mapping_key in ref_table_mapping.keys():
                if mapping_key.lower() == col.lower():
                    mapped_col = ref_table_mapping[mapping_key]
                    break
        else:
            # Normaler case-sensitive Lookup
            mapped_col = ref_table_mapping.get(col, col)
        pg_ref_columns.append(mapped_col)
    
    # Spalten nur zu lowercase wenn Normalisierung aktiv
    if normalize_enabled:
        pg_columns = [col.lower() for col in pg_columns]
        pg_ref_columns = [col.lower() for col in pg_ref_columns]
    
    # Normalisiere Namen (Bindestriche ersetzen, ggf. weitere lowercase wenn NORMALIZE_COLUMNS)
    norm_schema = normalize_name(pg_schema)
    norm_table = normalize_name(table)
    norm_constraint = normalize_name(constraint_name)
    norm_columns = [normalize_name(col) for col in pg_columns]
    norm_ref_schema = normalize_name(pg_ref_schema)
    norm_ref_table = normalize_name(ref_table)
    norm_ref_columns = [normalize_name(col) for col in pg_ref_columns]
    
    # Konvertiere MSSQL Regeln zu PostgreSQL
    delete_rule = 'NO ACTION'
    if fk['delete_rule'] == 'CASCADE':
        delete_rule = 'CASCADE'
    elif fk['delete_rule'] == 'SET_NULL':
        delete_rule = 'SET NULL'
    elif fk['delete_rule'] == 'SET_DEFAULT':
        delete_rule = 'SET DEFAULT'
    
    update_rule = 'NO ACTION'
    if fk['update_rule'] == 'CASCADE':
        update_rule = 'CASCADE'
    elif fk['update_rule'] == 'SET_NULL':
        update_rule = 'SET NULL'
    elif fk['update_rule'] == 'SET_DEFAULT':
        update_rule = 'SET DEFAULT'
    
    try:
        # Prüfe ob Zieltabelle existiert
        check_query = '''
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        '''
        cursor.execute(check_query, (norm_ref_schema, norm_ref_table))
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print_detail(f"   Übersprungen (Zieltabelle fehlt): {schema}.{table} -> {ref_schema}.{ref_table}", level='WARNING')
            return False
        
        # SQL mit Quotes
        columns = ', '.join([f'"{col}"' for col in norm_columns])
        ref_columns = ', '.join([f'"{col}"' for col in norm_ref_columns])
        drop_sql = f'ALTER TABLE "{norm_schema}"."{norm_table}" DROP CONSTRAINT IF EXISTS "{norm_constraint}" CASCADE'
        add_sql = f'''
            ALTER TABLE "{norm_schema}"."{norm_table}" 
            ADD CONSTRAINT "{norm_constraint}" 
            FOREIGN KEY ({columns}) 
            REFERENCES "{norm_ref_schema}"."{norm_ref_table}" ({ref_columns})
            ON DELETE {delete_rule}
            ON UPDATE {update_rule}
        '''
        
        # Entferne vorhandenen Constraint falls vorhanden
        cursor.execute(drop_sql)
        
        # Füge neuen Constraint hinzu
        sql = add_sql
        cursor.execute(sql)
        pg_conn.commit()
        print_detail(f"   Foreign Key: {schema}.{table} -> {ref_schema}.{ref_table}", level='INFO')
        return True
    except Exception as e:
        pg_conn.rollback()
        print_detail(f"   FEHLER bei Foreign Key {constraint_name}:", level='ERROR')
        print_detail(f"     Von: {schema}.{table}({', '.join(fk['columns'])})", level='ERROR')
        print_detail(f"     Nach: {ref_schema}.{ref_table}({', '.join(fk['referenced_columns'])})", level='ERROR')
        print_detail(f"     Fehler: {e}", level='ERROR')
        return False

def main():
    """Hauptfunktion"""
    print_summary("=" * 60)
    print_summary("SCHRITT 3: CONSTRAINTS, KEYS UND INDEXES MIGRIEREN")
    print_summary("=" * 60)
    print_summary("")
    
    start_time = datetime.now()
    
    # Verbindungen herstellen
    mssql_conn = connect_mssql()
    pg_conn = connect_postgresql()
    
    try:
        # Primary Keys
        print_detail("\n--- PRIMARY KEYS ---")
        pks = get_primary_keys(mssql_conn)
        print_summary(f"Gefundene Primary Keys: {len(pks)}")
        pk_success = 0
        for pk in pks:
            if add_primary_key(pg_conn, pk):
                pk_success += 1
        
        # Unique Constraints
        print_detail("\n--- UNIQUE CONSTRAINTS ---")
        uqs = get_unique_constraints(mssql_conn)
        print_summary(f"Gefundene Unique Constraints: {len(uqs)}")
        uq_success = 0
        for uq in uqs:
            if add_unique_constraint(pg_conn, uq):
                uq_success += 1
        
        # Foreign Keys
        print_detail("\n--- FOREIGN KEYS ---")
        fks = get_foreign_keys(mssql_conn)
        print_summary(f"Gefundene Foreign Keys: {len(fks)}")
        fk_success = 0
        for fk in fks:
            if add_foreign_key(pg_conn, fk):
                fk_success += 1
        
        # Filtered/Partial Indexes
        print_detail("\n--- FILTERED INDEXES (mit WHERE Klauseln) ---")
        indexes = get_filtered_indexes(mssql_conn)
        print_summary(f"Gefundene Filtered Indexes: {len(indexes)}")
        idx_success = 0
        for idx in indexes:
            if add_filtered_index(pg_conn, idx):
                idx_success += 1
        
        # Zusammenfassung
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print_detail("")
        print_detail("=" * 60)
        print_summary("ZUSAMMENFASSUNG")
        print_detail("=" * 60)
        print_summary(f"Primary Keys: {pk_success}/{len(pks)} erfolgreich")
        print_summary(f"Unique Constraints: {uq_success}/{len(uqs)} erfolgreich")
        print_summary(f"Foreign Keys: {fk_success}/{len(fks)} erfolgreich")
        print_summary(f"Filtered Indexes: {idx_success}/{len(indexes)} erfolgreich")
        print_summary(f"Dauer: {duration:.2f} Sekunden")
        print_detail("")
        
        # Exit Code setzen
        has_errors = (pk_success < len(pks) or uq_success < len(uqs) or fk_success < len(fks) or idx_success < len(indexes))
        
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
