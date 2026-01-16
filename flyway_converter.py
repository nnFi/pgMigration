"""
Flyway SQL Converter - MSSQL zu PostgreSQL
Konvertiert SQL-Migration Scripts von MSSQL-Syntax zu PostgreSQL-Syntax
"""

import re
from pathlib import Path
from typing import Tuple, Dict
from type_mappings_manager import load_type_mappings_with_fallback
from collations_manager import load_collations_with_fallback


class FlywayConverter:
    """Konvertiert MSSQL SQL-Scripts zu PostgreSQL"""
    
    def __init__(self, skip_collations=False):
        self.conversion_log = []
        self.skip_collations = skip_collations
        # Lade Datentyp-Mappings aus JSON (mit Fallback aus type_mappings_manager)
        self.type_mappings = load_type_mappings_with_fallback()
        
        # Lade Collations-Mappings wenn nicht übersprungen
        if not skip_collations:
            self.collations = load_collations_with_fallback()
        else:
            self.collations = {}
        
        # Erstelle regex-Mapping für convert_data_types()
        self.TYPE_MAPPING = {}
        for mssql_type, pg_type in self.type_mappings.items():
            # Konvertiere zu Case-Insensitive Regex
            pattern = r'\b' + re.escape(mssql_type) + r'\b'
            self.TYPE_MAPPING[pattern] = pg_type
    
    def convert_file(self, sql_content: str) -> Tuple[str, list]:
        """
        Konvertiere SQL von MSSQL zu PostgreSQL
        
        Returns:
            Tuple[str, list]: Konvertierter SQL und Liste der Änderungen
        """
        self.conversion_log = []
        result = sql_content
        
        # 1. Entferne/Konvertiere GO Statements
        result = self._convert_go_statements(result)
        
        # 2. Konvertiere Datentypen
        result = self._convert_data_types(result)
        
        # 3. Konvertiere Collations (wenn nicht übersprungen)
        if not self.skip_collations and self.collations:
            result = self._convert_collations(result)
        
        # 4. Entferne dbo. Präfixe
        result = self._remove_dbo_prefix(result)
        
        # 5. Konvertiere Bracket-Identifiers [name] → "name"
        result = self._convert_bracket_identifiers(result)
        
        # 6. Konvertiere IDENTITY zu SERIAL/GENERATED ALWAYS AS IDENTITY
        result = self._convert_identity(result)
        
        # 7. Konvertiere Stored Procedures zu Functions
        result = self._convert_procedures_to_functions(result)
        
        # 8. Konvertiere Transactions (BEGIN TRANSACTION → BEGIN)
        result = self._convert_transactions(result)
        
        # 9. Konvertiere Extended Properties zu COMMENT ON Statements
        result = self._convert_extended_properties(result)
        
        # 10. Vereinfache DROP INDEX Statements
        result = self._convert_drop_index(result)
        
        # 11. Vereinfache DROP TABLE / object_id checks
        result = self._convert_object_id_checks(result)
        
        # 12. Konvertiere DEFAULT CURRENT_TIMESTAMP
        result = self._convert_timestamp_defaults(result)
        
        # 13. Entferne MSSQL-spezifische Variablendeklarationen
        result = self._remove_mssql_variables(result)
        
        # 14. Vereinfache IF EXISTS Statements (nur für spezifische sys.*)
        result = self._convert_if_exists_statements(result)
        
        # 15. Räume leere Zeilen und überschüssige Semikolons auf
        result = self._cleanup_empty_lines_and_semicolons(result)
        
        return result, self.conversion_log
    
    def _convert_go_statements(self, sql: str) -> str:
        """Konvertiere GO zu Semikolon"""
        pattern = r'^\s*go\s*$'
        matches = len(re.findall(pattern, sql, re.MULTILINE | re.IGNORECASE))
        if matches > 0:
            self.conversion_log.append(f"GO → ; ({matches} Vorkommen)")
        return re.sub(pattern, ';', sql, flags=re.MULTILINE | re.IGNORECASE)
    
    def _convert_data_types(self, sql: str) -> str:
        """Konvertiere MSSQL Datentypen zu PostgreSQL"""
        result = sql
        for mssql_type, pg_type in self.TYPE_MAPPING.items():
            new_result = re.sub(mssql_type, pg_type, result, flags=re.IGNORECASE)
            if new_result != result:
                self.conversion_log.append(f"{mssql_type} → {pg_type}")
                result = new_result
        return result
    
    def _convert_collations(self, sql: str) -> str:
        """Konvertiere MSSQL Collations zu PostgreSQL"""
        result = sql
        changes_made = 0
        
        # Pattern für COLLATE ... IN ... (z.B. COLLATE LATIN1_GENERAL_CI_AS)
        for mssql_collation, pg_collation in self.collations.items():
            pattern = r'\bCOLLATE\s+' + re.escape(mssql_collation) + r'\b'
            new_result = re.sub(
                pattern,
                f'COLLATE "{pg_collation}"',
                result,
                flags=re.IGNORECASE
            )
            if new_result != result:
                changes_made += 1
                result = new_result
        
        if changes_made > 0:
            self.conversion_log.append(f"Collations konvertiert ({changes_made} Vorkommen)")
        
        return result
    
    def _remove_dbo_prefix(self, sql: str) -> str:
        """Entferne dbo. Präfixe"""
        pattern = r'\bdbo\.'
        matches = len(re.findall(pattern, sql))
        if matches > 0:
            self.conversion_log.append(f"dbo. Präfixe entfernt ({matches} Vorkommen)")
        return re.sub(pattern, '', sql, flags=re.IGNORECASE)
    
    def _convert_drop_index(self, sql: str) -> str:
        """Konvertiere DROP INDEX Statements"""
        pattern = r'drop\s+index\s+if\s+exists\s+(\w+)\s+on\s+(\w+)'
        new_sql = re.sub(
            pattern,
            r'drop index if exists \1',
            sql,
            flags=re.IGNORECASE
        )
        if new_sql != sql:
            self.conversion_log.append("DROP INDEX ... ON TABLE → DROP INDEX")
        return new_sql
    
    def _convert_object_id_checks(self, sql: str) -> str:
        """Vereinfache object_id() Checks"""
        # Entferne komplexe object_id if statements und ersetze mit einfacheren
        pattern = r"if\s+object_id\s*\([^)]+\)[^;]*is\s+not\s+null\s+"
        matches = len(re.findall(pattern, sql, re.IGNORECASE))
        if matches > 0:
            self.conversion_log.append(f"object_id() checks vereinfacht ({matches} Vorkommen)")
        
        # Einfacheres Pattern: entferne IF-Blöcke mit object_id Checks
        result = re.sub(
            r"if\s+object_id\s*\([^)]*\)[^;]*\n\s*",
            "",
            sql,
            flags=re.IGNORECASE
        )
        return result
    
    def _convert_timestamp_defaults(self, sql: str) -> str:
        """Konvertiere DEFAULT CURRENT_TIMESTAMP"""
        pattern = r'default\s+current_timestamp'
        matches = len(re.findall(pattern, sql, re.IGNORECASE))
        if matches > 0:
            self.conversion_log.append(f"DEFAULT CURRENT_TIMESTAMP angepasst ({matches} Vorkommen)")
        return re.sub(pattern, 'default current_timestamp', sql, flags=re.IGNORECASE)
    
    def _remove_mssql_variables(self, sql: str) -> str:
        """Entferne MSSQL-spezifische Variable"""
        pattern = r'declare\s+@\w+\s+\w+\s*=\s*[^;]*;?\n'
        matches = len(re.findall(pattern, sql, re.IGNORECASE))
        if matches > 0:
            self.conversion_log.append(f"MSSQL DECLARE-Statements entfernt ({matches} Vorkommen)")
        return re.sub(pattern, '', sql, flags=re.IGNORECASE)
    
    def _convert_bracket_identifiers(self, sql: str) -> str:
        """Konvertiere Bracket-Identifiers [name] zu "name" """
        pattern = r'\[([^\]]+)\]'
        matches = len(re.findall(pattern, sql))
        if matches > 0:
            self.conversion_log.append(f"Bracket-Identifiers konvertiert ({matches} Vorkommen)")
        return re.sub(pattern, r'"\1"', sql)
    
    def _convert_identity(self, sql: str) -> str:
        """Konvertiere MSSQL IDENTITY zu PostgreSQL GENERATED ALWAYS AS IDENTITY"""
        pattern = r'IDENTITY\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)'
        matches = len(re.findall(pattern, sql, re.IGNORECASE))
        if matches > 0:
            self.conversion_log.append(f"IDENTITY konvertiert zu GENERATED ALWAYS AS IDENTITY ({matches} Vorkommen)")
        return re.sub(pattern, 'GENERATED ALWAYS AS IDENTITY', sql, flags=re.IGNORECASE)
    
    def _convert_procedures_to_functions(self, sql: str) -> str:
        """Konvertiere MSSQL Stored Procedures zu PostgreSQL Functions"""
        # CREATE PROCEDURE → CREATE FUNCTION
        pattern = r'CREATE\s+PROCEDURE\s+'
        matches = len(re.findall(pattern, sql, re.IGNORECASE))
        
        if matches > 0:
            self.conversion_log.append(f"Stored Procedures zu Functions konvertiert ({matches} Vorkommen)")
            
            # Ersetze CREATE PROCEDURE mit CREATE FUNCTION
            result = re.sub(pattern, 'CREATE FUNCTION ', sql, flags=re.IGNORECASE)
            
            # Ersetze AS mit AS $$ (für Function Body)
            # Nur die erste AS nach CREATE FUNCTION
            result = re.sub(
                r'(CREATE\s+FUNCTION\s+\w+(?:\s*\([^)]*\))?\s+RETURNS\s+[^\s]+)\s+AS\s+',
                r'\1 AS $$',
                result,
                flags=re.IGNORECASE
            )
            
            # Füge language plpgsql am Ende hinzu (vor GO oder am Ende der Prozedur)
            result = re.sub(
                r'(\$\$)\s*(GO|;)',
                r'\1 LANGUAGE plpgsql;\2',
                result,
                flags=re.IGNORECASE
            )
            
            return result
        
        return sql
    
    def _convert_transactions(self, sql: str) -> str:
        """Konvertiere Transaction Syntax"""
        # BEGIN TRANSACTION → BEGIN
        pattern = r'BEGIN\s+TRANSACTION'
        matches = len(re.findall(pattern, sql, re.IGNORECASE))
        if matches > 0:
            self.conversion_log.append(f"Transaction-Statements konvertiert ({matches} Vorkommen)")
        return re.sub(pattern, 'BEGIN', sql, flags=re.IGNORECASE)
    
    def _convert_extended_properties(self, sql: str) -> str:
        """Entferne Extended Properties Blöcke - diese sind zu komplex zum automatischen Konvertieren"""
        # Extended Properties sind MSSQL-spezifisch und haben kein direktes PostgreSQL-Äquivalent
        # Entferne diese kompletten IF/BEGIN/END Blöcke komplett
        pattern = r"if\s+exists\s*\(\s*select\s+.*?from\s+sys\.extended_properties.*?\)\s*begin\s+exec\s+sys\.sp_(?:add|update)extendedproperty.*?end\s*else\s*begin\s+exec\s+sys\.sp_(?:add|update)extendedproperty.*?end"
        
        matches = len(re.findall(pattern, sql, re.IGNORECASE | re.DOTALL))
        if matches > 0:
            self.conversion_log.append(f"Extended Properties Blöcke entfernt ({matches}) - manuell prüfen!")
            result = re.sub(pattern, '', sql, flags=re.IGNORECASE | re.DOTALL)
        else:
            result = sql
        
        return result
    
    def _convert_if_exists_statements(self, sql: str) -> str:
        """Vereinfache nur SEHR spezifische unnötige IF EXISTS Statements"""
        # Entferne NUR: if exists (select * from sys.foreign_keys where object_id(...))
        # Lasse ALLES andere, inklusive extended_properties!
        pattern = r'if\s+exists\s*\(\s*select\s+\*\s+from\s+sys\.foreign_keys\s+where\s+object_id.*?\)\s*\n'
        matches = len(re.findall(pattern, sql, re.IGNORECASE | re.DOTALL))
        if matches > 0:
            self.conversion_log.append(f"Unnötige sys.foreign_keys IF EXISTS entfernt ({matches})")
            return re.sub(pattern, '', sql, flags=re.IGNORECASE | re.DOTALL)
        
        return sql
    
    def _cleanup_empty_lines_and_semicolons(self, sql: str) -> str:
        """Räume leere Zeilen und überschüssige Semikolons auf"""
        result = sql
        
        # Ersetze mehrere aufeinanderfolgende Semikolons durch eines (mit Whitespace)
        pattern_multiple_semicolons = r';\s*;\s*'
        matches_semicolons = len(re.findall(pattern_multiple_semicolons, result))
        if matches_semicolons > 0:
            result = re.sub(pattern_multiple_semicolons, ';\n', result)
        
        # Ersetze mehrere aufeinanderfolgende leere Zeilen durch eine
        pattern_empty_lines = r'\n\s*\n\s*\n+'
        result = re.sub(pattern_empty_lines, '\n\n', result)
        
        # Entferne Zeilen die NUR Whitespace + Semikolon haben (ohne Code)
        # aber behalte Semikolons nach Code-Statements
        lines = result.split('\n')
        cleaned_lines = []
        for line in lines:
            # Ignoriere reine Semikolon-Zeilen
            if line.strip() == ';':
                continue
            cleaned_lines.append(line)
        
        result = '\n'.join(cleaned_lines)
        
        # Entferne führende und abschließende leere Zeilen
        result = result.strip()
        
        if matches_semicolons > 0:
            self.conversion_log.append(f"Mehrfache Semikolons bereinigt ({matches_semicolons} Vorkommen)")
        
        return result


def convert_flyway_scripts(source_dir: Path, target_dir: Path, log_callback=None, skip_collations=False) -> dict:
    """
    Konvertiere alle SQL-Dateien von Quelle nach Ziel
    
    Args:
        source_dir: Quellverzeichnis mit MSSQL Scripts
        target_dir: Zielverzeichnis (wird erstellt/geleert)
        log_callback: Callback für Logging
        skip_collations: Wenn True, werden Collations nicht konvertiert
    
    Returns:
        dict: Statistiken und Ergebnisse
    """
    source_dir = Path(source_dir)
    target_dir = Path(target_dir)
    
    if not source_dir.exists():
        return {"error": f"Quellverzeichnis existiert nicht: {source_dir}"}
    
    # Erstelle Zielverzeichnis
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # Leere Zielverzeichnis vollständig (inkl. Subdirectories)
    for item in target_dir.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            import shutil
            shutil.rmtree(item)
    
    converter = FlywayConverter(skip_collations=skip_collations)
    results = {
        "converted": 0,
        "failed": 0,
        "files": [],
        "total_changes": 0
    }
    
    # Verarbeite alle SQL-Dateien
    for sql_file in sorted(source_dir.glob("*.sql")):
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            converted_content, changes = converter.convert_file(original_content)
            
            # Schreibe konvertierte Datei
            target_file = target_dir / sql_file.name
            with open(target_file, 'w', encoding='utf-8') as f:
                f.write(converted_content)
            
            results["converted"] += 1
            results["files"].append({
                "name": sql_file.name,
                "changes": len(changes),
                "details": changes
            })
            results["total_changes"] += len(changes)
            
            if log_callback:
                log_callback(f"{sql_file.name} konvertiert ({len(changes)} Änderungen)")
        
        except Exception as e:
            results["failed"] += 1
            if log_callback:
                log_callback(f"{sql_file.name} FEHLER: {str(e)}")
    
    return results
