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
        
        NEUE STRATEGIE:
        1. Erkenne Blöcke (DO $$, Variablendeklarationen, IF-Logik)
        2. Konvertiere Inhalte der Blöcke (T-SQL → PL/pgSQL)
        3. Halte einfache DDL AUSSERHALB von Blöcken
        
        Returns:
            Tuple[str, list]: Konvertierter SQL und Liste der Änderungen
        """
        self.conversion_log = []
        result = sql_content
        
        # SCHRITT 1: Basis-Konvertierungen (überall gültig)
        # ===================================================
        
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
        
        # SCHRITT 2: DO-Block spezifische Konvertierungen
        # ================================================
        
        # Erkenne ob DO $$ Blöcke vorhanden sind
        has_do_blocks = 'DO $$' in result
        
        if has_do_blocks:
            # Konvertiere INNEN von DO Blöcken
            result = self._convert_inside_do_blocks(result)
        else:
            # WICHTIG: T-SQL IF EXISTS/BEGIN/END außerhalb von DO Blöcken
            # müssen in DO Blöcke gewrapped werden, weil sie in PostgreSQL ungültig sind!
            result = self._wrap_if_exists_in_do_blocks(result)
            
            # 6. Konvertiere IDENTITY zu SERIAL/GENERATED ALWAYS AS IDENTITY
            result = self._convert_identity(result)
        
        # SCHRITT 3: Allgemeine DDL-Cleanups
        # ===================================
        
        # 7. Stored Procedures zu Functions (falls vorhanden)
        result = self._convert_procedures_to_functions(result)
        
        # 8. Transactions
        result = self._convert_transactions(result)
        
        # 9. Extended Properties
        result = self._convert_extended_properties(result)
        
        # 10. Entferne parent_object_id Checks
        result = self._remove_parent_object_id(result)
        
        # 11. DROP INDEX
        result = self._convert_drop_index(result)
        
        # 12. DROP CONSTRAINT
        result = self._convert_drop_constraint(result)
        
        # 15. Verbessere DROP Statements (IF EXISTS, CASCADE)
        result = self._improve_drop_statements(result)
        
        # 16. object_id Checks
        result = self._convert_object_id_checks(result)
        
        # 14. CURRENT_TIMESTAMP
        result = self._convert_timestamp_defaults(result)
        
        # 16. IF EXISTS Cleanup
        result = self._convert_if_exists_statements(result)
        
        # 17. Konvertiere Keywords und Identifiers zu lowercase
        result = self._lowercase_keywords_and_identifiers(result)
        
        # 18. Formatiere SQL für bessere Lesbarkeit
        result = self._format_sql_output(result)
        
        # 19. Cleanup
        result = self._cleanup_empty_lines_and_semicolons(result)
        
        return result, self.conversion_log
    
    def _wrap_if_exists_in_do_blocks(self, sql: str) -> str:
        """
        Konvertiere T-SQL IF/BEGIN/END Blöcke zu PostgreSQL
        
        WICHTIG: PostgreSQL kann KEINE DDL (DROP INDEX, DROP TABLE) direkt in DO-Blöcken ausführen!
        
        Strategie:
        - IF-Blöcke die NUR DDL enthalten → direkt außerhalb DO ausführen
        - IF-Blöcke die NUR geschäftslogik enthalten → in DO wrappen
        - If-Blöcke die DDL UND logik enthalten → DDL raus, logik ins DO
        
        T-SQL:
            declare @DropDb boolean = 1;
            if @DropDb = 1 begin
                DROP TABLE tbl1;
            end
        
        PostgreSQL (RICHTIG):
            DROP TABLE IF EXISTS tbl1;
        
        (Weil @DropDb=1 immer true, brauchen wir keine IF!)
        """
        # Pattern: DECLARE boolean = 1; IF @var = 1 BEGIN (nur DDL statements) END
        # Wenn die IF-Variable immer wahr ist (= 1/true), können wir die IF weglassen
        declare_if_ddl_pattern = r'declare\s+@(\w+)\s+boolean\s*=\s*1;.*?if\s+@\w+\s*=\s*1\s*begin\s+((?:(?:drop|alter|create)\s+(?:table|index|constraint).*?;(?:\s|$))+)\s*end'
        
        def convert_declare_if_ddl(match):
            body = match.group(1)
            # Normalize each DDL statement
            lines = []
            for line in body.split('\n'):
                line = line.strip()
                if not line:
                    continue
                line_lower = line.lower()
                # Add IF EXISTS if not present for DROP
                if 'drop' in line_lower and 'if exists' not in line_lower:
                    line = re.sub(r'drop\s+(\w+)\s+', lambda m: f'drop {m.group(1)} if exists ', line, flags=re.IGNORECASE)
                # Add CASCADE for DROP TABLE if not present
                if 'drop table' in line_lower and 'cascade' not in line_lower and line.endswith(';'):
                    line = line[:-1] + ' cascade;'
                lines.append(line)
            
            result = '\n'.join(lines)
            self.conversion_log.append("[OK] DECLARE + IF (DDL-only) -> Direct DDL")
            return result
        
        result = re.sub(declare_if_ddl_pattern, convert_declare_if_ddl, sql, flags=re.DOTALL | re.IGNORECASE)
        
        # Pattern 1: IF EXISTS ... BEGIN ... END
        pattern_if_exists = r'IF\s+EXISTS\s*\(([^)]+)\)\s*BEGIN\s+(.*?)\s*\bend\b(?!\s+IF)'
        
        def wrap_if_exists_block(match):
            condition = match.group(1).strip()
            body = match.group(2).strip()
            
            # Nur dbo. entfernen und Bracket identifiers konvertieren
            body = re.sub(r'\bdbo\.', '', body, flags=re.IGNORECASE)
            body = re.sub(r'\[(\w+)\]', r'\1', body, flags=re.IGNORECASE)
            
            # Wickle in DO Block ein
            wrapped = f"""DO $$
BEGIN
    IF EXISTS ({condition}) THEN
        {body};
    END IF;
END $$;"""
            
            self.conversion_log.append("[OK] IF EXISTS -> DO $$ Block")
            return wrapped
        
        result = re.sub(pattern_if_exists, wrap_if_exists_block, result, flags=re.DOTALL | re.IGNORECASE)
        
        return result
    
    def _convert_inside_do_blocks(self, sql: str) -> str:
        """
        Konvertiere MSSQL Syntax INNEN von DO $$ ... $$ Blöcken
        
        Diese Konvertierungen sind NUR valide innerhalb von DO/Function-Blöcken!
        """
        # Pattern: DO $$ ... $$;
        pattern = r'DO\s*\$\$\s*(.*?)\s*\$\$\s*;?'
        
        def convert_do_block_content(match):
            block_content = match.group(1)
            content = block_content
            
            # ============= STEP 1: Remove @ symbols everywhere first =============
            content = re.sub(r'@(\w+)', r'\1', content)
            
            # ============= STEP 2: Convert BOOLEAN values and lowercase everything =============
            # DECLARE varname BOOLEAN = 1; → DECLARE varname boolean := true;
            def convert_declare(m):
                decl_line = m.group(0)
                # Make everything lowercase
                decl_line_lower = decl_line.lower()
                # Replace = with :=
                decl_line_lower = decl_line_lower.replace(' = ', ' := ')
                # Replace 1 with true, 0 with false (for boolean)
                decl_line_lower = re.sub(r':= 1\b', ':= true', decl_line_lower)
                decl_line_lower = re.sub(r':= 0\b', ':= false', decl_line_lower)
                return decl_line_lower
            
            # Match DECLARE lines and convert them properly
            def format_declare(m):
                decl_line = m.group(0)
                # Make content lowercase but keep DECLARE uppercase
                # Format: DECLARE varname type := value;
                match = re.match(r'(DECLARE)\s+(\w+)\s+(\w+)\s*=\s*([^;]*);', decl_line, re.IGNORECASE)
                if match:
                    varname = match.group(2).lower()
                    vartype = match.group(3).lower()
                    value = match.group(4).strip()
                    # Convert 1 to true, 0 to false
                    if value == '1':
                        value = 'true'
                    elif value == '0':
                        value = 'false'
                    return f'DECLARE {varname} {vartype} := {value};'
                return decl_line.lower()
            
            content = re.sub(r'DECLARE\s+\w+\s+\w+\s*=\s*[^;]*;', format_declare, content, flags=re.IGNORECASE)
            
            # ============= STEP 3: Convert IF statements =============
            # IF varname = 1 BEGIN → IF varname THEN (for boolean)
            content = re.sub(r'IF\s+(\w+)\s*=\s*1\s+BEGIN', 
                           lambda m: f"IF {m.group(1).lower()} THEN", 
                           content, flags=re.IGNORECASE)
            
            # Generic IF condition BEGIN → IF condition THEN
            content = re.sub(r'IF\s+(.+?)\s+BEGIN', 
                           lambda m: f"IF {m.group(1).strip().lower()} THEN", 
                           content, flags=re.IGNORECASE | re.DOTALL)
            
            # ============= STEP 4: END → END IF;  =============
            # DECLARE var TYPE = value; → DECLARE var TYPE := value;
            # ONLY im DECLARE Statement
            def fix_declare(m):
                decl = m.group(0)
                # Ersetze '=' mit ':=' aber NUR bei erster Zuordnung
                parts = decl.split('=', 1)
                if len(parts) == 2:
                    # Stellersichere, dass Spacing gut ist: := mit Spaces
                    return parts[0].rstrip() + ' := ' + parts[1].lstrip()
                return decl
            
            # Match: DECLARE varname TYPE = value;
            content = re.sub(r'DECLARE\s+\w+\s+\w+\s*=\s*[^;]*;', fix_declare, content, flags=re.IGNORECASE)
            
            # ============= STEP 3: Konvertiere IF/BEGIN zu IF/THEN =============
            # Special case: IF varname = 1 THEN → IF varname THEN (for boolean)
            content = re.sub(
                r'\bIF\s+(\w+)\s*=\s*1\s+BEGIN',
                lambda m: f"IF {m.group(1).strip()} THEN",
                content,
                flags=re.IGNORECASE | re.DOTALL
            )
            # General: IF condition BEGIN → IF condition THEN
            content = re.sub(
                r'\bIF\s+(.+?)\s+BEGIN',
                lambda m: f"IF {m.group(1).strip()} THEN",
                content,
                flags=re.IGNORECASE | re.DOTALL
            )
            
            # ============= STEP 4: END → END IF; (intelligent) =============
            lines = content.split('\n')
            new_lines = []
            in_if_block = False
            if_depth = 0
            
            for i, line in enumerate(lines):
                stripped = line.strip().upper()
                
                # Track IF blocks
                if 'IF ' in stripped and 'THEN' in stripped:
                    in_if_block = True
                    if_depth += 1
                
                # Handle END
                if stripped == 'END' or stripped == 'END;':
                    if in_if_block and if_depth > 0:
                        # Replace END with END IF;
                        new_lines.append(re.sub(r'\bEND\s*;?$', 'END IF;', line, flags=re.IGNORECASE))
                        if_depth -= 1
                        if if_depth == 0:
                            in_if_block = False
                    else:
                        # Keep END as is (end of BEGIN block)
                        new_lines.append(line)
                else:
                    new_lines.append(line)
            
            content = '\n'.join(new_lines)
            
            # ============= STEP 5: Remove extra BEGIN statements =============
            # BEGIN nach DO $$ bleibt, aber BEGIN nach IF sollte weg sein (durch THEN ersetzt)
            content = re.sub(r'THEN\s+BEGIN', r'THEN', content, flags=re.IGNORECASE)
            
            self.conversion_log.append("[OK] DO $$ Block konvertiert (T-SQL -> PL/pgSQL)")
            
            return f"DO $$\n{content}\n$$;"
        
        result = re.sub(pattern, convert_do_block_content, sql, flags=re.DOTALL | re.IGNORECASE)
        
        return result
    
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
        
        # Zuerst: BYTEA(max) → BYTEA (vor allgemeinem BYTEA Pattern)
        pattern_bytea_max = r'BYTEA\s*\(\s*max\s*\)'
        matches_bytea = len(re.findall(pattern_bytea_max, result, re.IGNORECASE))
        if matches_bytea > 0:
            result = re.sub(pattern_bytea_max, 'BYTEA', result, flags=re.IGNORECASE)
            self.conversion_log.append(f"BYTEA(max) → BYTEA ({matches_bytea} Vorkommen)")
        
        # VARCHAR(MAX) → TEXT
        pattern_varchar_max = r'VARCHAR\s*\(\s*MAX\s*\)'
        matches_varchar = len(re.findall(pattern_varchar_max, result, re.IGNORECASE))
        if matches_varchar > 0:
            result = re.sub(pattern_varchar_max, 'TEXT', result, flags=re.IGNORECASE)
            self.conversion_log.append(f"VARCHAR(MAX) → TEXT ({matches_varchar} Vorkommen)")
        
        # Dann: Andere Datentypen aus TYPE_MAPPING
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
    
    def _convert_drop_constraint(self, sql: str) -> str:
        """Konvertiere DROP CONSTRAINT Statements zu PostgreSQL Format"""
        result = sql
        
        # Format 1: "alter table X drop constraint Y;" (bereits als ALTER TABLE)
        # Dies ist der häufigere Fall
        pattern_alter = r"alter\s+table\s+(\w+)\s+drop\s+constraint\s+(\w+)\s*;"
        matches_alter = len(re.findall(pattern_alter, result, re.IGNORECASE))
        
        if matches_alter > 0:
            # Konvertiere zu PostgreSQL Format mit IF EXISTS und Kleinbuchstaben
            result = re.sub(
                pattern_alter,
                lambda m: f'ALTER TABLE {m.group(1).lower()} DROP CONSTRAINT IF EXISTS {m.group(2).lower()};',
                result,
                flags=re.IGNORECASE
            )
            self.conversion_log.append(f"ALTER TABLE DROP CONSTRAINT konvertiert ({matches_alter})")
        
        # Format 2: "drop constraint FK_NAME;" (OHNE ALTER TABLE vorhanden)
        # Verwende Split-Ansatz statt komplexes Lookbehind Regex
        lines = result.split('\n')
        new_lines = []
        
        for line in lines:
            # Prüfe ob die Linie ist: "DROP CONSTRAINT ..." UND nicht "ALTER TABLE"
            if 'DROP CONSTRAINT' in line.upper() and 'ALTER TABLE' not in line.upper():
                # Ersetze: DROP CONSTRAINT FK_Name; → ALTER TABLE table_name DROP CONSTRAINT IF EXISTS fk_name;
                new_line = re.sub(
                    r'drop\s+constraint\s+(\w+)\s*;?',
                    lambda m: f'ALTER TABLE table_name DROP CONSTRAINT IF EXISTS {m.group(1).lower()};',
                    line,
                    flags=re.IGNORECASE
                )
                new_lines.append(new_line)
            else:
                new_lines.append(line)
        
        result = '\n'.join(new_lines)
        
        matches_simple = result.count('table_name')  # Crude check
        if 'DROP CONSTRAINT' in sql and 'ALTER TABLE' not in sql:
            self.conversion_log.append(f"DROP CONSTRAINT (ohne ALTER TABLE) in DO Block konvertiert")
        
        return result
    
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
    
    def _improve_drop_statements(self, sql: str) -> str:
        """
        Verbessere DROP Statements:
        - DROP TABLE → DROP TABLE IF EXISTS
        - Füge CASCADE hinzu
        - Table names zu lowercase
        """
        result = sql
        
        # Pattern 1: DROP TABLE name; → DROP TABLE IF EXISTS name CASCADE;
        # Nur wenn nicht schon IF EXISTS
        pattern = r'\bDROP\s+TABLE\s+(?!IF\s+EXISTS)(\w+)\s*;'
        matches = len(re.findall(pattern, result, re.IGNORECASE))
        if matches > 0:
            result = re.sub(
                pattern,
                lambda m: f'DROP TABLE IF EXISTS {m.group(1).lower()} CASCADE;',
                result,
                flags=re.IGNORECASE
            )
            self.conversion_log.append(f"DROP TABLE-Statements verbessert ({matches} Vorkommen)")
        
        # Pattern 2: DROP INDEX name; → DROP INDEX IF EXISTS name;
        pattern_idx = r'\bDROP\s+INDEX\s+(?!IF\s+EXISTS)(\w+)\s*;'
        matches_idx = len(re.findall(pattern_idx, result, re.IGNORECASE))
        if matches_idx > 0:
            result = re.sub(
                pattern_idx,
                lambda m: f'DROP INDEX IF EXISTS {m.group(1).lower()};',
                result,
                flags=re.IGNORECASE
            )
            self.conversion_log.append(f"DROP INDEX-Statements verbessert ({matches_idx} Vorkommen)")
        
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
    
    def _convert_mssql_if_syntax(self, sql: str) -> str:
        """Konvertiere MSSQL T-SQL IF/BEGIN/END zu PostgreSQL PL/pgSQL IF/THEN/END IF"""
        result = sql
        
        # Pattern 1: IF @var = value BEGIN ... END → IF var = value THEN ... END IF;
        pattern1 = r'IF\s+@(\w+)\s*=\s*(.+?)\s+BEGIN'
        matches1 = len(re.findall(pattern1, result, re.IGNORECASE | re.DOTALL))
        if matches1 > 0:
            result = re.sub(pattern1, r'IF \1 = \2 THEN', result, flags=re.IGNORECASE | re.DOTALL)
            self.conversion_log.append(f"IF @var = value konvertiert ({matches1})")
        
        # Pattern 2: IF condition BEGIN ... END → IF condition THEN ... END IF;
        pattern2 = r'IF\s+(.+?)\s+BEGIN'
        matches2 = len(re.findall(pattern2, result, re.IGNORECASE | re.DOTALL))
        if matches2 > 0:
            result = re.sub(pattern2, r'IF \1 THEN', result, flags=re.IGNORECASE | re.DOTALL)
        
        # END → END IF; (nur wenn nicht schon IF)
        result = re.sub(r'\bEND\b(?!\s+IF)', 'END IF;', result, flags=re.IGNORECASE)
        
        if matches1 > 0 or matches2 > 0:
            self.conversion_log.append(f"T-SQL IF/BEGIN/END zu PL/pgSQL IF/THEN/END IF konvertiert")
        
        return result
    
    def _remove_parent_object_id(self, sql: str) -> str:
        """Entferne MSSQL parent_object_id Checks"""
        pattern = r'\s*and\s+parent_object_id\s*=\s*object_id\s*\([^)]*\)'
        matches = len(re.findall(pattern, sql, re.IGNORECASE))
        
        if matches > 0:
            result = re.sub(pattern, '', sql, flags=re.IGNORECASE)
            self.conversion_log.append(f"parent_object_id Checks entfernt ({matches})")
            return result
        
        return sql
    
    def _convert_mssql_variables(self, sql: str) -> str:
        """Konvertiere T-SQL @Variablen zu PostgreSQL Variablen (ohne @)"""
        # Pattern: DECLARE @var TYPE = value;
        # Zu: DECLARE var type := value;
        
        # Nur innerhalb von DO $$ Blöcken konvertieren
        pattern_var = r'@(\w+)'
        matches = len(re.findall(pattern_var, sql))
        
        if matches > 0:
            # Ersetze @var mit var (nur wenn in DECLARE oder Bedingungen)
            result = re.sub(pattern_var, r'\1', sql)
            self.conversion_log.append(f"@Variablen konvertiert ({matches} Vorkommen)")
            return result
        
        return sql
    def _convert_procedures_to_functions(self, sql: str) -> str:
        """Konvertiere MSSQL Stored Procedures zu PostgreSQL Functions"""
        # CREATE PROCEDURE → CREATE FUNCTION
        pattern = r'CREATE\s+PROCEDURE\s+'
        matches = len(re.findall(pattern, sql, re.IGNORECASE))
        
        if matches > 0:
            self.conversion_log.append(f"Stored Procedures zu Functions konvertiert ({matches} Vorkommen)")
            
            # Ersetze CREATE PROCEDURE mit CREATE FUNCTION
            result = re.sub(pattern, 'CREATE FUNCTION ', sql, flags=re.IGNORECASE)
            
            # WICHTIG: Entferne @-Symbole in PARAMETERS
            # Pattern: @ParameterName TYPE wird zu ParameterName TYPE
            result = re.sub(r'@(\w+)', r'\1', result)
            
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
            
            # Konvertiere Function-Body Syntax:
            # - DECLARE → DECLARE (bleibt gleich in PostgreSQL)
            # - BEGIN...END bleibt für Function Body
            # Allerdings: Wenn IF/THEN in Body vorhanden, müssen sie konvertiert sein
            # Das wird aber NICHT hier gemacht, da Functions nicht durch DECLARE-logik gehen
            
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
        """Vereinfache IF EXISTS Statements die sys.* objects prüfen"""
        pattern = r"IF\s+EXISTS\s*\(SELECT\s+1\s+FROM\s+sys\.\w+\s+WHERE"
        if re.search(pattern, sql, re.IGNORECASE):
            self.conversion_log.append("IF EXISTS (sys.*) Statements erkannt")
        return sql
    
    def _lowercase_keywords_and_identifiers(self, sql: str) -> str:
        """Konvertiere SQL Keywords und Identifier zu lowercase für PostgreSQL"""
        result = sql
        
        # Keywords im CREATE TABLE / Definitions kontext zu lowercase
        keywords_in_create = [
            ('PRIMARY KEY', 'primary key'),
            ('FOREIGN KEY', 'foreign key'),
            ('UNIQUE', 'unique'),
            ('NOT NULL', 'not null'),
            ('DEFAULT', 'default'),
            ('CHECK', 'check'),
            ('DROP TABLE', 'drop table'),
            ('DROP INDEX', 'drop index'),
            ('DROP CONSTRAINT', 'drop constraint'),
            ('ALTER TABLE', 'alter table'),
            ('CREATE TABLE', 'create table'),
        ]
        
        for upper, lower in keywords_in_create:
            result = re.sub(rf'\b{upper}\b', lower, result, flags=re.IGNORECASE)
        
        # Konvertiere Data Types zu lowercase
        data_types = [
            'VARCHAR', 'CHAR', 'TEXT', 'BYTEA', 
            'INTEGER', 'SMALLINT', 'BIGINT', 
            'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'PRECISION',
            'BOOLEAN', 'TIMESTAMP', 'DATE', 'TIME'
        ]
        
        for dtype in data_types:
            result = re.sub(rf'\b{dtype}\b', dtype.lower(), result, flags=re.IGNORECASE)
        
        # Lowercase table names in CREATE TABLE statements
        # Remove unnecessary quotes around lowercase-only names
        # Pattern: create table "lowercase_name" → create table lowercase_name
        result = re.sub(
            r'(create\s+table\s+)"([a-z_][a-z0-9_]*)"(\s*\()',
            r'\1\2\3',
            result,
            flags=re.IGNORECASE
        )
        
        # Also convert remaining uppercase table names to lowercase without quotes
        # Pattern: create table TableName → create table tablename
        result = re.sub(
            r'(create\s+table\s+)([A-Z][A-Z0-9_]*)(\s*\()',
            lambda m: f'{m.group(1)}{m.group(2).lower()}{m.group(3)}',
            result,
            flags=re.IGNORECASE
        )
        
        # Lowercase column names in CREATE TABLE definitions
        # Pattern: column_name type constraints
        result = re.sub(
            r'^\s+([A-Z_][A-Z0-9_]*)\s+(varchar|char|text|bytea|integer|smallint|bigint|decimal|numeric|float|double|boolean|timestamp|date|time)',
            lambda m: f'    {m.group(1).lower()} {m.group(2).lower()}',
            result,
            flags=re.IGNORECASE | re.MULTILINE
        )
        
        # CASCADE, IF EXISTS, THEN, END IF should be lowercase
        cascade_patterns = [
            (r'\bCASCADE\b', 'cascade'),
            (r'\bIF\s+EXISTS\b', 'if exists'),
            (r'\bTHEN\b', 'then'),
            (r'\bEND\s+IF\b', 'end if'),
        ]
        
        for pattern, replacement in cascade_patterns:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
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
    
    def _format_sql_output(self, sql: str) -> str:
        """
        Formatiere SQL für bessere Lesbarkeit:
        - Ordne DROP INDEX/TABLE Statements
        - Füge Comments hinzu
        - Bessere Indentation in DO Blöcken
        """
        lines = sql.split('\n')
        formatted_lines = []
        in_do_block = False
        indent_level = 0
        
        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            
            # Track DO block entry/exit
            if 'DO $$' in line:
                in_do_block = True
                formatted_lines.append(line)
                i += 1
                continue
            
            if in_do_block and line.strip() == '$$;':
                in_do_block = False
                formatted_lines.append(line)
                i += 1
                continue
            
            # Group and comment DROP INDEX statements
            if in_do_block and stripped.upper().startswith('DROP INDEX'):
                # Sammle alle DROP INDEX Statements
                drop_indexes = []
                while i < len(lines):
                    current = lines[i].strip()
                    if current.upper().startswith('DROP INDEX'):
                        drop_indexes.append(current)
                        i += 1
                    else:
                        break
                
                if drop_indexes:
                    formatted_lines.append('    -- Drop indexes')
                    for idx_stmt in drop_indexes:
                        formatted_lines.append(f'    {idx_stmt}')
                continue
            
            # Group and comment ALTER TABLE statements
            if in_do_block and stripped.upper().startswith('ALTER TABLE'):
                # Sammle alle ALTER TABLE DROP CONSTRAINT
                alter_stmts = []
                while i < len(lines):
                    current = lines[i].strip()
                    if current.upper().startswith('ALTER TABLE'):
                        alter_stmts.append(current)
                        i += 1
                    else:
                        break
                
                if alter_stmts:
                    formatted_lines.append('    -- Drop constraints')
                    for alter_stmt in alter_stmts:
                        formatted_lines.append(f'    {alter_stmt}')
                continue
            
            # Group and comment DROP TABLE statements
            if in_do_block and stripped.upper().startswith('DROP TABLE'):
                # Sammle alle DROP TABLE
                drop_tables = []
                while i < len(lines):
                    current = lines[i].strip()
                    if current.upper().startswith('DROP TABLE'):
                        drop_tables.append(current)
                        i += 1
                    else:
                        break
                
                if drop_tables:
                    formatted_lines.append('    -- Drop tables')
                    for tbl_stmt in drop_tables:
                        # Addiere CASCADE wenn nicht vorhanden
                        if 'CASCADE' not in tbl_stmt.upper():
                            tbl_stmt = tbl_stmt.rstrip(';') + ' CASCADE;'
                        formatted_lines.append(f'    {tbl_stmt}')
                continue
            
            # Normale Lines
            formatted_lines.append(line)
            i += 1
        
        result = '\n'.join(formatted_lines)
        self.conversion_log.append("SQL formatiert (Gruppierung, Comments, Indentation)")
        return result
    
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
    
    def _wrap_ddl_in_do_block(self, sql: str) -> str:
        """
        PostgreSQL DO $$ Blöcke: NICHT für einfache DDL!
        
        ⚠️ KRITISCH: CREATE TABLE, ALTER TABLE, DROP TABLE sind VALIDE SQL ohne DO $$!
        
        DO $$ Blöcke sind ERST nötig wenn:
        - Variablen (DECLARE) vorhanden
        - Komplexe Logik (IF/THEN mit Business-Logik)
        - Schleifen oder Conditions
        
        Einfache Statements: DIREKT ausführen ohne DO $$!
        """
        # Diese Methode ist DEPRECATED - DO Blöcke sollten NICHT automatisch angelegt werden
        # Sie können am Ende des Prozesses manuell hinzugefügt werden wenn wirklich nötig
        return sql
    
    def _create_do_block(self, statements: list) -> list:
        """Erstelle einen DO $$ BEGIN ... END $$; Block aus Statements (DEPRECATED)"""
        # Nicht mehr in Verwendung - siehe _wrap_ddl_in_do_block()
        block = [
            'DO $$',
            'BEGIN'
        ]
        
        # Indentiere alle Statements
        for stmt in statements:
            if stmt.strip() and not stmt.strip().startswith('--'):
                block.append('    ' + stmt)
            else:
                block.append(stmt)
        
        block.extend([
            'END',
            '$$;'
        ])
        
        return block



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
