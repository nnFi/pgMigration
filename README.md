# MSSQL zu PostgreSQL Migration

Professionelle Migrationsanwendung zur Konvertierung von Microsoft SQL Server Datenbanken nach PostgreSQL mit benutzerfreundlicher GUI und portabler .exe-Version.

---

## 📋 Inhaltsverzeichnis

1. [Installation](#installation)
2. [Konfiguration](#konfiguration)
3. [Verwendung](#verwendung)
4. [GUI Anwendung](#gui-anwendung)
5. [Flyway SQL Converter](#flyway-sql-converter-neu)
6. [Datentyp-Mappings Konfiguration](#datentyp-mappings-konfiguration)
7. [Portable Version (USB)](#portable-version-usb)
8. [Technische Details](#technische-details)
9. [Lizenz & Hinweis](#lizenz--hinweis)

---

## Installation

### Voraussetzungen

- Python 3.8+

### Setup

```bash
# Dependencies installieren
pip install -r requirements.txt

# Oder einzeln für minimale Installation:
pip install pyodbc psycopg2-binary python-dotenv PyQt6
```

---

## Konfiguration

### .env Datei

Die Konfiguration erfolgt über eine `.env`-Datei im Stammverzeichnis:

```env
# MSSQL Server
MSSQL_SERVER=your-server.example.com
MSSQL_PORT=1433
MSSQL_DATABASE=your_database
MSSQL_USER=sa
MSSQL_PASSWORD=your_password

# PostgreSQL Ziel
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=target_database
PG_USER=postgres
PG_PASSWORD=postgres_password

# Migration Settings
LOG_LEVEL=DEBUG
MIGRATE_DATA=true
IDENTITY_ALWAYS=false
```

**Hinweise:**
- `MSSQL_PORT` ist optional (Standard: 1433)
- Sie können die .env-Datei direkt bearbeiten oder über die GUI konfigurieren
- In der GUI können Sie die Einstellungen auch importieren und exportieren

---

## Verwendung

### Mit GUI (empfohlen) ✅

Die GUI ist die einfachste Methode für Datenbankmigrationen:

```bash
python migration_gui.py
```

#### GUI Features

✅ **Datenbankverbindungen konfigurieren**
- MSSQL und PostgreSQL Eingabefelder
- Passwort anzeigen/verstecken mit 👁-Button
- Verbindungstests durchführen

✅ **Konfiguration verwalten**
- .env-Dateien importieren
- Einstellungen speichern/laden
- Export für Sicherung oder Weiterverteilung

✅ **Migration durchführen**
- Alle Schritte auf einmal ausführen
- Oder einzelne Schritte selektiv

✅ **Echtzeit-Monitoring**
- Live-Log-Ausgabe während Migration
- Progress Bar mit Fortschrittsanzeige
- Status-Meldungen und Fehlerbenachrichtigungen

✅ **Debug & Analyse**
- Exportiert alle Debug-Logs aus der Migration
- Column-Mapping Analyse anzeigen
- JSON-Export mit Detailinformationen

✅ **Collations konfigurieren** ⚙️
- Button "⚙️ Collations konfigurieren" öffnet `collations_config.json`
- Definieren Sie Ihr eigenes MSSQL → PostgreSQL Collations-Mapping
- Automatische Konvertierung beim Start wenn nicht vorhanden
- Step 4 nutzt automatisch Ihre Custom-Mappings

✅ **Datentyp-Mappings bearbeiten** 🔄
- Button "🔄 Datentypen bearbeiten" öffnet interaktiven Editor
- Alle 28 MSSQL → PostgreSQL Datentyp-Mappings anpassbar
- Live-Änderungen werden automatisch gespeichert
- Wird von Step1 und Flyway-Converter verwendet

✅ **Flyway SQL Scripts konvertieren** ✨ (NEU)
- Konvertiert MSSQL SQL-Scripts zu PostgreSQL-Syntax
- Support für 40+ MSSQL → PostgreSQL Datentypen
- Automatische Konvertierung von Statements und Funktionen
- Logs mit Detailinformationen über alle Änderungen

✅ **Sicherheit**
- Warnung bei Einzelschritt-Ausführung (richtige Reihenfolge wichtig!)
- Verbindungstests vor Migration
- Optional: Step 4 (Collations) bei Migration überspringen

### Kommandozeile

Für Automatisierung oder Server-Deployments:

**Komplette Migration**
```bash
python run_all.py
```

**Einzelne Schritte**
```bash
python step1_migrate_data.py        # Tabellen und Daten
python step2_verify_columns.py      # Verifizierung
python step3_migrate_constraints.py # Constraints und Keys
python step4_migrate_collations.py  # Collations (optional)
```

⚠️ **Wichtig:** Die Schritte müssen in der richtigen Reihenfolge ausgeführt werden!

---

## Flyway SQL Converter (NEU) ✨

Der **Flyway SQL Converter** konvertiert MSSQL SQL-Scripts automatisch zu PostgreSQL-Syntax. Dies ist besonders nützlich für Datenbankmigrationen, bei denen Sie vorhandene SQL-Scripts anpassen müssen.

### Features

✅ **40+ MSSQL → PostgreSQL Datentypen**
- Vollständige Datentyp-Konvertierung
- Unterstützt komplexe Typen (decimal, numeric, varchar(max), etc.)
- Konfigurierbar über Datentyp-Mappings Editor

✅ **SQL-Syntax Konvertierungen**
- `GO` Statements in PostgreSQL Syntax
- `dbo.` Präfixe entfernen
- `DROP INDEX` vereinfachen
- `OBJECT_ID` Checks konvertieren
- `GETDATE()` zu `CURRENT_TIMESTAMP`
- `NEWID()` zu `gen_random_uuid()`
- `DEFAULT CURRENT_TIMESTAMP` anpassen
- `IF EXISTS` Statements konvertieren

✅ **Detailliertes Logging**
- Logs zeigen alle durchgeführten Konvertierungen
- Fehlerberichte für problematische Scripts
- Dateiweise Änderungsübersicht
- Export der Logs möglich

### Verwendung in der GUI

1. **Flyway Sektion** unten in der Migration GUI öffnen
2. **Quellverzeichnis wählen** - Verzeichnis mit MSSQL .sql-Dateien
3. **Zielverzeichnis wählen** - Wo die konvertierten Dateien gespeichert werden
4. **"Konvertiere SQL-Scripts" Button** klicken
5. **Ergebnisse prüfen** - Live-Log zeigt alle Änderungen
6. **Logs exportieren** - Optional zum Weiterverarbeiten speichern

### Kommandozeile Verwendung

```python
from flyway_converter import convert_flyway_scripts

result = convert_flyway_scripts(
    source_dir='./sql_scripts',
    target_dir='./sql_scripts_converted'
)

print(f"Konvertiert: {result['converted']} Dateien")
print(f"Fehler: {result['failed']} Dateien")
print(f"Änderungen: {result['total_changes']} gesamt")
```

---

## Datentyp-Mappings Konfiguration

Die Datentyp-Mappings definieren, wie MSSQL-Datentypen zu PostgreSQL konvertiert werden. Sie können alle 28 Mappings anpassen.

### Automatische Erstellung

Beim Start der GUI wird automatisch `type_mappings_config.json` erstellt mit:
- 28 Standard MSSQL → PostgreSQL Datentyp-Mappings
- Alle gängigen Typen: bigint, int, varchar, decimal, datetime, etc.
- Konfigurierbar und erweiterbar

### Mappings Editor (GUI)

1. **Migration GUI öffnen**
2. **"🔄 Datentypen bearbeiten" Button** klicken (im Log-Bereich)
3. **Tabelle mit Mappings öffnet sich**
4. **Änderungen vornehmen:**
   - Neue Zeile hinzufügen (➕ Button)
   - Bestehende Einträge direkt bearbeiten
   - Zeilen löschen (🗑️ Button)
5. **Speichern** (💾 Button) speichert in `type_mappings_config.json`

### Beispiel `type_mappings_config.json`

```json
{
  "type_mappings": {
    "bigint": "BIGINT",
    "int": "INTEGER",
    "smallint": "SMALLINT",
    "tinyint": "SMALLINT",
    "bit": "BOOLEAN",
    "decimal": "DECIMAL",
    "numeric": "NUMERIC",
    "money": "NUMERIC(19,4)",
    "float": "DOUBLE PRECISION",
    "real": "REAL",
    "datetime": "TIMESTAMPTZ",
    "datetime2": "TIMESTAMPTZ",
    "datetimeoffset": "TIMESTAMP WITH TIME ZONE",
    "varchar": "VARCHAR",
    "nvarchar": "VARCHAR",
    "text": "TEXT",
    "ntext": "TEXT",
    "varbinary": "BYTEA",
    "image": "BYTEA",
    "uniqueidentifier": "UUID",
    "xml": "XML"
  }
}
```

### Mappings verwenden

Die Mappings werden automatisch verwendet von:
- **Step1** (`step1_migrate_data.py`) - bei Tabellenerstellung
- **Flyway Converter** (`flyway_converter.py`) - bei SQL-Script-Konvertierung

Änderungen werden sofort übernommen, ohne dass die Anwendung neu gestartet werden muss!

---

## Was wird migriert?

| Element | Details |
|---------|---------|
| **Tabellen** | Struktur und alle Daten |
| **Datentypen** | Automatische Konvertierung MSSQL → PostgreSQL (28 Typen) |
| **Constraints** | Primary Keys, Unique Constraints, Check Constraints |
| **Foreign Keys** | Referentielle Integrität |
| **Indizes** | Performance-Indizes |
| **Collations** | Optional, mappe MSSQL → PostgreSQL Collations |
| **SQL-Scripts** | Mit Flyway Converter konvertierbar |

---

## Collations Konfiguration

Step 4 migriert Collations von MSSQL zu PostgreSQL. Die Mappings sind anpassbar:

### Automatische Erstellung

Beim ersten Start der GUI wird automatisch `collations_config.json` erstellt mit:
- Standard MSSQL Collations (SQL_Latin1_General_CP1_CI_AS, etc.)
- Fallback PostgreSQL Collations pro MSSQL Collation
- Konfigurierbar und erweiterbar

### Custom Mappings definieren

1. **GUI Button:** Klicken Sie auf "⚙️ Collations konfigurieren"
2. **Datei öffnet sich** in Ihrem Standard-Editor
3. **Bearbeiten:** Passen Sie die Mappings an
4. **Speichern:** Änderungen werden beim nächsten Run verwendet

**Beispiel `collations_config.json`:**
```json
{
  "collations": {
    "SQL_Latin1_General_CP1_CI_AS": [
      "de-DE-x-icu",
      "de_DE.utf8",
      "de_DE",
      "default"
    ],
    "Latin1_General_CI_AS": ["en-US-x-icu", "default"],
    "YOUR_CUSTOM_COLLATION": ["your-mapping"]
  }
}
```

**Vorgehensweise:**
- Pro MSSQL Collation können mehrere PostgreSQL Optionen definiert werden
- Die **erste verfügbare** wird automatisch verwendet
- Mit "default" wird die DB-Standard-Collation genutzt

---

## Portable Version (USB)

### Standalone .exe erstellen

Die Anwendung kann auch als portable .exe für USB-Sticks erstellt werden:

```bash
# PyInstaller installieren
pip install pyinstaller

# Build ausführen
python build_exe.py

# Fertig! Datei: dist/MSSQL_PostgreSQL_Migration.exe
```

### Auf USB-Stick kopieren

Struktur für USB-Stick:

```
USB:\MigrationTool\
├── MSSQL_PostgreSQL_Migration.exe  ← aus dist/
├── step1_migrate_data.py
├── step2_verify_columns.py
├── step3_migrate_constraints.py
├── step4_migrate_collations.py
├── run_all.py
├── .env                            ← wird automatisch erstellt
└── logs/                           ← wird automatisch erstellt
```

### Vorteile der Portable Version

✅ **Kein Installer** nötig  
✅ **Keine Admin-Rechte** erforderlich  
✅ **Keine Installation** auf dem Zielcomputer  
✅ **Keine Registry-Einträge**  
✅ **Auf jedem PC einsatzfähig** (mit USB-Stick)  
✅ **Konfiguration auf dem Stick** (.env, logs bleiben lokal)  
✅ **Vollständig portabel** - einfach mitnehmen und nutzen

### USB-Verwendung

1. USB-Stick in beliebigen PC einstecken
2. Doppelklick auf `MSSQL_PostgreSQL_Migration.exe`
3. GUI wird gestartet, Anwendung lädt sich direkt
4. Migration kann sofort durchgeführt werden

**Kein Python, kein pip, keine Installation erforderlich!**

---

## Logs und Debugging

### Log-Verzeichnis

Alle Debug-Logs und Mappings befinden sich im `logs/`-Verzeichnis:

```
logs/
├── step1_debug.log          # Tabellen und Daten Migration
├── step2_debug.log          # Verifizierung
├── step3_debug.log          # Constraints und Keys
├── step4_debug.log          # Collations
└── column_mapping.json      # Spalten-Konvertierungstabelle
```

---

## Technische Details

### Framework & Technologie

| Komponente | Details |
|-----------|---------|
| **GUI Framework** | PyQt6 (modernes, natives UI) |
| **Datenbank MSSQL** | pyodbc (ODBC Driver 17) |
| **Datenbank PostgreSQL** | psycopg2 |
| **Konfiguration** | python-dotenv (.env-Dateien) |
| **Threading** | QThread für nicht-blockierende Migration |
| **Packaging** | PyInstaller für .exe-Erstellung |

### Modulararchitektur

Die Anwendung ist in spezialisierte Module aufgeteilt:

| Modul | Aufgabe |
|-------|---------|
| `migration_gui.py` | Hauptanwendung & GUI-Orchestrierung |
| `gui_builder.py` | Wiederverwendbare UI-Komponenten |
| `config_manager.py` | .env-Konfigurationsverwaltung |
| `connection_tester.py` | Datenbank-Verbindungstests |
| `dialogs.py` | Dialog-Fenster (Mappings, Logs, Konfiguration) |
| `collations_manager.py` | Collations-Konfigurationsverwaltung |
| `type_mappings_manager.py` | Datentyp-Mappings Verwaltung |
| `type_mappings_editor.py` | GUI-Editor für Datentyp-Mappings |
| `flyway_converter.py` | SQL-Script-Konvertierungs-Engine |
| `flyway_gui.py` | Flyway UI-Komponenten |
| `step1_migrate_data.py` | Tabellen & Daten Migration |
| `step2_verify_columns.py` | Spalten-Verifizierung |
| `step3_migrate_constraints.py` | Constraints & Indizes Migration |
| `step4_migrate_collations.py` | Collations Migration |

### Datentyp-Support

**Unterstützte MSSQL → PostgreSQL Konvertierungen (28 Typen):**

Ganze Zahlen: `bigint`, `int`, `smallint`, `tinyint`  
Boolesch: `bit` → `boolean`  
Dezimalzahlen: `decimal`, `numeric`, `money`, `smallmoney`  
Fließkomma: `float` → `double precision`, `real`  
Datum/Zeit: `datetime`, `datetime2`, `smalldatetime` → `timestamp`, `datetimeoffset` → `timestamp with time zone`, `date`, `time`  
Text: `varchar`, `nvarchar`, `char`, `nchar`, `text`, `ntext`  
Binär: `binary`, `varbinary`, `image` → `bytea`  
Spezielle: `uniqueidentifier` → `uuid`, `xml` → `xml`  

---

## Wichtige Hinweise

- ⚠️ Erstellen Sie ein Backup der Quell-Datenbank
- Testen Sie mit einer Test-Datenbank
- Die Schritte müssen in Reihenfolge ausgeführt werden
- Flyway Converter: Überprüfen Sie konvertierte Scripts vor Einsatz
- Datentypen: Testen Sie Custom-Mappings mit Ihre Daten

---

## Häufig gestellte Fragen (FAQ)

### Kann ich bestimmte Tabellen ausschließen?
Derzeit werden alle Tabellen migriert. Für selektive Migrationen nutzen Sie separate Datenbanken.

### Was ist wenn eine Verbindung fehlschlägt?
1. Nutzen Sie "MSSQL testen" / "PostgreSQL testen" Buttons zum Debuggen
2. Überprüfen Sie Netzwerk-Konnektivität
3. Verifizieren Sie Server-Name, Port und Zugangsdaten

### Kann ich Mappings während der Migration ändern?
Ja! Änderungen in `type_mappings_config.json` werden sofort übernommen.
Step1 nutzt die aktuellen Mappings beim Start, nicht beim Import!

### Wie kann ich den SQL Converter erweitern?
Bearbeiten Sie `flyway_converter.py`:
- Fügen Sie neue Regex-Pattern in `_convert_data_types()` hinzu
- Oder erweitern Sie `load_type_mappings()` für zusätzliche Konvertierungen

---

## Wichtige Hinweise

- ⚠️ Erstellen Sie ein Backup der Quell-Datenbank
- Testen Sie mit einer Test-Datenbank
- Die Schritte müssen in Reihenfolge ausgeführt werden

---

## Lizenz & Hinweis

**⚠️ Dieses Projekt wurde zu 100% durch künstliche Intelligenz (KI) generiert.**

Alle Komponenten wurden mit KI-Unterstützung entwickelt, einschließlich:
- ✅ Quellcode (Python, PyQt6)
- ✅ Datenbankmigrations-Logik
- ✅ GUI-Oberfläche
- ✅ Fehlerbehandlung
- ✅ Dokumentation

### Nutzung & Lizenz

Sie dürfen:
- ✅ Das Projekt nutzen
- ✅ Den Code modifizieren und anpassen
- ✅ Eigene Forks erstellen
- ✅ Weiterentwicklungen durchführen
- ✅ Weitere Versionen verteilen

Sie dürfen **nicht**:
- ❌ Direkt in dieses Repository pushen
- ❌ Dieses Repository als Ihr eigenes ausgeben

**Für Änderungen:** Erstellen Sie einfach einen **Fork** und verwenden Sie das für Ihre Modifikationen!