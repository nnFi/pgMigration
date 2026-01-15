# MSSQL zu PostgreSQL Migration

Professionelle Migrationsanwendung zur Konvertierung von Microsoft SQL Server Datenbanken nach PostgreSQL mit benutzerfreundlicher GUI und portabler .exe-Version.

---

## 📋 Inhaltsverzeichnis

1. [Installation](#installation)
2. [Konfiguration](#konfiguration)
3. [Verwendung](#verwendung)
4. [GUI Anwendung](#gui-anwendung)
5. [Portable Version (USB)](#portable-version-usb)
6. [Technische Details](#technische-details)
7. [Lizenz & Hinweis](#lizenz--hinweis)

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

## Was wird migriert?

| Element | Details |
|---------|---------|
| **Tabellen** | Struktur und alle Daten |
| **Datentypen** | Automatische Konvertierung MSSQL → PostgreSQL |
| **Constraints** | Primary Keys, Unique Constraints, Check Constraints |
| **Foreign Keys** | Referentielle Integrität |
| **Indizes** | Performance-Indizes |
| **Collations** | Optional, mappe MSSQL → PostgreSQL Collations |

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