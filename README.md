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

✅ **Sicherheit**
- Warnung bei Einzelschritt-Ausführung (richtige Reihenfolge wichtig!)
- Verbindungstests vor Migration
- Sprechende Fehlermeldungen

#### GUI Screenshot

```
┌─────────────────────────────────────────┐
│  MSSQL → PostgreSQL Migration Tool      │
├─────────────────────────────────────────┤
│                                         │
│  [MSSQL Verbindung]  [PostgreSQL]      │
│  Password: ******* [👁]  Password: ** [👁]│
│  [Test Verbindung]                      │
│                                         │
│  [1️⃣ Tabellen] [2️⃣ Verify] [3️⃣ Keys]   │
│  [▶️ ALLE SCHRITTE AUSFÜHREN]           │
│  [████████░░] 80%                       │
│                                         │
│  ┌─ Migration Log ─────────────────┐  │
│  │ [12:34:56] Verbindung OK...      │  │
│  │ [12:34:57] Migriere Tabelle...   │  │
│  └─────────────────────────────────┘  │
│  [🗑️] [💾 Debug-Logs] [📋 Mapping]    │
└─────────────────────────────────────────┘
```

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
| **Collations** | Optional, kann manuelle Anpassung erfordern |

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

### Optionales Launcher-Script

Für einfachere Bedienung können Sie eine `start.bat` erstellen:

```batch
@echo off
cd /d "%~dp0"
start MSSQL_PostgreSQL_Migration.exe
```

Dann einfach die `start.bat` doppelklicken statt die .exe.

### Backup und Sicherung

Da .exe, Konfiguration und Logs alle auf dem USB-Stick sind:
- Stick kopieren = komplettes Backup
- Alle Einstellungen und Protokolle bleiben beieinander
- Kann auf mehreren Computern verwendet werden

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

### Debug-Logs exportieren

In der GUI können Sie über den "Debug-Logs" Button alle Protokolle exportieren und weitergeben:

```bash
# Exportiert: step1_debug.log, step2_debug.log, ... + column_mapping.json
# Mit Zeitstempel versehen für Unterscheidung mehrerer Migrationen
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

### Architektur

- **Nicht-blockierende UI** - Migrations laufen in separatem Thread
- **Echtzeit-Logging** - Output wird live angezeigt
- **Fehlerbehandlung** - Robuste Exception-Handling mit Benutzer-Feedback
- **Wiederholbarkeit** - Logs in separaten Verzeichnissen pro Lauf

### Performance

- Portable .exe Größe: ca. 50-70 MB (enthält Python Runtime)
- Erste Start von USB: 10-20 Sekunden (normal danach)
- Migrationsgeschwindigkeit abhängig von Datenmenge
- Multi-Schritt-Verarbeitung mit Parallelisierung wo möglich

---

## Hinweise & Best Practices

⚠️ **Vor Migration**
- Erstellen Sie ein Backup der Quell-Datenbank
- Testen Sie mit einer Test-Datenbank
- Überprüfen Sie Datentyp-Kompatibilität

💡 **Während Migration**
- Lassen Sie andere Anwendungen nicht auf die MSSQL-Datenbank zugreifen
- Achten Sie auf ausreichend Speicherplatz
- Monitorieren Sie die Log-Dateien auf Warnungen

✅ **Nach Migration**
- Testen Sie die migrierte Datenbank gründlich
- Überprüfen Sie Constraints und Beziehungen
- Collations können manuelle Anpassung erfordern
- Vergleichen Sie Datenbankgrößen und Datensätze

---

## Lizenz & Hinweis

**⚠️ Dieses Projekt wurde zu 100% durch künstliche Intelligenz (KI) generiert.**

Alle Komponenten wurden mit KI-Unterstützung entwickelt, einschließlich:
- ✅ Quellcode (Python, PyQt6)
- ✅ Datenbankmigrations-Logik
- ✅ GUI-Oberfläche
- ✅ Fehlerbehandlung
- ✅ Dokumentation
- ✅ Tests und Validierung

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