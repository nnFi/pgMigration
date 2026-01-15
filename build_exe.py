"""
Build Script für Windows Executable
Erstellt eine standalone .exe Datei mit PyInstaller
"""

import PyInstaller.__main__
import sys
from pathlib import Path

# Bestimme Pfade
base_path = Path(__file__).parent
icon_path = base_path / "icon.ico"  # Optional: Eigenes Icon

# PyInstaller Optionen
options = [
    str(base_path / "migration_gui.py"),  # Hauptscript
    "--name=MSSQL_PostgreSQL_Migration",  # Name der .exe
    "--onefile",                           # Alles in einer Datei
    "--windowed",                          # Keine Console
    "--clean",                             # Bereinige Cache
    f"--distpath={base_path / 'dist'}",   # Output Verzeichnis
    f"--workpath={base_path / 'build'}",  # Build Cache
    f"--specpath={base_path}",            # .spec Datei
    # Füge die Migration-Scripts als Daten-Dateien hinzu
    f"--add-data={base_path / 'step1_migrate_data.py'};.",
    f"--add-data={base_path / 'step2_verify_columns.py'};.",
    f"--add-data={base_path / 'step3_migrate_constraints.py'};.",
    f"--add-data={base_path / 'step4_migrate_collations.py'};.",
    f"--add-data={base_path / 'run_all.py'};.",
    # Hidden imports für Dependencies die in embedded scripts verwendet werden
    "--hidden-import=pyodbc",
    "--hidden-import=psycopg2",
    "--hidden-import=psycopg2.extensions",
    "--hidden-import=dotenv",
]

# Füge Icon hinzu wenn vorhanden
if icon_path.exists():
    options.append(f"--icon={icon_path}")

# Füge Daten-Dateien hinzu (optional)
# options.append("--add-data=README.md:.")

print("=" * 60)
print("Erstelle Windows Executable...")
print("=" * 60)

try:
    PyInstaller.__main__.run(options)
    
    print("\n" + "=" * 60)
    print("Build erfolgreich!")
    print("=" * 60)
    print(f"\nExecutable: {base_path / 'dist' / 'MSSQL_PostgreSQL_Migration.exe'}")
    print("\nHinweise:")
    print("1. Die .exe befindet sich im 'dist' Ordner")
    print("2. Kopiere die Migration-Scripts (step*.py) in das gleiche Verzeichnis")
    print("3. Die .exe kann ohne Python Installation ausgeführt werden")
    print("4. Erste Ausführung kann langsam sein (ca. 10-20 Sekunden)")
    
except Exception as e:
    print(f"\nFehler beim Build: {e}")
    sys.exit(1)
