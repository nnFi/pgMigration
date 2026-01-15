"""
Master Script: Führt alle Migrationsschritte in Reihenfolge aus
"""

import sys
import os
import importlib
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import time

def print_log_file_contents(log_file_path):
    """Lese und drucke Log-Datei Inhalt für GUI"""
    # Warte bis Datei existiert (max 5 Sekunden)
    max_wait = 50
    while not log_file_path.exists() and max_wait > 0:
        time.sleep(0.1)
        max_wait -= 1
    
    if not log_file_path.exists():
        print(f"Warnung: Log-Datei nicht gefunden: {log_file_path}")
        return
    
    # Warte kurz, damit die Datei vollständig geschrieben ist
    time.sleep(0.5)
    
    # Versuche mehrmals zu lesen (falls Datei noch geöffnet)
    max_retries = 5
    for attempt in range(max_retries):
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if lines:
                    for line in lines:
                        print(line.rstrip())
                    return
                else:
                    time.sleep(0.2)
        except (IOError, OSError) as e:
            if attempt < max_retries - 1:
                time.sleep(0.2)
            else:
                print(f"Fehler beim Lesen von {log_file_path}: {e}")

def run_script(module_name, step_number, description, total_steps):
    """Führe ein Python-Script aus durch dynamischen Import"""
    print()
    print("=" * 70)
    print(f"SCHRITT {step_number}/{total_steps}: {description}")
    print("=" * 70)
    print()
    
    # Sende Progress-Signal (falls GUI läuft)
    progress_percent = int((step_number - 1) / total_steps * 100)
    print(f"PROGRESS:{progress_percent}")
    
    # Ermittle Log-Datei-Pfad
    run_dir = Path(os.getenv('MIGRATION_RUN_DIR', 'logs'))
    log_file = run_dir / f"step{step_number}_debug.log"
    
    try:
        # WICHTIG: Entferne gecachtes Modul damit bei jedem Lauf die aktuelle Version geladen wird
        if module_name in sys.modules:
            del sys.modules[module_name]
        
        # Dynamischer Import des Moduls
        module = importlib.import_module(module_name)
        
        # Führe main() aus
        if hasattr(module, 'main'):
            try:
                module.main()
                
                print(f"Schritt {step_number} erfolgreich abgeschlossen")
                
                # Progress nach Abschluss
                progress_percent = int(step_number / total_steps * 100)
                print(f"PROGRESS:{progress_percent}")
                
                return True
            except SystemExit as e:
                # Nach Fehler nur Zusammenfassung zeigen, keine Details
                if e.code == 0:
                    print(f"Schritt {step_number} erfolgreich abgeschlossen")
                    
                    # Progress nach Abschluss
                    progress_percent = int(step_number / total_steps * 100)
                    print(f"PROGRESS:{progress_percent}")
                    
                    return True
                else:
                    print(f"Schritt {step_number} mit Fehler beendet (Exit Code: {e.code})")
                    return False
        else:
            print(f"Fehler: {module_name} hat keine main() Funktion")
            return False
            
    except Exception as e:
        print(f"Fehler beim Ausführen von {module_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Hauptfunktion - Führt alle Schritte aus"""
    print("")
    print("=" * 70)
    print("         MSSQL -> PostgreSQL MIGRATION")
    print("=" * 70)
    print("")
    
    start_time = datetime.now()
    
    # Erstelle logs-Verzeichnis falls nicht vorhanden
    logs_dir = Path.cwd() / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Erstelle Ordner für diesen Lauf mit Timestamp (mit Mikrosekunden für Eindeutigkeit)
    run_timestamp = start_time.strftime('%Y%m%d_%H%M%S_%f')
    run_dir = logs_dir / f"run_{run_timestamp}"
    
    # Stelle sicher, dass das Verzeichnis erstellt wird (auch wenn es bereits existiert)
    run_dir.mkdir(parents=True, exist_ok=True)
    
    # Überschreibe alte MIGRATION_RUN_DIR (falls sie noch gesetzt ist)
    os.environ['MIGRATION_RUN_DIR'] = str(run_dir)
    
    # Debug: Zeige das run_dir und prüfe ob es existiert
    print(f"MIGRATION_RUN_DIR={os.environ['MIGRATION_RUN_DIR']}")
    print(f"run_dir existiert: {run_dir.exists()}")
    print(f"run_dir ist absolut: {run_dir.is_absolute()}")
    
    print(f"Log-Verzeichnis: {run_dir}")
    print("")
    
    # Prüfe ob .env Datei existiert
    if not os.path.exists('.env'):
        print("Fehler: .env Datei nicht gefunden!")
        print("Bitte erstellen Sie eine .env Datei mit den Datenbankverbindungen.")
        sys.exit(1)
    
    # Lade .env Datei BEVOR Scripts ausgeführt werden!
    load_dotenv(override=False)
    
    # Definiere Schritte (Modulnamen ohne .py)
    steps = [
        {
            'module': 'step1_migrate_data',
            'number': 1,
            'description': 'Tabellen und Daten migrieren',
            'required': True
        },
        {
            'module': 'step2_verify_columns',
            'number': 2,
            'description': 'Spalten verifizieren',
            'required': True
        },
        {
            'module': 'step3_migrate_constraints',
            'number': 3,
            'description': 'Constraints und Keys migrieren',
            'required': True
        },
        {
            'module': 'step4_migrate_collations',
            'number': 4,
            'description': 'Collations migrieren',
            'required': False
        }
    ]
    
    # Führe alle Schritte aus
    failed_steps = []
    total_steps = len(steps)
    
    for step in steps:
        # Führe Script aus (Module sind jetzt importierbar)
        success = run_script(step['module'], step['number'], step['description'], total_steps)
        
        if not success:
            failed_steps.append(step['number'])
            if step['required']:
                print()
                print("Kritischer Fehler - Migration abgebrochen!")
                print("Bitte beheben Sie die Fehler und führen Sie die Migration erneut aus.")
                break
    
    # Gesamtzusammenfassung
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("")
    print("=" * 70)
    print("              GESAMTZUSAMMENFASSUNG")
    print("=" * 70)
    print("")
    
    print(f"Ausgeführte Schritte: {len(steps)}")
    
    if not failed_steps:
        print("Alle Schritte erfolgreich abgeschlossen!")
        print()
        print("Die Migration wurde erfolgreich durchgeführt.")
    else:
        print(f"{len(failed_steps)} Schritt(e) fehlgeschlagen: {', '.join(map(str, failed_steps))}")
        print()
        print("Bitte prüfen Sie die Fehler in den einzelnen Schritten und")
        print("führen Sie die fehlgeschlagenen Schritte einzeln erneut aus:")
        for step_num in failed_steps:
            step = next((s for s in steps if s['number'] == step_num), None)
            if step:
                print(f"  python {step['module']}.py")
    
    print()
    print(f"Gesamtdauer: {duration:.2f} Sekunden")
    print(f"Abgeschlossen: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Exit Code setzen
    if failed_steps:
        sys.exit(1)

if __name__ == "__main__":
    main()
