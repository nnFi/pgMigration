"""
Master Script: Führt alle Migrationsschritte in Reihenfolge aus
"""

import subprocess
import sys
import os
from datetime import datetime

def run_script(script_name, step_number, description):
    """Führe ein Python-Script aus und prüfe den Exit-Code"""
    print()
    print("=" * 70)
    print(f"SCHRITT {step_number}: {description}")
    print("=" * 70)
    print()
    
    try:
        # Führe Script aus
        result = subprocess.run(
            [sys.executable, script_name],
            check=False,
            capture_output=False,
            text=True
        )
        
        if result.returncode == 0:
            print()
            print(f"Schritt {step_number} erfolgreich abgeschlossen")
            return True
        else:
            print()
            print(f"Schritt {step_number} mit Fehler beendet (Exit Code: {result.returncode})")
            return False
            
    except Exception as e:
        print(f"Fehler beim Ausführen von {script_name}: {e}")
        return False

def main():
    """Hauptfunktion - Führt alle Schritte aus"""
    print("")
    print("=" * 70)
    print("         MSSQL -> PostgreSQL MIGRATION")
    print("=" * 70)
    print("")
    
    start_time = datetime.now()
    
    # Prüfe ob .env Datei existiert
    if not os.path.exists('.env'):
        print("Fehler: .env Datei nicht gefunden!")
        print("Bitte erstellen Sie eine .env Datei mit den Datenbankverbindungen.")
        sys.exit(1)
    
    # Definiere Schritte
    steps = [
        {
            'script': 'step1_migrate_data.py',
            'number': 1,
            'description': 'Tabellen und Daten migrieren',
            'required': True
        },
        {
            'script': 'step2_verify_columns.py',
            'number': 2,
            'description': 'Spalten verifizieren',
            'required': True
        },
        {
            'script': 'step3_migrate_constraints.py',
            'number': 3,
            'description': 'Constraints und Keys migrieren',
            'required': True
        },
        {
            'script': 'step4_migrate_collations.py',
            'number': 4,
            'description': 'Collations migrieren',
            'required': False
        }
    ]
    
    # Führe alle Schritte aus
    failed_steps = []
    
    for step in steps:
        # Prüfe ob Script existiert
        if not os.path.exists(step['script']):
            print(f"Fehler: {step['script']} nicht gefunden!")
            failed_steps.append(step['number'])
            if step['required']:
                break
            continue
        
        # Führe Script aus
        success = run_script(step['script'], step['number'], step['description'])
        
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
                print(f"  python {step['script']}")
    
    print()
    print(f"Gesamtdauer: {duration:.2f} Sekunden")
    print(f"Abgeschlossen: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Exit Code setzen
    if failed_steps:
        sys.exit(1)

if __name__ == "__main__":
    main()
