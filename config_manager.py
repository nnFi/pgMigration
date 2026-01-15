"""
Config Manager für Migration Tool
Verwaltet .env Datei und Konfiguration
"""

from pathlib import Path


def save_env(work_path, env_vars, log_callback=None):
    """Speichere Konfiguration in .env"""
    try:
        env_path = work_path / ".env"
        
        with open(env_path, 'w') as f:
            f.write("# ========== DATENBANK-KONFIGURATION ==========\n")
            for key, value in env_vars.items():
                f.write(f"{key}={value}\n")
            f.write("# ============================================\n")
        
        if log_callback:
            log_callback("Konfiguration gespeichert: .env")
        return True
    except Exception as e:
        if log_callback:
            log_callback(f"Fehler beim Speichern der Konfiguration: {str(e)}")
        return False


def load_env_file(env_path):
    """Lade .env Datei und gebe Dictionary zurück"""
    env_vars = {}
    
    if not env_path.exists():
        return env_vars
    
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                env_vars[key] = value
    
    return env_vars


def load_env_into_ui(env_path, ui_controls):
    """Lade .env Datei und setze UI-Felder"""
    env_vars = load_env_file(env_path)
    
    for key, value in env_vars.items():
        if key == 'MSSQL_SERVER':
            ui_controls['mssql_server'][1].setText(value)
        elif key == 'MSSQL_PORT':
            ui_controls['mssql_port'][1].setText(value)
        elif key == 'MSSQL_DATABASE':
            ui_controls['mssql_database'][1].setText(value)
        elif key == 'MSSQL_USER':
            ui_controls['mssql_user'][1].setText(value)
        elif key == 'MSSQL_PASSWORD':
            ui_controls['mssql_password'][1].setText(value)
        elif key == 'PG_HOST':
            ui_controls['pg_host'][1].setText(value)
        elif key == 'PG_PORT':
            ui_controls['pg_port'][1].setText(value)
        elif key == 'PG_DATABASE':
            ui_controls['pg_database'][1].setText(value)
        elif key == 'PG_USER':
            ui_controls['pg_user'][1].setText(value)
        elif key == 'PG_PASSWORD':
            ui_controls['pg_password'][1].setText(value)
        elif key == 'MIGRATE_DATA':
            ui_controls['migrate_data_checkbox'].setChecked(value.lower() == 'true')
        elif key == 'IDENTITY_ALWAYS':
            ui_controls['identity_always_checkbox'].setChecked(value.lower() == 'true')
        elif key == 'SKIP_STEP4':
            ui_controls['step4_checkbox'].setChecked(value.lower() != 'true')


def get_env_vars_from_ui(ui_controls):
    """Hole Umgebungsvariablen aus UI-Feldern"""
    env_vars = {
        'MSSQL_SERVER': ui_controls['mssql_server'][1].text(),
        'MSSQL_DATABASE': ui_controls['mssql_database'][1].text(),
        'MSSQL_USER': ui_controls['mssql_user'][1].text(),
        'MSSQL_PASSWORD': ui_controls['mssql_password'][1].text(),
    }
    
    mssql_port = ui_controls['mssql_port'][1].text().strip()
    if mssql_port:
        env_vars['MSSQL_PORT'] = mssql_port
    
    env_vars.update({
        'PG_HOST': ui_controls['pg_host'][1].text(),
        'PG_PORT': ui_controls['pg_port'][1].text(),
        'PG_DATABASE': ui_controls['pg_database'][1].text(),
        'PG_USER': ui_controls['pg_user'][1].text(),
        'PG_PASSWORD': ui_controls['pg_password'][1].text(),
        'LOG_LEVEL': ui_controls['log_level_combo'].currentText(),
        'MIGRATE_DATA': 'true' if ui_controls['migrate_data_checkbox'].isChecked() else 'false',
        'IDENTITY_ALWAYS': 'true' if ui_controls['identity_always_checkbox'].isChecked() else 'false',
    })
    
    return env_vars
