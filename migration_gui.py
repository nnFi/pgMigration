"""
MSSQL zu PostgreSQL Migration Tool - GUI
Professionelle Desktop-Anwendung mit PyQt6
"""

import sys
import importlib.util
import os
from datetime import datetime
from pathlib import Path
import io
from contextlib import redirect_stdout, redirect_stderr

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGroupBox, QLineEdit, QLabel, QPushButton, QTextEdit, QProgressBar,
    QTabWidget, QFileDialog, QMessageBox, QSpinBox, QComboBox, QCheckBox
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QTimer
from PyQt6.QtGui import QFont, QTextCursor, QIcon

import json
from PyQt6.QtWidgets import QDialog, QPushButton, QVBoxLayout, QHBoxLayout, QTextEdit, QLabel


class MigrationWorker(QThread):
    """Worker Thread für Migration um UI nicht zu blockieren"""
    progress = pyqtSignal(int, int, str)
    log_output = pyqtSignal(str)
    finished = pyqtSignal(bool, str)
    
    def __init__(self, script_path, env_vars):
        super().__init__()
        self.script_path = script_path
        self.env_vars = env_vars
        self.output_buffer = io.StringIO()
        self._last_pos = 0
        
    def get_new_output(self):
        """Hole neuen Output seit letztem Aufruf"""
        self.output_buffer.seek(self._last_pos)
        new_output = self.output_buffer.read()
        self._last_pos = self.output_buffer.tell()
        return new_output
        
    def run(self):
        try:
            # Setze Umgebungsvariablen
            old_env = {}
            for key, value in self.env_vars.items():
                old_env[key] = os.environ.get(key)
                os.environ[key] = value
            
            # Bestimme Arbeitsverzeichnis - nutze work_path der GUI
            import __main__
            if hasattr(__main__, 'work_path'):
                work_dir = __main__.work_path
            else:
                work_dir = Path(self.script_path).parent
            
            old_cwd = os.getcwd()
            os.chdir(work_dir)
            
            # Importiere und führe Script direkt aus
            script_name = Path(self.script_path).stem
            
            try:
                # Entferne gecachtes Modul damit bei jedem Lauf die aktuelle Version geladen wird
                if script_name in sys.modules:
                    del sys.modules[script_name]
                
                # Dynamischer Import
                spec = importlib.util.spec_from_file_location(script_name, self.script_path)
                module = importlib.util.module_from_spec(spec)
                
                # Umleite stdout/stderr
                with redirect_stdout(self.output_buffer), redirect_stderr(self.output_buffer):
                    spec.loader.exec_module(module)
                    
                    # Führe main() aus falls vorhanden
                    if hasattr(module, 'main'):
                        module.main()
                
                # Hole restlichen Output
                remaining = self.get_new_output()
                for line in remaining.split('\n'):
                    if line.strip():
                        self.log_output.emit(line)
                
                self.finished.emit(True, f"{script_name} erfolgreich abgeschlossen")
                
            except SystemExit as e:
                # Script hat sys.exit() aufgerufen
                remaining = self.get_new_output()
                for line in remaining.split('\n'):
                    if line.strip():
                        self.log_output.emit(line)
                
                if e.code == 0:
                    self.finished.emit(True, f"{script_name} erfolgreich abgeschlossen")
                else:
                    self.finished.emit(False, f"{script_name} fehlgeschlagen (Exit Code: {e.code})")
            
            # Stelle Umgebung wieder her
            os.chdir(old_cwd)
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
                    
        except Exception as e:
            import traceback
            error_msg = f"Fehler: {str(e)}\n{traceback.format_exc()}"
            self.log_output.emit(error_msg)
            self.finished.emit(False, f"Fehler: {str(e)}")


class MigrationGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MSSQL → PostgreSQL Migration Tool")
        self.setMinimumSize(1000, 800)
        
        # Status
        self.worker = None
        
        # Bestimme Pfade
        if getattr(sys, 'frozen', False):
            # Läuft als .exe
            self.base_path = Path(sys._MEIPASS)
            # Daten (logs, .env) im Verzeichnis der .exe
            self.work_path = Path(sys.executable).parent
        else:
            # Läuft als .py - alles im gleichen Verzeichnis
            self.base_path = Path(__file__).parent
            self.work_path = Path(__file__).parent
        
        # UI aufbauen
        self.setup_ui()
        
        # Lade .env falls vorhanden
        self.load_env()
        
    def setup_ui(self):
        """Erstelle Benutzeroberfläche"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # === DATENBANKVERBINDUNGEN ===
        db_group = QGroupBox("Datenbankverbindungen")
        db_layout = QVBoxLayout()
        
        # MSSQL
        mssql_layout = QVBoxLayout()
        mssql_label = QLabel("MSSQL Server:")
        mssql_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        mssql_layout.addWidget(mssql_label)
        
        self.mssql_server = self.create_input("Server:", "")
        self.mssql_port = self.create_input("Port (optional):", "", tooltip="Standardport: 1433")
        self.mssql_database = self.create_input("Database:", "")
        self.mssql_user = self.create_input("User:", "")
        self.mssql_password = self.create_input("Password:", "", password=True)
        
        mssql_layout.addLayout(self.mssql_server[0])
        mssql_layout.addLayout(self.mssql_port[0])
        mssql_layout.addLayout(self.mssql_database[0])
        mssql_layout.addLayout(self.mssql_user[0])
        mssql_layout.addLayout(self.mssql_password[0])
        
        # PostgreSQL
        pg_layout = QVBoxLayout()
        pg_label = QLabel("PostgreSQL:")
        pg_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        pg_layout.addWidget(pg_label)
        
        self.pg_host = self.create_input("Host:", "")
        self.pg_port = self.create_input("Port (optional):", "", tooltip="Standardport: 5432")
        self.pg_database = self.create_input("Database:", "")
        self.pg_user = self.create_input("User:", "")
        self.pg_password = self.create_input("Password:", "", password=True)
        
        pg_layout.addLayout(self.pg_host[0])
        pg_layout.addLayout(self.pg_port[0])
        pg_layout.addLayout(self.pg_database[0])
        pg_layout.addLayout(self.pg_user[0])
        pg_layout.addLayout(self.pg_password[0])
        
        # Kombiniere MSSQL und PostgreSQL nebeneinander
        db_row = QHBoxLayout()
        db_row.addLayout(mssql_layout)
        db_row.addLayout(pg_layout)
        
        db_layout.addLayout(db_row)
        
        # Log-Level Einstellung
        options_layout = QHBoxLayout()
        log_level_label = QLabel("Log-Level:")
        log_level_label.setMinimumWidth(80)
        self.log_level_combo = QComboBox()
        self.log_level_combo.addItems(["DEBUG", "INFO", "WARNING", "ERROR"])
        self.log_level_combo.setCurrentText("DEBUG")
        self.log_level_combo.setToolTip("Wählen Sie das Logging-Level:\nDEBUG = Alle Meldungen\nINFO = Wichtige Meldungen\nWARNING = Nur Warnungen und Fehler\nERROR = Nur Fehler")
        options_layout.addWidget(log_level_label)
        options_layout.addWidget(self.log_level_combo)
        
        # Checkbox für Datenmigrationen
        self.migrate_data_checkbox = QCheckBox("Daten migrieren")
        self.migrate_data_checkbox.setChecked(True)
        self.migrate_data_checkbox.setToolTip("Wenn aktiviert: Tabelleninhalt wird migriert\nWenn deaktiviert: Nur die Tabellenstruktur")
        options_layout.addWidget(self.migrate_data_checkbox)
        
        # Checkbox für IDENTITY-Variante
        self.identity_always_checkbox = QCheckBox("Manuelle IDs deaktivieren")
        self.identity_always_checkbox.setChecked(False)
        self.identity_always_checkbox.setToolTip("Unchecked: GENERATED BY DEFAULT (erlaubt manuelle ID-Eingabe)\nChecked: GENERATED ALWAYS (erzwingt nur automatische IDs)")
        options_layout.addWidget(self.identity_always_checkbox)
        
        # Checkbox für Step 4 (Collations)
        self.step4_checkbox = QCheckBox("Step 4: Collations ausführen")
        self.step4_checkbox.setChecked(True)
        self.step4_checkbox.setToolTip("Unchecked: Step 4 wird übersprungen\nChecked: Step 4 wird bei 'ALLE SCHRITTE' ausgeführt")
        options_layout.addWidget(self.step4_checkbox)
        
        options_layout.addStretch()
        db_layout.addLayout(options_layout)
        
        # Buttons zum Speichern/Laden
        btn_layout = QHBoxLayout()
        save_btn = QPushButton("💾 Konfiguration speichern")
        save_btn.clicked.connect(self.save_env)
        load_btn = QPushButton("📁 Konfiguration laden")
        load_btn.clicked.connect(self.load_env)
        import_btn = QPushButton("📥 Konfiguration importieren")
        import_btn.clicked.connect(self.import_env)
        import_btn.setToolTip("Importiere .env-Datei von einem anderen Ort")
        
        # Test-Buttons
        test_mssql_btn = QPushButton("🔌 MSSQL testen")
        test_mssql_btn.clicked.connect(self.test_mssql_connection)
        test_mssql_btn.setStyleSheet("background-color: #0078D4; color: white;")
        
        test_pg_btn = QPushButton("🐘 PostgreSQL testen")
        test_pg_btn.clicked.connect(self.test_pg_connection)
        test_pg_btn.setStyleSheet("background-color: #336791; color: white;")
        
        btn_layout.addWidget(save_btn)
        btn_layout.addWidget(load_btn)
        btn_layout.addWidget(import_btn)
        btn_layout.addWidget(test_mssql_btn)
        btn_layout.addWidget(test_pg_btn)
        btn_layout.addStretch()
        db_layout.addLayout(btn_layout)
        
        db_group.setLayout(db_layout)
        main_layout.addWidget(db_group)
        
        # === MIGRATIONS-SCHRITTE ===
        steps_group = QGroupBox("Migrationsschritte")
        steps_layout = QVBoxLayout()
        
        # Einzelne Schritte
        step_buttons = QHBoxLayout()
        
        self.step1_btn = QPushButton("1️⃣ Tabellen & Daten")
        self.step1_btn.clicked.connect(lambda: self.run_single_step_with_warning("step1_migrate_data.py", 1))
        
        self.step2_btn = QPushButton("2️⃣ Verifizieren")
        self.step2_btn.clicked.connect(lambda: self.run_single_step_with_warning("step2_verify_columns.py", 2))
        
        self.step3_btn = QPushButton("3️⃣ Constraints & Indexes")
        self.step3_btn.clicked.connect(lambda: self.run_single_step_with_warning("step3_migrate_constraints.py", 3))
        
        self.step4_btn = QPushButton("4️⃣ Collations")
        self.step4_btn.clicked.connect(lambda: self.run_single_step_with_warning("step4_migrate_collations.py", 4))
        
        step_buttons.addWidget(self.step1_btn)
        step_buttons.addWidget(self.step2_btn)
        step_buttons.addWidget(self.step3_btn)
        step_buttons.addWidget(self.step4_btn)
        
        steps_layout.addLayout(step_buttons)
        
        # Alle Schritte Button
        self.run_all_btn = QPushButton("ALLE SCHRITTE AUSFÜHREN")
        self.run_all_btn.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        self.run_all_btn.setMinimumHeight(50)
        self.run_all_btn.clicked.connect(self.run_all_steps)
        self.run_all_btn.setStyleSheet("""
            QPushButton {
                background-color: #2E7D32;
                color: white;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #388E3C;
            }
            QPushButton:disabled {
                background-color: #BDBDBD;
            }
        """)
        steps_layout.addWidget(self.run_all_btn)
        
        # Progress Bar
        self.progress = QProgressBar()
        self.progress.setTextVisible(True)
        steps_layout.addWidget(self.progress)
        
        steps_group.setLayout(steps_layout)
        main_layout.addWidget(steps_group)
        
        log_group = QGroupBox("Migration Log")
        log_layout = QVBoxLayout()
        
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setFont(QFont("Consolas", 9))
        self.log_output.setStyleSheet("background-color: #1E1E1E; color: #D4D4D4;")
        log_layout.addWidget(self.log_output)
        
        log_btn_layout = QHBoxLayout()
        clear_btn = QPushButton("🗑️ Log löschen")
        clear_btn.clicked.connect(self.clear_log)
        save_log_btn = QPushButton("💾 Debug-Logs exportieren")
        save_log_btn.clicked.connect(self.save_debug_logs)
        view_mapping_btn = QPushButton("📋 Column Mapping anzeigen")
        view_mapping_btn.clicked.connect(self.view_column_mapping)
        log_btn_layout.addWidget(clear_btn)
        log_btn_layout.addWidget(save_log_btn)
        log_btn_layout.addWidget(view_mapping_btn)
        log_btn_layout.addStretch()
        log_layout.addLayout(log_btn_layout)
        
        log_group.setLayout(log_layout)
        main_layout.addWidget(log_group, stretch=1)
        
        self.statusBar().showMessage("Bereit")
        
    def create_input(self, label, default_value, password=False, tooltip=""):
        """Erstelle Label + Input Feld"""
        layout = QHBoxLayout()
        lbl = QLabel(label)
        lbl.setMinimumWidth(80)
        input_field = QLineEdit(default_value)
        
        if tooltip:
            input_field.setToolTip(tooltip)
        
        if password:
            input_field.setEchoMode(QLineEdit.EchoMode.Password)
            
            # Toggle Button für Passwort anzeigen
            toggle_btn = QPushButton("👁")
            toggle_btn.setMaximumWidth(40)
            toggle_btn.setCheckable(True)
            toggle_btn.setToolTip("Passwort anzeigen/verstecken")
            toggle_btn.clicked.connect(lambda checked: self.toggle_password_visibility(input_field, checked))
            
            layout.addWidget(lbl)
            layout.addWidget(input_field)
            layout.addWidget(toggle_btn)
        else:
            layout.addWidget(lbl)
            layout.addWidget(input_field)
            
        return (layout, input_field)
    
    def toggle_password_visibility(self, input_field, show):
        """Schalte Passwort-Sichtbarkeit um"""
        if show:
            input_field.setEchoMode(QLineEdit.EchoMode.Normal)
        else:
            input_field.setEchoMode(QLineEdit.EchoMode.Password)
    
    def get_env_vars(self):
        """Hole aktuelle Umgebungsvariablen aus UI"""
        env_vars = {
            'MSSQL_SERVER': self.mssql_server[1].text(),
            'MSSQL_DATABASE': self.mssql_database[1].text(),
            'MSSQL_USER': self.mssql_user[1].text(),
            'MSSQL_PASSWORD': self.mssql_password[1].text(),
        }
        
        mssql_port = self.mssql_port[1].text().strip()
        if mssql_port:
            env_vars['MSSQL_PORT'] = mssql_port
        
        env_vars.update({
            'PG_HOST': self.pg_host[1].text(),
            'PG_PORT': self.pg_port[1].text(),
            'PG_DATABASE': self.pg_database[1].text(),
            'PG_USER': self.pg_user[1].text(),
            'PG_PASSWORD': self.pg_password[1].text(),
            'LOG_LEVEL': self.log_level_combo.currentText(),
            'MIGRATE_DATA': 'true' if self.migrate_data_checkbox.isChecked() else 'false',
            'IDENTITY_ALWAYS': 'true' if self.identity_always_checkbox.isChecked() else 'false',
        })
        
        return env_vars
    
    def save_env(self):
        """Speichere Konfiguration in .env"""
        try:
            env_vars = self.get_env_vars()
            env_path = self.work_path / ".env"
            
            with open(env_path, 'w') as f:
                f.write("# ========== DATENBANK-KONFIGURATION ==========\n")
                for key, value in env_vars.items():
                    f.write(f"{key}={value}\n")
                f.write("# ============================================\n")
            
            self.log("Konfiguration gespeichert: .env")
        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"Fehler beim Speichern: {str(e)}")
    
    def load_env(self):
        """Lade Konfiguration aus .env"""
        try:
            env_path = self.work_path / ".env"
            if not env_path.exists():
                return
            
            self._load_env_file(env_path)
            self.log("Konfiguration geladen aus .env")
        except Exception as e:
            self.log(f"Fehler beim Laden der Konfiguration: {str(e)}")
    
    def import_env(self):
        """Importiere .env Datei von irgendwo"""
        filename, _ = QFileDialog.getOpenFileName(
            self,
            "Konfiguration (.env) importieren",
            str(Path.home()),
            "ENV Files (*.env);;All Files (*)"
        )
        
        if filename:
            try:
                env_path = Path(filename)
                self._load_env_file(env_path)
                
                # Kopiere die Datei ins work_path
                import shutil
                target_path = self.work_path / ".env"
                shutil.copy2(env_path, target_path)
                
                self.log(f"Konfiguration importiert aus: {filename}")
                self.log(f"Gespeichert als: {target_path}")
            except Exception as e:
                QMessageBox.critical(self, "Fehler", f"Fehler beim Importieren: {str(e)}")
                self.log(f"Fehler beim Importieren: {str(e)}")
    
    def _load_env_file(self, env_path):
        """Hilfsmethode zum Laden einer .env Datei"""
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    
                    if key == 'MSSQL_SERVER':
                        self.mssql_server[1].setText(value)
                    elif key == 'MSSQL_PORT':
                        self.mssql_port[1].setText(value)
                    elif key == 'MSSQL_DATABASE':
                        self.mssql_database[1].setText(value)
                    elif key == 'MSSQL_USER':
                        self.mssql_user[1].setText(value)
                    elif key == 'MSSQL_PASSWORD':
                        self.mssql_password[1].setText(value)
                    elif key == 'PG_HOST':
                        self.pg_host[1].setText(value)
                    elif key == 'PG_PORT':
                        self.pg_port[1].setText(value)
                    elif key == 'PG_DATABASE':
                        self.pg_database[1].setText(value)
                    elif key == 'PG_USER':
                        self.pg_user[1].setText(value)
                    elif key == 'PG_PASSWORD':
                        self.pg_password[1].setText(value)
                    elif key == 'MIGRATE_DATA':
                        self.migrate_data_checkbox.setChecked(value.lower() == 'true')
                    elif key == 'IDENTITY_ALWAYS':
                        self.identity_always_checkbox.setChecked(value.lower() == 'true')
                    elif key == 'SKIP_STEP4':
                        self.step4_checkbox.setChecked(value.lower() != 'true')
    
    def log(self, message):
        """Füge Nachricht zum Log hinzu"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_output.append(f"[{timestamp}] {message}")
        self.log_output.moveCursor(QTextCursor.MoveOperation.End)
    
    def clear_log(self):
        self.log_output.clear()
        if hasattr(self, 'live_timer') and self.live_timer.isActive():
            self.live_timer.stop()
        if hasattr(self, 'worker'):
            self.worker = None
    
    def run_single_step_with_warning(self, script_name, step_number):
        """Führe einzelnen Step mit Warnung aus"""
        reply = QMessageBox.warning(
            self,
            "Warnung: Einzelner Schritt",
            f"Sie führen Schritt {step_number} einzeln aus.\n\n"
            "ACHTUNG:\n"
            "• Die Schritte müssen in der richtigen Reihenfolge ausgeführt werden!\n"
            "• Keinen Schritt überspringen!\n"
            "• Alle vorherigen Schritte müssen erfolgreich abgeschlossen sein!\n\n"
            "Es wird empfohlen, \"ALLE SCHRITTE AUSFÜHREN\" zu verwenden.\n\n"
            "Trotzdem fortfahren?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.run_step(script_name)
    
    def run_step(self, script_name):
        """Führe einzelnen Migrationsschritt aus"""
        if self.worker and self.worker.isRunning():
            QMessageBox.warning(self, "Läuft bereits", "Eine Migration läuft bereits!")
            return
        
        script_path = self.base_path / script_name
        if not script_path.exists():
            QMessageBox.critical(self, "Fehler", f"Script nicht gefunden: {script_name}")
            return
        
        self.save_env()
        
        if script_name != "run_all.py":
            run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            run_dir = self.work_path / "logs" / f"run_{run_timestamp}"
            run_dir.mkdir(parents=True, exist_ok=True)
            os.environ['MIGRATION_RUN_DIR'] = str(run_dir)
            self.log(f"Log-Verzeichnis: {run_dir}")
        
        self.log(f"{'='*70}")
        self.log(f"Starte: {script_name}")
        self.log(f"{'='*70}")
        
        self.set_buttons_enabled(False)
        self.progress.setValue(0)
        self.statusBar().showMessage(f"Läuft: {script_name}...")
        
        # Starte Worker Thread
        self.worker = MigrationWorker(str(script_path), self.get_env_vars())
        self.worker.log_output.connect(self.log)
        self.worker.finished.connect(self.on_step_finished)
        
        # Timer für Live-Updates
        self.live_timer = QTimer()
        self.live_timer.timeout.connect(self.update_live_output)
        self.live_timer.start(100)
        
        self.worker.start()
    
    def update_live_output(self):
        """Hole neuen Output vom Worker während der Ausführung"""
        if self.worker and self.worker.isRunning():
            new_output = self.worker.get_new_output()
            if new_output:
                for line in new_output.split('\n'):
                    if line.strip():
                        # Prüfe auf Progress-Signal
                        if line.startswith('PROGRESS:'):
                            try:
                                progress = int(line.split(':')[1])
                                self.progress.setValue(progress)
                            except:
                                pass
                        else:
                            self.log(line)
        else:
            # Worker fertig, stoppe Timer
            if hasattr(self, 'live_timer'):
                self.live_timer.stop()
    
    def run_all_steps(self):
        """Führe alle Schritte nacheinander aus"""
        step4_text = "4. Collations" if self.step4_checkbox.isChecked() else "4. Collations (übersprungen)"
        reply = QMessageBox.question(
            self,
            "Bestätigung",
            f"Alle Migrationsschritte ausführen?\n\n1. Tabellen & Daten\n2. Verifizierung\n3. Constraints & Indexes\n{step4_text}",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Speichere Step 4 Einstellung
            if not self.step4_checkbox.isChecked():
                os.environ['SKIP_STEP4'] = 'true'
            else:
                os.environ.pop('SKIP_STEP4', None)
            
            self.run_step("run_all.py")
    
    def on_step_finished(self, success, message):
        """Callback wenn Schritt fertig ist"""
        self.set_buttons_enabled(True)
        self.progress.setValue(100 if success else 0)
        
        if success:
            self.log(message)
            self.statusBar().showMessage(f"{message}", 5000)
        else:
            self.log(message)
            self.statusBar().showMessage(f"{message}", 5000)
            QMessageBox.warning(self, "Fehler", message)
    
    def set_buttons_enabled(self, enabled):
        """Aktiviere/Deaktiviere alle Buttons"""
        self.step1_btn.setEnabled(enabled)
        self.step2_btn.setEnabled(enabled)
        self.step3_btn.setEnabled(enabled)
        self.step4_btn.setEnabled(enabled)
        self.run_all_btn.setEnabled(enabled)
    
    def save_debug_logs(self):
        """Exportiere alle Debug-Logs aus dem neuesten run_* Ordner"""
        logs_dir = self.work_path / "logs"
        
        if not logs_dir.exists():
            QMessageBox.warning(self, "Keine Logs", "Der logs/ Ordner existiert nicht.\nFühren Sie zuerst eine Migration aus.")
            return
        
        # Finde alle run_* Verzeichnisse
        run_dirs = sorted(logs_dir.glob("run_*"), key=lambda x: x.name, reverse=True)
        
        if not run_dirs:
            QMessageBox.warning(self, "Keine Logs", "Keine Run-Verzeichnisse gefunden.\nFühren Sie zuerst eine Migration aus.")
            return
        
        # Nutze neuestes run_* Verzeichnis
        latest_run = run_dirs[0]
        
        # Finde alle Debug-Log Dateien im neuesten Run
        log_files = list(latest_run.glob("step*_debug.log"))
        
        if not log_files:
            QMessageBox.warning(self, "Keine Logs", f"Keine Debug-Logs in {latest_run.name} gefunden.")
            return
        
        # Zeige Auswahl-Dialog falls mehrere Runs vorhanden
        if len(run_dirs) > 1:
            from PyQt6.QtWidgets import QInputDialog
            items = [d.name for d in run_dirs]
            selected, ok = QInputDialog.getItem(
                self,
                "Run auswählen",
                f"Mehrere Runs gefunden. Welche Logs exportieren?\n(Neuester: {items[0]})",
                items,
                0,
                False
            )
            if ok and selected:
                latest_run = logs_dir / selected
                log_files = list(latest_run.glob("step*_debug.log"))
        
        # Wähle Zielverzeichnis
        target_dir = QFileDialog.getExistingDirectory(
            self,
            f"Debug-Logs exportieren nach ({latest_run.name})",
            str(Path.home() / "Desktop")
        )
        
        if not target_dir:
            return
        
        target_path = Path(target_dir)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            exported = []
            
            # Kopiere alle Debug-Logs
            for log_file in log_files:
                target_file = target_path / f"{latest_run.name}_{log_file.stem}.log"
                with open(log_file, 'r', encoding='utf-8') as src:
                    content = src.read()
                with open(target_file, 'w', encoding='utf-8') as dst:
                    dst.write(content)
                exported.append(target_file.name)
            
            # Kopiere column_mapping.json aus dem Run-Verzeichnis
            mapping_file = latest_run / "column_mapping.json"
            if mapping_file.exists():
                target_file = target_path / f"{latest_run.name}_column_mapping.json"
                with open(mapping_file, 'r', encoding='utf-8') as src:
                    content = src.read()
                with open(target_file, 'w', encoding='utf-8') as dst:
                    dst.write(content)
                exported.append(target_file.name)
            
            # Kopiere auch die aktuelle column_mapping.json aus logs/
            current_mapping = logs_dir / "column_mapping.json"
            if current_mapping.exists():
                target_file = target_path / f"column_mapping_current_{timestamp}.json"
                with open(current_mapping, 'r', encoding='utf-8') as src:
                    content = src.read()
                with open(target_file, 'w', encoding='utf-8') as dst:
                    dst.write(content)
                exported.append(target_file.name)
            
            self.log(f"{len(exported)} Datei(en) von {latest_run.name} exportiert nach: {target_dir}")
            
            files_list = "\n".join(f"  • {f}" for f in exported)
            QMessageBox.information(
                self,
                "Export erfolgreich",
                f"Run: {latest_run.name}\n\nFolgende Dateien wurden exportiert:\n\n{files_list}\n\nZiel: {target_dir}"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"Fehler beim Exportieren: {str(e)}")
    
    def view_column_mapping(self):
        """Zeige column_mapping.json in neuem Fenster"""
        
        mapping_file = self.work_path / "logs" / "column_mapping.json"
        
        if not mapping_file.exists():
            QMessageBox.information(
                self,
                "Keine Mappings",
                "Die Datei logs/column_mapping.json existiert nicht.\n\n"
                "Diese Datei wird erstellt, wenn Tabellen mit Spaltennamen >63 Zeichen migriert werden."
            )
            return
        
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                mapping_data = json.load(f)
            
            # Erstelle vergrößerbares Dialog-Fenster
            dialog = QDialog(self)
            dialog.setWindowTitle("Column Mapping")
            dialog.setMinimumSize(800, 600)
            dialog.resize(1000, 700)
            
            layout = QVBoxLayout(dialog)
            
            # Info-Label
            total_mappings = sum(len(cols) for cols in mapping_data.values())
            info_label = QLabel(
                f"Column Mapping:\n"
                f"• {len(mapping_data)} Tabelle(n) mit gekürzten Spaltennamen\n"
                f"• {total_mappings} Spalte(n) gekürzt (>63 Zeichen)"
            )
            info_label.setStyleSheet("font-weight: bold; padding: 10px; background-color: #E3F2FD;")
            layout.addWidget(info_label)
            
            # Text-Editor für JSON
            text_edit = QTextEdit()
            text_edit.setReadOnly(True)
            text_edit.setFont(QFont("Consolas", 10))
            
            # Formatiere JSON für Anzeige
            formatted = json.dumps(mapping_data, indent=2, ensure_ascii=False)
            text_edit.setPlainText(formatted)
            
            layout.addWidget(text_edit)
            
            # Button-Leiste
            button_layout = QHBoxLayout()
            
            save_btn = QPushButton("💾 Mapping speichern")
            save_btn.clicked.connect(lambda: self.save_mapping_file(mapping_file))
            save_btn.clicked.connect(dialog.accept)
            
            close_btn = QPushButton("Schließen")
            close_btn.clicked.connect(dialog.accept)
            
            button_layout.addStretch()
            button_layout.addWidget(save_btn)
            button_layout.addWidget(close_btn)
            
            layout.addLayout(button_layout)
            
            dialog.exec()
                
        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"Fehler beim Lesen der Mapping-Datei:\n{str(e)}")
    
    def save_mapping_file(self, source_file):
        """Speichere column_mapping.json an gewünschten Ort"""
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "Column Mapping speichern",
            f"column_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            "JSON Files (*.json);;All Files (*)"
        )
        
        if filename:
            try:
                with open(source_file, 'r', encoding='utf-8') as src:
                    content = src.read()
                with open(filename, 'w', encoding='utf-8') as dst:
                    dst.write(content)
                    
                self.log(f"Column Mapping gespeichert: {filename}")
                QMessageBox.information(self, "Erfolg", f"Mapping gespeichert:\n{filename}")
            except Exception as e:
                QMessageBox.critical(self, "Fehler", f"Fehler beim Speichern: {str(e)}")
    
    def test_mssql_connection(self):
        """Teste MSSQL Verbindung"""
        self.log("Teste MSSQL Verbindung...")
        
        try:
            import pyodbc
            
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.mssql_server[1].text()}"
            )
            
            # Füge Port hinzu wenn angegeben
            mssql_port = self.mssql_port[1].text().strip()
            if mssql_port:
                conn_str += f",{mssql_port}"
            
            conn_str += (
                f";DATABASE={self.mssql_database[1].text()};"
                f"UID={self.mssql_user[1].text()};"
                f"PWD={self.mssql_password[1].text()}"
            )
            
            conn = pyodbc.connect(conn_str, timeout=5)
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            conn.close()
            
            # Kürze Version auf erste Zeile
            version_short = version.split('\n')[0][:100]
            
            self.log(f"MSSQL Verbindung erfolgreich!")
            self.log(f"   Version: {version_short}")
            QMessageBox.information(
                self, 
                "Verbindung erfolgreich", 
                f"MSSQL Verbindung erfolgreich!\n\nServer: {self.mssql_server[1].text()}\nDatabase: {self.mssql_database[1].text()}\n\n{version_short}"
            )
            
        except Exception as e:
            self.log(f"MSSQL Verbindung fehlgeschlagen: {str(e)}")
            QMessageBox.critical(
                self,
                "Verbindungsfehler",
                f"MSSQL Verbindung fehlgeschlagen:\n\n{str(e)}\n\nBitte prüfen Sie:\n• Server erreichbar?\n• Credentials korrekt?\n• ODBC Driver 17 installiert?"
            )
    
    def test_pg_connection(self):
        """Teste PostgreSQL Verbindung"""
        self.log("Teste PostgreSQL Verbindung...")
        
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host=self.pg_host[1].text(),
                port=self.pg_port[1].text(),
                database=self.pg_database[1].text(),
                user=self.pg_user[1].text(),
                password=self.pg_password[1].text(),
                connect_timeout=5
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            conn.close()
            
            # Kürze Version
            version_short = version.split(',')[0]
            
            self.log(f"PostgreSQL Verbindung erfolgreich!")
            self.log(f"   Version: {version_short}")
            QMessageBox.information(
                self,
                "Verbindung erfolgreich",
                f"PostgreSQL Verbindung erfolgreich!\n\nHost: {self.pg_host[1].text()}\nDatabase: {self.pg_database[1].text()}\n\n{version_short}"
            )
            
        except Exception as e:
            self.log(f"PostgreSQL Verbindung fehlgeschlagen: {str(e)}")
            QMessageBox.critical(
                self,
                "Verbindungsfehler",
                f"PostgreSQL Verbindung fehlgeschlagen:\n\n{str(e)}\n\nBitte prüfen Sie:\n• Server läuft?\n• Credentials korrekt?\n• Firewall/Port offen?"
            )


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    window = MigrationGUI()
    
    # Mache work_path global verfügbar für Worker
    import __main__
    __main__.work_path = window.work_path
    
    window.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
