"""
MSSQL zu PostgreSQL Migration Tool - GUI (REFAKTORIERT)
Professionelle Desktop-Anwendung mit PyQt6
"""

import sys
import importlib.util
import os
import shutil
from datetime import datetime
from pathlib import Path
import io
from contextlib import redirect_stdout, redirect_stderr

from PyQt6.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QMessageBox, QFileDialog
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QTimer
from PyQt6.QtGui import QTextCursor

# Importiere Module
from collations_manager import ensure_collations_config
from gui_builder import build_database_section, build_migration_steps_section, build_log_section
from config_manager import save_env, load_env_into_ui, get_env_vars_from_ui
from connection_tester import test_mssql_connection, test_pg_connection
from dialogs import show_column_mapping_dialog, save_debug_logs, edit_collations_config


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
            old_env = {}
            for key, value in self.env_vars.items():
                old_env[key] = os.environ.get(key)
                os.environ[key] = value
            
            import __main__
            if hasattr(__main__, 'work_path'):
                work_dir = __main__.work_path
            else:
                work_dir = Path(self.script_path).parent
            
            old_cwd = os.getcwd()
            os.chdir(work_dir)
            
            script_name = Path(self.script_path).stem
            
            try:
                if script_name in sys.modules:
                    del sys.modules[script_name]
                
                spec = importlib.util.spec_from_file_location(script_name, self.script_path)
                module = importlib.util.module_from_spec(spec)
                
                with redirect_stdout(self.output_buffer), redirect_stderr(self.output_buffer):
                    spec.loader.exec_module(module)
                    if hasattr(module, 'main'):
                        module.main()
                
                remaining = self.get_new_output()
                for line in remaining.split('\n'):
                    if line.strip():
                        self.log_output.emit(line)
                
                self.finished.emit(True, f"{script_name} erfolgreich abgeschlossen")
                
            except SystemExit as e:
                remaining = self.get_new_output()
                for line in remaining.split('\n'):
                    if line.strip():
                        self.log_output.emit(line)
                
                if e.code == 0:
                    self.finished.emit(True, f"{script_name} erfolgreich abgeschlossen")
                else:
                    self.finished.emit(False, f"{script_name} fehlgeschlagen (Exit Code: {e.code})")
            
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
    """Hauptklasse für die Migration GUI"""
    
    def __init__(self):
        super().__init__()
        self.setWindowTitle("MSSQL → PostgreSQL Migration Tool")
        self.setMinimumSize(1000, 800)
        
        self.worker = None
        
        if getattr(sys, 'frozen', False):
            self.base_path = Path(sys._MEIPASS)
            self.work_path = Path(sys.executable).parent
        else:
            self.base_path = Path(__file__).parent
            self.work_path = Path(__file__).parent
        
        self.setup_ui()
        ensure_collations_config(self.work_path)
        self.load_env()
        
    def setup_ui(self):
        """Erstelle Benutzeroberfläche"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        db_group, self.db_controls = build_database_section()
        steps_group, self.step_controls = build_migration_steps_section()
        log_group, self.log_controls = build_log_section()
        
        main_layout.addWidget(db_group)
        main_layout.addWidget(steps_group)
        main_layout.addWidget(log_group, stretch=1)
        
        self._connect_signals()
        self.statusBar().showMessage("Bereit")
    
    def _connect_signals(self):
        """Verbinde alle Button-Signale dynamisch"""
        central = self.centralWidget()
        layout = central.layout()
        
        for i in range(layout.count()):
            group = layout.itemAt(i).widget()
            if not group or not hasattr(group, 'title'):
                continue
                
            inner_layout = group.layout()
            
            for j in range(inner_layout.count()):
                item = inner_layout.itemAt(j)
                if not item:
                    continue
                    
                if hasattr(item, 'count'):
                    for k in range(item.count()):
                        widget = item.itemAt(k).widget() if hasattr(item.itemAt(k), 'widget') else None
                        if widget and hasattr(widget, 'clicked'):
                            self._connect_button(widget)
                else:
                    widget = item.widget() if hasattr(item, 'widget') else None
                    if widget and hasattr(widget, 'clicked'):
                        self._connect_button(widget)
    
    def _connect_button(self, button):
        """Verbinde einzelnen Button basierend auf Text"""
        text = button.text()
        
        if "Konfiguration speichern" in text:
            button.clicked.connect(self.save_env)
        elif "Konfiguration laden" in text:
            button.clicked.connect(self.load_env)
        elif "Konfiguration importieren" in text:
            button.clicked.connect(self.import_env)
        elif "MSSQL testen" in text:
            button.clicked.connect(self.test_mssql_connection)
        elif "PostgreSQL testen" in text:
            button.clicked.connect(self.test_pg_connection)
        elif "1️⃣" in text:
            button.clicked.connect(lambda: self.run_single_step_with_warning("step1_migrate_data.py", 1))
        elif "2️⃣" in text:
            button.clicked.connect(lambda: self.run_single_step_with_warning("step2_verify_columns.py", 2))
        elif "3️⃣" in text:
            button.clicked.connect(lambda: self.run_single_step_with_warning("step3_migrate_constraints.py", 3))
        elif "4️⃣" in text:
            button.clicked.connect(lambda: self.run_single_step_with_warning("step4_migrate_collations.py", 4))
        elif "ALLE" in text:
            button.clicked.connect(self.run_all_steps)
        elif "Log löschen" in text:
            button.clicked.connect(self.clear_log)
        elif "Debug-Logs" in text:
            button.clicked.connect(self.export_debug_logs)
        elif "Column Mapping" in text:
            button.clicked.connect(self.view_column_mapping)
        elif "konfigurieren" in text.lower():
            button.clicked.connect(self.edit_collations_config_dialog)
    
    # ========== Config-Methoden ==========
    
    def get_env_vars(self):
        return get_env_vars_from_ui(self.db_controls)
    
    def save_env(self):
        save_env(self.work_path, self.get_env_vars(), self.log)
    
    def load_env(self):
        try:
            env_path = self.work_path / ".env"
            if env_path.exists():
                load_env_into_ui(env_path, self.db_controls)
                self.log("Konfiguration geladen aus .env")
        except Exception as e:
            self.log(f"Fehler beim Laden der Konfiguration: {str(e)}")
    
    def import_env(self):
        filename, _ = QFileDialog.getOpenFileName(
            self, "Konfiguration (.env) importieren",
            str(Path.home()), "ENV Files (*.env);;All Files (*)"
        )
        
        if filename:
            try:
                env_path = Path(filename)
                load_env_into_ui(env_path, self.db_controls)
                target_path = self.work_path / ".env"
                shutil.copy2(env_path, target_path)
                self.log(f"Konfiguration importiert aus: {filename}")
                self.log(f"Gespeichert als: {target_path}")
            except Exception as e:
                QMessageBox.critical(self, "Fehler", f"Fehler beim Importieren: {str(e)}")
                self.log(f"Fehler beim Importieren: {str(e)}")
    
    # ========== Log-Methoden ==========
    
    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_widget = self.log_controls['log_output']
        log_widget.append(f"[{timestamp}] {message}")
        log_widget.moveCursor(QTextCursor.MoveOperation.End)
    
    def clear_log(self):
        self.log_controls['log_output'].clear()
        if hasattr(self, 'live_timer') and self.live_timer.isActive():
            self.live_timer.stop()
        if self.worker:
            self.worker = None
    
    def export_debug_logs(self):
        save_debug_logs(self, self.work_path, self.log)
    
    def view_column_mapping(self):
        show_column_mapping_dialog(self, self.work_path, self.log)
    
    def edit_collations_config_dialog(self):
        edit_collations_config(self, self.work_path, self.log)
    
    # ========== Migration-Methoden ==========
    
    def run_single_step_with_warning(self, script_name, step_number):
        reply = QMessageBox.warning(
            self, "Warnung: Einzelner Schritt",
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
        self.step_controls['progress'].setValue(0)
        self.statusBar().showMessage(f"Läuft: {script_name}...")
        
        self.worker = MigrationWorker(str(script_path), self.get_env_vars())
        self.worker.log_output.connect(self.log)
        self.worker.finished.connect(self.on_step_finished)
        
        self.live_timer = QTimer()
        self.live_timer.timeout.connect(self.update_live_output)
        self.live_timer.start(100)
        
        self.worker.start()
    
    def update_live_output(self):
        if self.worker and self.worker.isRunning():
            new_output = self.worker.get_new_output()
            if new_output:
                for line in new_output.split('\n'):
                    if line.strip():
                        if line.startswith('PROGRESS:'):
                            try:
                                progress = int(line.split(':')[1])
                                self.step_controls['progress'].setValue(progress)
                            except:
                                pass
                        else:
                            self.log(line)
        else:
            if hasattr(self, 'live_timer'):
                self.live_timer.stop()
    
    def run_all_steps(self):
        step4_checked = self.db_controls['step4_checkbox'].isChecked()
        step4_text = "4. Collations" if step4_checked else "4. Collations (übersprungen)"
        
        reply = QMessageBox.question(
            self, "Bestätigung",
            f"Alle Migrationsschritte ausführen?\n\n1. Tabellen & Daten\n2. Verifizierung\n"
            f"3. Constraints & Indexes\n{step4_text}",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            if not step4_checked:
                os.environ['SKIP_STEP4'] = 'true'
            else:
                os.environ.pop('SKIP_STEP4', None)
            self.run_step("run_all.py")
    
    def on_step_finished(self, success, message):
        self.set_buttons_enabled(True)
        progress = self.step_controls['progress']
        progress.setValue(100 if success else 0)
        
        self.log(message)
        self.statusBar().showMessage(message, 5000)
        
        if not success:
            QMessageBox.warning(self, "Fehler", message)
    
    def set_buttons_enabled(self, enabled):
        pass  # Buttons sind in den Gruppen, nicht direkt zugänglich
    
    def test_mssql_connection(self):
        self.log("Teste MSSQL Verbindung...")
        
        success, result = test_mssql_connection(
            self.db_controls['mssql_server'][1].text(),
            self.db_controls['mssql_port'][1].text().strip(),
            self.db_controls['mssql_database'][1].text(),
            self.db_controls['mssql_user'][1].text(),
            self.db_controls['mssql_password'][1].text(),
        )
        
        if success:
            self.log(f"MSSQL Verbindung erfolgreich!")
            self.log(f"   Version: {result}")
            QMessageBox.information(
                self, "Verbindung erfolgreich",
                f"MSSQL Verbindung erfolgreich!\n\nServer: {self.db_controls['mssql_server'][1].text()}\n"
                f"Database: {self.db_controls['mssql_database'][1].text()}\n\n{result}"
            )
        else:
            self.log(f"MSSQL Verbindung fehlgeschlagen: {result}")
            QMessageBox.critical(
                self, "Verbindungsfehler",
                f"MSSQL Verbindung fehlgeschlagen:\n\n{result}\n\nBitte prüfen Sie:\n"
                "• Server erreichbar?\n• Credentials korrekt?\n• ODBC Driver 17 installiert?"
            )
    
    def test_pg_connection(self):
        self.log("Teste PostgreSQL Verbindung...")
        
        success, result = test_pg_connection(
            self.db_controls['pg_host'][1].text(),
            self.db_controls['pg_port'][1].text(),
            self.db_controls['pg_database'][1].text(),
            self.db_controls['pg_user'][1].text(),
            self.db_controls['pg_password'][1].text(),
        )
        
        if success:
            self.log(f"PostgreSQL Verbindung erfolgreich!")
            self.log(f"   Version: {result}")
            QMessageBox.information(
                self, "Verbindung erfolgreich",
                f"PostgreSQL Verbindung erfolgreich!\n\nHost: {self.db_controls['pg_host'][1].text()}\n"
                f"Database: {self.db_controls['pg_database'][1].text()}\n\n{result}"
            )
        else:
            self.log(f"PostgreSQL Verbindung fehlgeschlagen: {result}")
            QMessageBox.critical(
                self, "Verbindungsfehler",
                f"PostgreSQL Verbindung fehlgeschlagen:\n\n{result}\n\nBitte prüfen Sie:\n"
                "• Server läuft?\n• Credentials korrekt?\n• Firewall/Port offen?"
            )


def main():
    """Haupteinstiegspunkt"""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    window = MigrationGUI()
    
    import __main__
    __main__.work_path = window.work_path
    
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
