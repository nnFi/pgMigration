"""
Dialog-Fenster für Migration Tool
Zeigt Mapping, Logs und weitere Dialoge
"""

import json
from pathlib import Path
from datetime import datetime
from PyQt6.QtWidgets import (
    QDialog, QPushButton, QVBoxLayout, QHBoxLayout,
    QTextEdit, QLabel, QMessageBox, QFileDialog, QInputDialog
)
from PyQt6.QtGui import QFont


def show_column_mapping_dialog(parent, work_path, log_callback):
    """Zeige column_mapping.json in neuem Fenster"""
    
    mapping_file = work_path / "logs" / "column_mapping.json"
    
    if not mapping_file.exists():
        QMessageBox.information(
            parent,
            "Keine Mappings",
            "Die Datei logs/column_mapping.json existiert nicht.\n\n"
            "Diese Datei wird erstellt, wenn Tabellen mit Spaltennamen >63 Zeichen migriert werden."
        )
        return
    
    try:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            mapping_data = json.load(f)
        
        # Erstelle vergrößerbares Dialog-Fenster
        dialog = QDialog(parent)
        dialog.setWindowTitle("Column Mapping")
        dialog.setMinimumSize(800, 600)
        dialog.resize(1000, 700)
        
        layout = QVBoxLayout(dialog)
        
        # Info-Label
        total_mappings = sum(len(cols) for cols in mapping_data.values())
        info_label = QLabel(
            f"Column Mapping:\n"
            f"• {len(mapping_data)} Tabellen erfasst\n"
            f"• {total_mappings} Spalte(n) gekürzt (>63 Zeichen)"
        )
        info_label.setStyleSheet("font-weight: bold; padding: 10px; background-color: #E3F2FD; color: #000000;")
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
        save_btn.clicked.connect(lambda: save_mapping_file(parent, mapping_file, log_callback))
        save_btn.clicked.connect(dialog.accept)
        
        close_btn = QPushButton("Schließen")
        close_btn.clicked.connect(dialog.accept)
        
        button_layout.addStretch()
        button_layout.addWidget(save_btn)
        button_layout.addWidget(close_btn)
        
        layout.addLayout(button_layout)
        
        dialog.exec()
            
    except Exception as e:
        QMessageBox.critical(parent, "Fehler", f"Fehler beim Lesen der Mapping-Datei:\n{str(e)}")


def save_mapping_file(parent, source_file, log_callback):
    """Speichere column_mapping.json an gewünschten Ort"""
    filename, _ = QFileDialog.getSaveFileName(
        parent,
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
                
            log_callback(f"Column Mapping gespeichert: {filename}")
            QMessageBox.information(parent, "Erfolg", f"Mapping gespeichert:\n{filename}")
        except Exception as e:
            QMessageBox.critical(parent, "Fehler", f"Fehler beim Speichern: {str(e)}")


def save_debug_logs(parent, work_path, log_callback):
    """Exportiere alle Debug-Logs aus dem neuesten run_* Ordner"""
    logs_dir = work_path / "logs"
    
    if not logs_dir.exists():
        QMessageBox.warning(parent, "Keine Logs", "Der logs/ Ordner existiert nicht.\nFühren Sie zuerst eine Migration aus.")
        return
    
    # Finde alle run_* Verzeichnisse
    run_dirs = sorted(logs_dir.glob("run_*"), key=lambda x: x.name, reverse=True)
    
    if not run_dirs:
        QMessageBox.warning(parent, "Keine Logs", "Keine Run-Verzeichnisse gefunden.\nFühren Sie zuerst eine Migration aus.")
        return
    
    # Nutze neuestes run_* Verzeichnis
    latest_run = run_dirs[0]
    
    # Finde alle Debug-Log Dateien im neuesten Run
    log_files = list(latest_run.glob("step*_debug.log"))
    
    if not log_files:
        QMessageBox.warning(parent, "Keine Logs", f"Keine Debug-Logs in {latest_run.name} gefunden.")
        return
    
    # Zeige Auswahl-Dialog falls mehrere Runs vorhanden
    if len(run_dirs) > 1:
        items = [d.name for d in run_dirs]
        selected, ok = QInputDialog.getItem(
            parent,
            "Run auswählen",
            f"Mehrere Runs gefunden. Welche Logs exportieren?\n(Neuester: {items[0]})",
            items,
            0,
            False
        )
        if not ok or not selected:
            return
        latest_run = logs_dir / selected
        log_files = list(latest_run.glob("step*_debug.log"))
    
    # Wähle Zielverzeichnis
    target_dir = QFileDialog.getExistingDirectory(
        parent,
        f"Debug-Logs exportieren nach ({latest_run.name})",
        str(Path.home() / "Desktop")
    )
    
    if not target_dir or target_dir == "":
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
        
        log_callback(f"{len(exported)} Datei(en) von {latest_run.name} exportiert nach: {target_dir}")
        
        files_list = "\n".join(f"  • {f}" for f in exported)
        QMessageBox.information(
            parent,
            "Export erfolgreich",
            f"Run: {latest_run.name}\n\nFolgende Dateien wurden exportiert:\n\n{files_list}\n\nZiel: {target_dir}"
        )
        
    except Exception as e:
        QMessageBox.critical(parent, "Fehler", f"Fehler beim Exportieren: {str(e)}")


def edit_collations_config(parent, work_path, log_callback):
    """Öffne die collations_config.json zum Bearbeiten"""
    import sys
    import os
    import subprocess
    
    config_file = work_path / "collations_config.json"
    
    if not config_file.exists():
        QMessageBox.warning(
            parent,
            "Datei nicht gefunden",
            f"collations_config.json nicht gefunden in:\n{work_path}"
        )
        return
    
    try:
        # Öffne Datei mit Standard-Editor
        if sys.platform == 'win32':
            os.startfile(config_file)
        elif sys.platform == 'darwin':  # macOS
            subprocess.Popen(['open', config_file])
        else:  # Linux
            subprocess.Popen(['xdg-open', config_file])
        
        log_callback(f"Öffne: {config_file}")
    except Exception as e:
        QMessageBox.critical(
            parent,
            "Fehler",
            f"Fehler beim Öffnen der Datei:\n{str(e)}"
        )
        log_callback(f"Fehler beim Öffnen von {config_file}: {str(e)}")
