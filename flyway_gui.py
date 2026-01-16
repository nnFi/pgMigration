"""
Flyway Converter GUI Builder
Baut die UI-Komponenten für den Flyway SQL Converter auf
"""

from PyQt6.QtWidgets import (
    QVBoxLayout, QHBoxLayout, QGroupBox, QPushButton, QLabel, QTextEdit,
    QFileDialog, QMessageBox
)
from PyQt6.QtGui import QFont


def connect_flyway_buttons(controls, source_callback, target_callback, convert_callback, download_callback, clear_logs_callback):
    """Verbinde Flyway Buttons mit ihren Callbacks"""
    controls['source_dir_btn'].clicked.connect(source_callback)
    controls['target_dir_btn'].clicked.connect(target_callback)
    controls['convert_btn'].clicked.connect(convert_callback)
    controls['download_btn'].clicked.connect(download_callback)
    controls['clear_logs_btn'].clicked.connect(clear_logs_callback)


def build_flyway_section():
    """Baue Flyway Converter Sektion"""
    flyway_group = QGroupBox("Flyway SQL Converter - MSSQL zu PostgreSQL")
    flyway_layout = QVBoxLayout()
    
    # Verzeichnis-Auswahl
    dir_layout = QHBoxLayout()
    
    source_label = QLabel("Quellverzeichnis:")
    source_label.setMinimumWidth(120)
    source_dir_btn = QPushButton("📁 Wählen...")
    source_dir_label = QLabel("(nicht ausgewählt)")
    source_dir_label.setStyleSheet("color: gray;")
    
    dir_layout.addWidget(source_label)
    dir_layout.addWidget(source_dir_btn)
    dir_layout.addWidget(source_dir_label)
    dir_layout.addStretch()
    flyway_layout.addLayout(dir_layout)
    
    dir_layout2 = QHBoxLayout()
    
    target_label = QLabel("Zielverzeichnis:")
    target_label.setMinimumWidth(120)
    target_dir_btn = QPushButton("📁 Wählen...")
    target_dir_label = QLabel("(nicht ausgewählt)")
    target_dir_label.setStyleSheet("color: gray;")
    
    dir_layout2.addWidget(target_label)
    dir_layout2.addWidget(target_dir_btn)
    dir_layout2.addWidget(target_dir_label)
    dir_layout2.addStretch()
    flyway_layout.addLayout(dir_layout2)
    
    # Convert Button - grün wie "ALLE SCHRITTE AUSFÜHREN"
    convert_btn = QPushButton("Konvertiere SQL-Scripts")
    convert_btn.setFont(QFont("Arial", 11, QFont.Weight.Bold))
    convert_btn.setMinimumHeight(40)
    convert_btn.setStyleSheet("""
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
    flyway_layout.addWidget(convert_btn)
    
    # Ergebnis-Anzeige
    result_label = QLabel("Konvertierungs-Ergebnisse:")
    flyway_layout.addWidget(result_label)
    
    result_text = QTextEdit()
    result_text.setReadOnly(True)
    result_text.setFont(QFont("Consolas", 9))
    result_text.setMaximumHeight(150)
    result_text.setStyleSheet("background-color: #1E1E1E; color: #D4D4D4;")
    flyway_layout.addWidget(result_text)
    
    # Download Button für Logs - wie Debug-Logs Button (kein spezielles Styling)
    download_btn = QPushButton("📥 Flyway-Logs exportieren")
    clear_logs_btn = QPushButton("🗑️ Konvertierungs Logs löschen")
    
    logs_btn_layout = QHBoxLayout()
    logs_btn_layout.addWidget(clear_logs_btn)
    logs_btn_layout.addWidget(download_btn)
    logs_btn_layout.addStretch()
    flyway_layout.addLayout(logs_btn_layout)
    
    flyway_group.setLayout(flyway_layout)
    
    return flyway_group, {
        'source_dir_btn': source_dir_btn,
        'source_dir_label': source_dir_label,
        'target_dir_btn': target_dir_btn,
        'target_dir_label': target_dir_label,
        'convert_btn': convert_btn,
        'download_btn': download_btn,
        'clear_logs_btn': clear_logs_btn,
        'result_text': result_text,
    }
