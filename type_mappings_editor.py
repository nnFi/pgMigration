"""
Type Mappings Editor - Bearbeite MSSQL zu PostgreSQL Datentyp-Mappings
"""

import json
from pathlib import Path
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QMessageBox, QHeaderView
)
from PyQt6.QtCore import Qt


class TypeMappingsEditor(QDialog):
    """Dialog zum Bearbeiten von Datentyp-Mappings"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Datentyp-Mappings bearbeiten")
        self.setGeometry(100, 100, 600, 500)
        self.config_file = Path('type_mappings_config.json')
        self.init_ui()
        self.load_mappings()
    
    def init_ui(self):
        """Initialisiere UI"""
        layout = QVBoxLayout()
        
        # Titel
        title = QLabel("MSSQL → PostgreSQL Datentyp-Mappings")
        title.setStyleSheet("font-weight: bold; font-size: 12px;")
        layout.addWidget(title)
        
        # Tabelle
        self.table = QTableWidget()
        self.table.setColumnCount(2)
        self.table.setHorizontalHeaderLabels(["MSSQL Typ", "PostgreSQL Typ"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.itemChanged.connect(self.on_table_changed)
        layout.addWidget(self.table)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        add_btn = QPushButton("➕ Hinzufügen")
        add_btn.clicked.connect(self.add_row)
        button_layout.addWidget(add_btn)
        
        delete_btn = QPushButton("🗑️ Löschen")
        delete_btn.clicked.connect(self.delete_row)
        button_layout.addWidget(delete_btn)
        
        button_layout.addStretch()
        
        save_btn = QPushButton("💾 Speichern")
        save_btn.setStyleSheet("background-color: #2E7D32; color: white; padding: 5px;")
        save_btn.clicked.connect(self.save_mappings)
        button_layout.addWidget(save_btn)
        
        close_btn = QPushButton("✕ Schließen")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        
        layout.addLayout(button_layout)
        
        # Feedback Label
        self.status_label = QLabel("Änderungen nicht gespeichert")
        self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")
        layout.addWidget(self.status_label)
        
        self.setLayout(layout)
        self.modified = False
    
    def load_mappings(self):
        """Lade Mappings aus JSON-Datei"""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    mappings = config.get('type_mappings', {})
            else:
                mappings = {}
            
            # Fülle Tabelle
            self.table.setRowCount(0)
            self.table.itemChanged.disconnect()  # Verhindere Signale während dem Laden
            
            for mssql_type, pg_type in sorted(mappings.items()):
                row = self.table.rowCount()
                self.table.insertRow(row)
                
                mssql_item = QTableWidgetItem(mssql_type)
                pg_item = QTableWidgetItem(pg_type)
                
                self.table.setItem(row, 0, mssql_item)
                self.table.setItem(row, 1, pg_item)
            
            self.table.itemChanged.connect(self.on_table_changed)
            self.modified = False
            self.status_label.setText("Mappings geladen")
            self.status_label.setStyleSheet("color: #4CAF50; font-size: 10px;")
            
        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"Konnte Mappings nicht laden: {e}")
    
    def add_row(self):
        """Füge neue Zeile hinzu"""
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, 0, QTableWidgetItem(""))
        self.table.setItem(row, 1, QTableWidgetItem(""))
        self.table.editItem(self.table.item(row, 0))
        self.on_table_changed()
    
    def delete_row(self):
        """Lösche ausgewählte Zeile"""
        current_row = self.table.currentRow()
        if current_row >= 0:
            self.table.removeRow(current_row)
            self.on_table_changed()
        else:
            QMessageBox.warning(self, "Warnung", "Bitte wähle eine Zeile aus")
    
    def on_table_changed(self):
        """Markiere als modifiziert"""
        self.modified = True
        self.status_label.setText("⚠️  Änderungen nicht gespeichert")
        self.status_label.setStyleSheet("color: #FF9800; font-size: 10px;")
    
    def save_mappings(self):
        """Speichere Mappings in JSON-Datei"""
        try:
            # Sammle Mappings aus Tabelle
            mappings = {}
            for row in range(self.table.rowCount()):
                mssql_item = self.table.item(row, 0)
                pg_item = self.table.item(row, 1)
                
                if mssql_item and pg_item:
                    mssql_type = mssql_item.text().strip()
                    pg_type = pg_item.text().strip()
                    
                    if mssql_type and pg_type:
                        mappings[mssql_type] = pg_type
            
            if not mappings:
                QMessageBox.warning(self, "Fehler", "Keine Mappings zu speichern")
                return
            
            # Speichere in JSON
            config = {'type_mappings': mappings}
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            self.modified = False
            self.status_label.setText("✓ Erfolgreich gespeichert")
            self.status_label.setStyleSheet("color: #4CAF50; font-size: 10px;")
            
            QMessageBox.information(
                self,
                "Erfolg",
                f"Datentyp-Mappings gespeichert!\n\n"
                f"Anzahl der Typen: {len(mappings)}"
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Fehler", f"Konnte Mappings nicht speichern: {e}")
