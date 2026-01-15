"""
Collations Manager - Verwaltet collations_config.json
"""

import json
from pathlib import Path


DEFAULT_COLLATIONS_CONFIG = {
    "_comment": "Collations Konfiguration für Step 4",
    "_description": "Definieren Sie hier das Mapping von MSSQL zu PostgreSQL Collations",
    "_help": "Für jede MSSQL Collation können mehrere PostgreSQL Optionen definiert werden. Die erste verfügbare wird verwendet.",
    "collations": {
        "SQL_Latin1_General_CP1_CI_AS": [
            "de-DE-x-icu",
            "de_DE.utf8",
            "de_DE",
            "en-US-x-icu",
            "en_US.utf8",
            "C.UTF-8",
            "default"
        ],
        "Latin1_General_CI_AS": [
            "de-DE-x-icu",
            "de_DE.utf8",
            "de_DE",
            "en-US-x-icu",
            "en_US.utf8",
            "C.UTF-8",
            "default"
        ],
        "SQL_Latin1_General_CP1_CS_AS": ["C"],
        "Latin1_General_CS_AS": ["C"],
        "German_PhoneBook_CI_AS": [
            "de-DE-x-icu",
            "de_DE.utf8",
            "de_DE",
            "default"
        ],
        "SQL_Latin1_General_CP850_CI_AS": [
            "de-DE-x-icu",
            "de_DE.utf8",
            "de_DE",
            "en-US-x-icu",
            "en_US.utf8",
            "default"
        ],
        "default": [
            "en-US-x-icu",
            "en_US.utf8",
            "C.UTF-8",
            "default"
        ]
    },
    "_examples": {
        "CI_AS bedeutet": "Case Insensitive, Accent Sensitive",
        "CS_AS bedeutet": "Case Sensitive, Accent Sensitive",
        "_": "Mehr MSSQL Collations können beliebig hinzugefügt werden"
    }
}


def ensure_collations_config(work_path):
    """
    Erstelle collations_config.json falls nicht vorhanden
    
    Args:
        work_path: Path-Objekt zum Arbeitsverzeichnis
    """
    config_file = work_path / "collations_config.json"
    
    if config_file.exists():
        return True  # Datei existiert bereits
    
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_COLLATIONS_CONFIG, f, indent=2, ensure_ascii=False)
        print(f"Collations Konfiguration erstellt: {config_file}")
        return True
    except Exception as e:
        print(f"Fehler beim Erstellen von collations_config.json: {e}")
        return False
