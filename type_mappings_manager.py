"""
Initialisierung der Type Mappings Konfiguration
Stellt sicher dass die JSON-Datei beim Start existiert
"""

import json
from pathlib import Path


# Standard-Fallback Mappings - zentrale Definition
DEFAULT_TYPE_MAPPINGS = {
    'bigint': 'BIGINT',
    'int': 'INTEGER',
    'smallint': 'SMALLINT',
    'tinyint': 'SMALLINT',
    'bit': 'BOOLEAN',
    'decimal': 'DECIMAL',
    'numeric': 'NUMERIC',
    'money': 'NUMERIC(19,4)',
    'smallmoney': 'NUMERIC(10,4)',
    'float': 'DOUBLE PRECISION',
    'real': 'REAL',
    'datetime': 'TIMESTAMPTZ',
    'datetime2': 'TIMESTAMPTZ',
    'smalldatetime': 'TIMESTAMPTZ',
    'date': 'DATE',
    'time': 'TIME',
    'datetimeoffset': 'TIMESTAMP WITH TIME ZONE',
    'char': 'CHAR',
    'varchar': 'VARCHAR',
    'text': 'TEXT',
    'nchar': 'CHAR',
    'nvarchar': 'VARCHAR',
    'ntext': 'TEXT',
    'binary': 'BYTEA',
    'varbinary': 'BYTEA',
    'image': 'BYTEA',
    'uniqueidentifier': 'UUID',
    'xml': 'XML'
}


def load_type_mappings_with_fallback():
    """Lade TYPE_MAPPING aus JSON oder gib Fallback zurück"""
    config_file = Path('type_mappings_config.json')
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                mappings = config.get('type_mappings', {})
                if mappings:
                    return mappings
        except Exception as e:
            print(f"Warnung: Konnte type_mappings_config.json nicht laden: {e}")
    
    # Fallback auf Standard-Mappings
    return DEFAULT_TYPE_MAPPINGS


def ensure_type_mappings_config():
    """Stelle sicher dass type_mappings_config.json existiert"""
    config_file = Path('type_mappings_config.json')
    
    if not config_file.exists():
        try:
            config = {'type_mappings': DEFAULT_TYPE_MAPPINGS}
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            print(f"✓ type_mappings_config.json erstellt mit {len(DEFAULT_TYPE_MAPPINGS)} Standard-Mappings")
        except Exception as e:
            print(f"✗ Fehler beim Erstellen von type_mappings_config.json: {e}")
    else:
        # Prüfe ob Datei valid ist
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                json.load(f)
        except Exception as e:
            print(f"✗ Fehler in type_mappings_config.json: {e}")
            # Recreate
            try:
                config = {'type_mappings': DEFAULT_TYPE_MAPPINGS}
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                print(f"✓ type_mappings_config.json neu erstellt")
            except Exception as e2:
                print(f"✗ Konnte Datei nicht neu erstellen: {e2}")


if __name__ == '__main__':
    ensure_type_mappings_config()
