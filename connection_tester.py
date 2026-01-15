"""
Connection Tester für Migration Tool
Testet MSSQL und PostgreSQL Verbindungen
"""


def test_mssql_connection(server, port, database, user, password):
    """Teste MSSQL Verbindung und gebe Version zurück"""
    try:
        import pyodbc
        
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server}"
        )
        
        # Füge Port hinzu wenn angegeben
        if port:
            conn_str += f",{port}"
        
        conn_str += (
            f";DATABASE={database};"
            f"UID={user};"
            f"PWD={password}"
        )
        
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        conn.close()
        
        # Kürze Version auf erste Zeile
        version_short = version.split('\n')[0][:100]
        
        return True, version_short
        
    except Exception as e:
        return False, str(e)


def test_pg_connection(host, port, database, user, password):
    """Teste PostgreSQL Verbindung und gebe Version zurück"""
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=5
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        conn.close()
        
        # Kürze Version
        version_short = version.split(',')[0]
        
        return True, version_short
        
    except Exception as e:
        return False, str(e)
