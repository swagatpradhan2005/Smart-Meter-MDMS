"""
SQL Execution Module - Run SQL scripts and save outputs
Handles database initialization, table creation, data insertion, and query execution.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import logging
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils import setup_logger

logger = setup_logger(__name__, 'sql_runner.log')


class SQLRunner:
    """Execute SQL scripts and manage database operations."""
    
    def __init__(self, db_path: str = "data/smart_meter.db", sql_dir: str = "sql"):
        """
        Initialize SQL runner.
        
        Args:
            db_path: Path to SQLite database file
            sql_dir: Directory containing SQL scripts
        """
        self.db_path = Path(db_path)
        self.sql_dir = Path(sql_dir)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = None
        self.cursor = None
        self.query_results = {}
        
        logger.info(f"SQL Runner initialized with database: {self.db_path}")
    
    def connect(self) -> None:
        """Connect to SQLite database."""
        try:
            self.connection = sqlite3.connect(str(self.db_path))
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self) -> None:
        """Disconnect from database."""
        try:
            if self.connection:
                self.connection.close()
                logger.info("Disconnected from database")
        except Exception as e:
            logger.error(f"Error disconnecting: {e}")
    
    def execute_sql_file(self, sql_file: str) -> List[str]:
        """
        Execute SQL file containing multiple statements.
        
        Args:
            sql_file: Name of SQL file in sql_dir (e.g., 'schema.sql')
            
        Returns:
            List of SQL statements executed
        """
        file_path = self.sql_dir / sql_file
        
        if not file_path.exists():
            logger.error(f"SQL file not found: {file_path}")
            return []
        
        logger.info(f"Executing SQL file: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            # Split by semicolon and execute each statement
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]
            
            for i, statement in enumerate(statements, 1):
                try:
                    self.cursor.execute(statement)
                    self.connection.commit()
                    logger.info(f"  [{i}/{len(statements)}] Statement executed")
                except Exception as e:
                    logger.warning(f"  Statement {i} failed: {e}")
                    self.connection.rollback()
            
            logger.info(f"Completed {len(statements)} statements from {sql_file}")
            return statements
        
        except Exception as e:
            logger.error(f"Failed to execute SQL file: {e}")
            return []
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        if_exists: str = 'replace') -> int:
        """
        Insert DataFrame into SQL table.
        
        Args:
            df: Input DataFrame
            table_name: Target table name
            if_exists: 'replace' (drop table), 'append' (insert rows), or 'fail' (error if exists)
            
        Returns:
            Number of rows inserted
        """
        try:
            rows = len(df)
            df.to_sql(table_name, self.connection, if_exists=if_exists, index=False)
            self.connection.commit()
            logger.info(f"Inserted {rows} rows into table '{table_name}'")
            return rows
        except Exception as e:
            logger.error(f"Failed to insert DataFrame into {table_name}: {e}")
            return 0
    
    def execute_query(self, query: str, query_name: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Execute SQL query and return results as DataFrame.
        
        Args:
            query: SQL query string
            query_name: Optional name for the query (for logging)
            
        Returns:
            ResultDataFrame or None if failed
        """
        try:
            logger.info(f"Executing query: {query_name or 'unnamed'}")
            result_df = pd.read_sql_query(query, self.connection)
            logger.info(f"Query returned {len(result_df)} rows")
            return result_df
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return None
    
    def execute_query_file(self, sql_file: str) -> Dict[str, pd.DataFrame]:
        """
        Execute query file and save results.
        
        Args:
            sql_file: Name of SQL file in sql_dir
            
        Returns:
            Dictionary of {query_name: result_DataFrame}
        """
        file_path = self.sql_dir / sql_file
        
        if not file_path.exists():
            logger.error(f"SQL file not found: {file_path}")
            return {}
        
        logger.info(f"Executing queries from: {sql_file}")
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Simple query splitting (assumes each query ends with semicolon)
            queries = [q.strip() for q in content.split(';') if q.strip()]
            
            results = {}
            for query in queries:
                # Extract query name from comment if exists
                lines = query.split('\n')
                name = 'query'
                
                for line in lines:
                    if '-- QUERY:' in line:
                        name = line.replace('-- QUERY:', '').strip()
                        break
                
                result_df = self.execute_query(query, name)
                if result_df is not None:
                    results[name] = result_df
                    self.query_results[name] = result_df
            
            logger.info(f"Executed {len(results)} queries from {sql_file}")
            return results
        
        except Exception as e:
            logger.error(f"Failed to execute query file: {e}")
            return {}
    
    def save_query_results(self, output_dir: str = "outputs/reports") -> Dict[str, str]:
        """
        Save all query results to CSV files.
        
        Args:
            output_dir: Directory to save CSV files
            
        Returns:
            Dictionary of {query_name: output_file_path}
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        saved_files = {}
        
        for query_name, result_df in self.query_results.items():
            if result_df is not None and len(result_df) > 0:
                # Safe filename from query name
                safe_name = query_name.lower().replace(' ', '_').replace('-', '_')
                file_path = output_path / f"{safe_name}.csv"
                
                try:
                    result_df.to_csv(file_path, index=False)
                    logger.info(f"Saved {len(result_df)} rows to {file_path}")
                    saved_files[query_name] = str(file_path)
                except Exception as e:
                    logger.error(f"Failed to save {query_name}: {e}")
        
        return saved_files
    
    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """Get metadata about a table."""
        try:
            query = f"PRAGMA table_info({table_name})"
            result = self.cursor.execute(query).fetchall()
            
            if not result:
                logger.warning(f"Table '{table_name}' not found")
                return None
            
            columns = {}
            for row in result:
                col_name, col_type = row[1], row[2]
                columns[col_name] = col_type
            
            logger.info(f"Table '{table_name}' has {len(columns)} columns")
            return columns
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return None
    
    def list_tables(self) -> List[str]:
        """List all tables in database."""
        try:
            query = "SELECT name FROM sqlite_master WHERE type='table'"
            result = self.cursor.execute(query).fetchall()
            tables = [row[0] for row in result]
            logger.info(f"Database contains {len(tables)} tables: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            return []
    
    def close(self) -> None:
        """Close database connection."""
        self.disconnect()


def main():
    """Example usage."""
    logger.info("SQL Runner module loaded successfully")


if __name__ == "__main__":
    main()
