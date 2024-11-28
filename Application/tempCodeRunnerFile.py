    def execute_query(self, query_info: Dict[str, str]) -> Optional[pd.DataFrame]:
        """Execute the generated SQL query"""
        try:
            # Handle multi-database queries
            if isinstance(query_info['database'], list):
                results = {}
                for db in query_info['database']:
                    db_file = self.available_databases.get(db)
                    if not db_file:
                        raise ValueError(f"Database {db} not found")
                    
                    conn = sqlite3.connect(os.path.join(self.db_path, db_file))
                    try:
                        query = query_info['queries'][db]  # Get query for this specific database
                        results[db] = pd.read_sql_query(query, conn)
                    finally:
                        conn.close()
                return results
            else:
                # Handle single database query (existing code)
                db_file = self.available_databases.get(query_info['database'])
                if not db_file:
                    raise ValueError(f"Database {query_info['database']} not found")

                conn = sqlite3.connect(os.path.join(self.db_path, db_file))
                try:
                    result = pd.read_sql_query(query_info['query'], conn)
                    return result
                finally:
                    conn.close()
                    
        except sqlite3.Error as e:
            print(f"Error executing query: {e}")
            return None