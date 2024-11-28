import json
from typing import Dict, List, Any, Optional
import sqlite3
import pandas as pd
import os
from datetime import datetime
from openai import OpenAI


class QueryHistory:
    def __init__(self):
        self.queries: List[Dict] = []
        self.max_history = 20  # Store more queries than conversations
        
    def add_query(self, 
                  question: str, 
                  query_info: Dict[str, str], 
                  timestamp: Optional[datetime] = None) -> None:
        """Add a query to the history"""
        if timestamp is None:
            timestamp = datetime.now()
            
        query_record = {
            "timestamp": timestamp,
            "question": question,
            "database": query_info.get('database'),
            "query": query_info.get('query'),
            "explanation": query_info.get('explanation')
        }
        
        self.queries.append(query_record)
        
        # Keep only the last max_history queries
        if len(self.queries) > self.max_history:
            self.queries = self.queries[-self.max_history:]
            
    def get_recent_queries(self, 
                          limit: Optional[int] = None, 
                          database: Optional[str] = None) -> List[Dict]:
        """Get recent queries, optionally filtered by database"""
        queries = self.queries
        if database:
            queries = [q for q in queries if q['database'] == database]
            
        if limit is None or limit > len(queries):
            return queries
       
        return queries[-limit:]
    
    def clear_history(self) -> None:
        """Clear query history"""
        self.queries = []

class ChatHistory:
    def __init__(self):
        self.conversations: List[Dict] = []
        self.max_history = 10
        
    def add_conversation(self, 
                        question: str, 
                        answer: Dict[str, Any], 
                        timestamp: Optional[datetime] = None) -> None:
        if timestamp is None:
            timestamp = datetime.now()
            
        conversation = {
            "timestamp": timestamp,
            "question": question,
            "answer": answer
        }
        
        self.conversations.append(conversation)
        
        if len(self.conversations) > self.max_history:
            self.conversations = self.conversations[-self.max_history:]
            
    def get_recent_conversations(self, limit: Optional[int] = None) -> List[Dict]:
        if limit is None or limit > len(self.conversations):
            return self.conversations
        return self.conversations[-limit:]
    
    def clear_history(self) -> None:
        self.conversations = []

class NvidiaLLMQueryGenerator:
    def __init__(self):
        """Initialize the Database Query Generator with NVIDIA's NeMo LLM"""
        self.db_path = "Transformed_database"
        self.available_databases = {
            'crop_prices': 'crop_prices_transformed.db',
            'soil_health': 'soil_health_transformed.db',
            'irrigation': 'irrigation_transformed.db',
            'crop_data': 'transformed_crop_data.db',
            'fertilizer': 'fertilizer_recommendation.db',
            'weather_data': 'weather_data.db',
            'soil_types': 'soil_types.db'
        }

        # Initialize NVIDIA API client
        self.client = OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key="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"
        )

        self.table_schemas = self._load_table_schemas()
        self.system_prompt = self._get_default_system_prompt()

        # Add both chat and query history
        self.chat_history = ChatHistory()
        self.query_history = QueryHistory()

    def _load_table_schemas(self) -> Dict[str, Dict[str, List[str]]]:
        """Load schemas for all available tables"""
        schemas = {}
        for db_name, db_file in self.available_databases.items():
            try:
                conn = sqlite3.connect(os.path.join(self.db_path, db_file))
                cursor = conn.cursor()

                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()

                schemas[db_name] = {}
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"PRAGMA table_info({table_name});")
                    columns = cursor.fetchall()
                    schemas[db_name][table_name] = [
                        f"{col[1]} ({col[2]})" for col in columns
                    ]

                conn.close()
            except sqlite3.Error as e:
                print(f"Error loading schema for {db_name}: {e}")
        return schemas

    def _get_default_system_prompt(self) -> str:
        """Generate system prompt with database schemas"""
        prompt = "You are an agricultural database expert. Generate SQL queries based on the following schema:\n"
        for db_name, tables in self.table_schemas.items():
            prompt += f"\n{db_name} DATABASE:\n"
            for table_name, columns in tables.items():
                prompt += f"Table: {table_name}\n"
                prompt += "Columns: " + ", ".join(columns) + "\n"

        prompt += """
        Generate a SQL query to answer the user's question. Return your response in JSON format:
        {
            "database": "database_name",
            "query": "SQL query",
            "explanation": "Brief explanation of what the query does"
        }
        """
        return prompt

    async def generate_query(self, user_question: str) -> Dict[str, str]:
        """Generate SQL query using NVIDIA's LLM"""
        try:
            messages = [
                {"role": "system", "content": """
You are an agricultural database expert. Generate SQL queries based on the schema provided.
For complex questions requiring multiple databases, return a JSON in this format:
{
    "database": ["database1", "database2"],
    "queries": {
        "database1": "query1",
        "database2": "query2"
    },
    "explanation": "brief explanation"
}

For single database queries, use this format:
{
    "database": "database_name",
    "query": "single_line_sql_query",
    "explanation": "brief explanation"
}
"""},
                {"role": "user", "content": self.system_prompt + "\n\nQuestion: " + user_question}
            ]
            
            completion = self.client.chat.completions.create(
                model="nvidia/llama-3.1-nemotron-70b-instruct",
                messages=messages,
                temperature=0.3,
                top_p=1,
                max_tokens=1024,
                stream=False
            )
            
            response_content = completion.choices[0].message.content.strip()
            
            # Clean up the response
            try:
                # Find the JSON content
                json_start = response_content.find('{')
                json_end = response_content.rfind('}') + 1
                
                if json_start != -1 and json_end != -1:
                    json_str = response_content[json_start:json_end]
                    
                    # Clean the JSON string
                    json_str = (json_str
                              .replace('\n', ' ')           # Remove newlines
                              .replace('\\', '')            # Remove backslashes
                              .replace('"""', '"')          # Fix triple quotes
                              .replace('  ', ' ')           # Remove double spaces
                              .strip())
                    
                    # Parse the JSON
                    query_info = json.loads(json_str)
                    
                    # Clean up the query - remove extra whitespace and newlines
                    if 'query' in query_info:
                        query_info['query'] = ' '.join(query_info['query'].split())
                    
                    # Add to query history
                    self.query_history.add_query(user_question, query_info)
                    
                    return query_info
                else:
                    print("No JSON found in response")
                    print(f"Raw response: {response_content}")
                    return {"error": "Invalid response format"}
                    
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON response: {e}")
                print(f"Raw response: {response_content}")
                return {"error": "Failed to parse response"}
                
        except Exception as e:
            print(f"Error generating query: {e}")
            return {"error": f"Query generation failed: {str(e)}"}

    async def get_similar_queries(self, question: str, limit: int = 3) -> List[Dict]:
        """Get similar previous queries based on the question"""
        try:
            # Create a prompt to find similar queries
            context = f"""Find the most relevant previous queries from this list that relate to the current question:

Current Question: {question}

Previous Queries:
{json.dumps([{
    'question': q['question'],
    'query': q['query'],
    'database': q['database']
} for q in self.query_history.get_recent_queries(10)], indent=2)}

Return the indices of the {limit} most relevant queries (0-based), or fewer if there aren't enough similar queries.
Return ONLY the indices as a comma-separated list, e.g.: "0,3,5" or "2,4" or "1"
"""

            completion = self.client.chat.completions.create(
                model="nvidia/llama-3.1-nemotron-70b-instruct",
                messages=[{"role": "user", "content": context}],
                temperature=0.3,
                max_tokens=50,
                stream=False
            )
            
            indices_str = completion.choices[0].message.content.strip()
            try:
                indices = [int(idx.strip()) for idx in indices_str.split(',')]
                recent_queries = self.query_history.get_recent_queries(10)
                return [recent_queries[idx] for idx in indices if idx < len(recent_queries)]
            except:
                return []
                
        except Exception as e:
            print(f"Error finding similar queries: {e}")
            return []

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

    async def format_results_with_context(self, results: List[Dict], query_info: Dict) -> str:
        """Convert query results to natural language using the LLM with conversation context"""
        try:
            # Get similar previous queries
            similar_queries = await self.get_similar_queries(query_info.get('explanation', ''))
            
            # Get recent conversations
            recent_conversations = self.chat_history.get_recent_conversations(limit=3)
            
            # Create context from both queries and conversations
            context = ""
            
            # Add query context
            if similar_queries:
                context += "\nRelated previous queries:\n"
                for q in similar_queries:
                    context += f"Q: {q['question']}\n"
                    context += f"Database: {q['database']}\n"
                    context += f"Query: {q['query']}\n\n"
            
            # Add conversation context
            if recent_conversations:
                context += "\nRecent conversations:\n"
                for conv in recent_conversations:
                    context += f"Q: {conv['question']}\n"
                    if 'natural_language_summary' in conv['answer']:
                        context += f"A: {conv['answer']['natural_language_summary']}\n\n"
            
            # Create the main prompt
            prompt = f"""Summarize these agricultural data results directly and concisely:

Previous Context:
{context}

Current Data Context: {query_info.get('explanation', 'Agricultural market data')}

Raw Data:
{json.dumps(results, indent=2)}

Instructions:
- Start your response with the key findings directly
- Use simple, clear language
- Include specific numbers and dates
- Avoid phrases like "Based on the data" or "Here is a summary"
- If relevant, reference or compare with previous conversation data
- Keep it brief but informative"""
            
            completion = self.client.chat.completions.create(
                model="nvidia/llama-3.1-nemotron-70b-instruct",
                messages=[
                    {"role": "system", "content": "You are a direct and concise agricultural data analyst. Provide summaries without any introductory phrases."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.5,
                top_p=1,
                max_tokens=1024,
                stream=False
            )
            
            response = completion.choices[0].message.content.strip()
            
            # Remove common introductory phrases
            intro_phrases = [
                "Here is", "Based on", "According to", "The data shows",
                "The results indicate", "Analysis shows", "Let me summarize",
                "Here's a summary", "To summarize", "In summary"
            ]
            
            for phrase in intro_phrases:
                if response.lower().startswith(phrase.lower()):
                    response = response[len(phrase):].lstrip(',:. ')
            
            return response
            
        except Exception as e:
            print(f"Error formatting results with context: {e}")
            return "Unable to generate summary of results."

    async def get_answer(self, user_question: str) -> Dict[str, Any]:
        """Process user question and return formatted answer"""
        query_info = await self.generate_query(user_question)
        
        if query_info is None or "error" in query_info:
            return {"error": query_info.get("error", "Failed to generate query")}
        
        results = self.execute_query(query_info)
        if results is None:
            return {"error": "Failed to execute query"}
        
        # Handle multi-database results
        if isinstance(results, dict):
            combined_results = []
            for db_name, db_results in results.items():
                if not db_results.empty:
                    db_results['source_database'] = db_name
                    combined_results.append(db_results)
            
            if combined_results:
                results = pd.concat(combined_results, ignore_index=True)
            else:
                results = pd.DataFrame()
        
        results_list = results.to_dict('records')
        
        # Get natural language summary
        natural_language_summary = await self.format_results_with_context(
            results_list, 
            query_info
        )
        
        return {
            "question": user_question,
            "query_info": query_info,
            "results": results_list,
            "columns": results.columns.tolist() if not results.empty else [],
            "natural_language_summary": natural_language_summary,
            "timestamp": datetime.now().isoformat()
        }

# Example usage with async/await
import asyncio

async def main():
    try:
        print("\n🌾 Agricultural Data Assistant Initialized...")
        print("Type your question (or 'exit' to quit)\n")
        
        query_generator = NvidiaLLMQueryGenerator()
        
        while True:
            try:
                # Get user input
                question = input("\n❓ Question: ")
                
                # Check for exit command
                if question.lower() in ['exit', 'quit', 'q']:
                    print("\nThank you for using Agricultural Data Assistant! Goodbye! 👋\n")
                    break
                
                # Skip empty questions
                if not question.strip():
                    continue
                
                print("\n⏳ Processing...", flush=True)
                
                # Get and display answer
                answer = await query_generator.get_answer(question)
                
                if "error" in answer:
                    print(f"\n❌ Error: {answer['error']}")
                else:
                    print("\n📊 Answer:")
                    print(answer['natural_language_summary'])
                
                print("\n" + "─"*50)  # Separator line
                
            except Exception as e:
                print(f"\n❌ Error processing question: {e}")
                print("\n" + "─"*50)
                
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
    
    except KeyboardInterrupt:
        print("\n\nThank you for using Agricultural Data Assistant! Goodbye! 👋\n")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nProgram terminated by user. Goodbye! 👋\n")
