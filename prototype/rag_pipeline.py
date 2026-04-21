"""
rag_pipeline.py - RAG Pipeline Architecture for ArduPilot Log Diagnosis

Uses ChromaDB (local embedded vector DB via SQLite) to store ArduPilot
Wiki extracts offline. At runtime, it queries the DB for context based
on the identified root cause.
"""

import functools
import chromadb

class ArduPilotRAG:
    def __init__(self, db_path="./ardupilot_knowledge_base"):
        self.chroma_client = chromadb.PersistentClient(path=db_path)
        self.collection = self.chroma_client.get_collection(name="ardupilot_docs")
        self.disabled = False

    def retrieve_context(self, diagnosis_event: dict) -> str:
        query_text = f"Failure: {diagnosis_event.get('name', '')}. Details: {diagnosis_event.get('description', '')}"
        
        results = self.collection.query(
            query_texts=[query_text],
            n_results=1
        )
        
        if not results['documents'] or not results['documents'][0]:
            return "No relevant documentation found."
            
        return "\n---\n".join(results['documents'][0])

    def generate_fix_suggestion(self, diagnosis_event: dict, llm_client=None) -> str:
        wiki_context = self.retrieve_context(diagnosis_event)
        
        prompt = f"""
        You are an ArduPilot Core Developer. A drone has crashed or failed.
        
        Detected Root Cause: {diagnosis_event.get('name', '')}
        Event Details: {diagnosis_event.get('description', '')}
        
        Relevant ArduPilot Documentation Context:
        {wiki_context}
        
        Based ONLY on the documentation context provided, what is the recommended fix to prevent this issue in the future?
        """
        
        return f"Simulated LLM Response based on root cause: {diagnosis_event.get('name', '')}."
