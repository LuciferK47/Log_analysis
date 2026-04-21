"""
ingest_kb.py - Script to vectorize the ArduPilot knowledge base
into ChromaDB for the RAG pipeline.
"""
import os
import chromadb

def main():
    db_path = "./ardupilot_knowledge_base"
    os.makedirs(db_path, exist_ok=True)

    print("Initializing ChromaDB PersistentClient...")
    client = chromadb.PersistentClient(path=db_path)
    
    # We use the default embedding function to avoid heavy PyTorch dependencies.
    print("Getting or creating collection 'ardupilot_docs'...")
    collection = client.get_or_create_collection(name="ardupilot_docs")

    docs = [
        "When an ESC fails or a motor desyncs, ArduPilot will push the corresponding RCOU channel to its maximum limit (e.g., 1900+ PWM) to compensate, but physical orientation (ATT.Roll/Pitch) will continue to diverge. Check ESC wiring and motor bells.",
        "A sudden drop in BATT.Volt accompanied by an immediate loss of altitude indicates a power brownout. Check the primary power rail.",
        "If GPS.HDop spikes while GPS.NSats drops, the EKF will begin to reject the position data. Check for RF interference."
    ]
    ids = ["doc_1", "doc_2", "doc_3"]
    metadatas = [
        {"source": "Motor/ESC Failure"},
        {"source": "Power Collapse"},
        {"source": "GPS Glitch"}
    ]

    print(f"Ingesting {len(docs)} documents...")
    collection.add(
        documents=docs,
        ids=ids,
        metadatas=metadatas
    )
    print(f"Successfully ingested {len(docs)} documents into ChromaDB at {db_path}.")

if __name__ == "__main__":
    main()
