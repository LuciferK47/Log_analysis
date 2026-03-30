"""
ingest_kb.py - Offline Script to vectorize the ArduPilot knowledge base
into ChromaDB for the RAG pipeline.
"""
import os

try:
    import chromadb
    from chromadb.utils import embedding_functions
except ImportError:
    print("Please install chromadb: pip install chromadb")
    exit(1)

def chunk_text(text, chunk_size=300, overlap=50):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size - overlap):
        chunks.append(" ".join(words[i:i+chunk_size]))
    return chunks

def main():
    kb_dir = "./knowledge_base"
    db_path = "./ardupilot_knowledge_base"

    os.makedirs(kb_dir, exist_ok=True)
    
    # Create sample docs if directory is empty
    if not os.listdir(kb_dir):
        with open(os.path.join(kb_dir, "sample.txt"), "w") as f:
            f.write("Primary Power Collapse is often caused by a bad battery or overdrawing current. Check BATT.Volt logs.\n")
            f.write("EKF Primary Core Divergence happens when GPS or compass data is heavily corrupted or excessive vibration is present.\n")

    client = chromadb.PersistentClient(path=db_path)
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    collection = client.get_or_create_collection(
        name="ardupilot_wiki",
        embedding_function=ef
    )

    docs, ids, metadatas = [], [], []
    doc_id = 0

    for filename in os.listdir(kb_dir):
        if not filename.endswith(".txt"): 
            continue
            
        filepath = os.path.join(kb_dir, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            text = f.read()

        for chunk in chunk_text(text):
            if not chunk.strip(): continue
            docs.append(chunk)
            ids.append(f"doc_{doc_id}")
            metadatas.append({"source": filename})
            doc_id += 1

    if docs:
        print(f"Ingesting {len(docs)} chunks...")
        collection.add(
            documents=docs,
            ids=ids,
            metadatas=metadatas
        )
        print(f"Successfully ingested {len(docs)} chunks from {kb_dir} into ChromaDB at {db_path}.")
    else:
        print("No documents found to ingest.")

if __name__ == "__main__":
    main()
