#!/usr/bin/env python3
"""
LAW Matrix v4.0 - Retrieval Augmented Generation (RAG) System
Contextual intelligence and observation for observant legal AI
"""

import os
import json
import numpy as np
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime
import sqlite3
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import faiss

@dataclass
class LegalDocument:
    """Represents a legal document in the knowledge base"""
    id: str
    title: str
    content: str
    document_type: str  # "case_law", "statute", "template", "memo"
    jurisdiction: str
    date_created: datetime
    tags: List[str]
    metadata: Dict[str, Any]

@dataclass
class UserContext:
    """Represents current user context and activity"""
    user_id: str
    current_case: str
    active_documents: List[str]
    recent_queries: List[str]
    user_preferences: Dict[str, Any]
    session_data: Dict[str, Any]

class LawMatrixRAGSystem:
    """
    Retrieval Augmented Generation system for LAW Matrix v4.0
    Provides contextual intelligence through real-time data retrieval
    """
    
    def __init__(self, db_path: str = "lawmatrix_knowledge.db"):
        self.db_path = db_path
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.index = None
        self.document_store = {}
        self.vector_dimension = 384
        
        # Initialize database and vector index
        self._initialize_database()
        self._build_vector_index()
        
    def _initialize_database(self):
        """Initialize SQLite database for document storage"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create documents table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS legal_documents (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                document_type TEXT NOT NULL,
                jurisdiction TEXT NOT NULL,
                date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                tags TEXT,
                metadata TEXT,
                embedding BLOB
            )
        ''')
        
        # Create user_context table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_context (
                user_id TEXT PRIMARY KEY,
                current_case TEXT,
                active_documents TEXT,
                recent_queries TEXT,
                user_preferences TEXT,
                session_data TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
        print("✅ LAW Matrix v4.0 - RAG Database initialized")
        
    def _build_vector_index(self):
        """Build FAISS vector index for semantic search"""
        
        self.index = faiss.IndexFlatIP(self.vector_dimension)  # Inner product for cosine similarity
        
        # Load existing documents and build index
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT id, content FROM legal_documents")
        
        documents = cursor.fetchall()
        if documents:
            embeddings = []
            doc_ids = []
            
            for doc_id, content in documents:
                embedding = self.embedding_model.encode(content)
                embeddings.append(embedding)
                doc_ids.append(doc_id)
                self.document_store[doc_id] = doc_id
                
            embeddings_array = np.array(embeddings).astype('float32')
            self.index.add(embeddings_array)
            
        conn.close()
        print(f"✅ LAW Matrix v4.0 - Vector index built with {len(documents)} documents")
        
    def add_document(self, document: LegalDocument):
        """Add a legal document to the knowledge base"""
        
        # Generate embedding
        embedding = self.embedding_model.encode(document.content)
        
        # Store in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO legal_documents 
            (id, title, content, document_type, jurisdiction, tags, metadata, embedding)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            document.id,
            document.title,
            document.content,
            document.document_type,
            document.jurisdiction,
            json.dumps(document.tags),
            json.dumps(document.metadata),
            embedding.tobytes()
        ))
        
        conn.commit()
        conn.close()
        
        # Update vector index
        embedding_array = np.array([embedding]).astype('float32')
        self.index.add(embedding_array)
        self.document_store[document.id] = document.id
        
        print(f"✅ LAW Matrix v4.0 - Document added: {document.title}")
        
    def retrieve_relevant_documents(self, query: str, user_context: UserContext, top_k: int = 5) -> List[Dict]:
        """
        Retrieve most relevant documents for a query with user context
        """
        
        # Enhance query with user context
        enhanced_query = self._enhance_query_with_context(query, user_context)
        
        # Generate query embedding
        query_embedding = self.embedding_model.encode(enhanced_query)
        query_array = np.array([query_embedding]).astype('float32')
        
        # Search vector index
        scores, indices = self.index.search(query_array, top_k)
        
        # Retrieve document details
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        relevant_docs = []
        for score, idx in zip(scores[0], indices[0]):
            if idx < len(self.document_store):
                doc_id = list(self.document_store.keys())[idx]
                cursor.execute("SELECT * FROM legal_documents WHERE id = ?", (doc_id,))
                doc_data = cursor.fetchone()
                
                if doc_data:
                    relevant_docs.append({
                        'id': doc_data[0],
                        'title': doc_data[1],
                        'content': doc_data[2],
                        'document_type': doc_data[3],
                        'jurisdiction': doc_data[4],
                        'tags': json.loads(doc_data[6]) if doc_data[6] else [],
                        'metadata': json.loads(doc_data[7]) if doc_data[7] else {},
                        'relevance_score': float(score)
                    })
        
        conn.close()
        
        # Filter and rank based on user context
        filtered_docs = self._filter_by_context(relevant_docs, user_context)
        
        return filtered_docs[:top_k]
        
    def _enhance_query_with_context(self, query: str, user_context: UserContext) -> str:
        """Enhance query with user context information"""
        
        context_parts = [query]
        
        # Add current case context
        if user_context.current_case:
            context_parts.append(f"Current case: {user_context.current_case}")
            
        # Add recent queries for continuity
        if user_context.recent_queries:
            recent_context = "Recent queries: " + "; ".join(user_context.recent_queries[-3:])
            context_parts.append(recent_context)
            
        # Add active documents context
        if user_context.active_documents:
            active_context = "Active documents: " + "; ".join(user_context.active_documents)
            context_parts.append(active_context)
            
        return " ".join(context_parts)
        
    def _filter_by_context(self, documents: List[Dict], user_context: UserContext) -> List[Dict]:
        """Filter documents based on user context preferences"""
        
        filtered_docs = []
        
        for doc in documents:
            # Priority for current case jurisdiction
            if user_context.current_case and "Utah" in doc.get('jurisdiction', ''):
                doc['relevance_score'] *= 1.2
                
            # Priority for document types user frequently uses
            if doc.get('document_type') in user_context.user_preferences.get('preferred_doc_types', []):
                doc['relevance_score'] *= 1.1
                
            # Filter out irrelevant jurisdictions
            if doc.get('jurisdiction') and doc.get('jurisdiction') not in ['Utah', 'Federal']:
                doc['relevance_score'] *= 0.8
                
            filtered_docs.append(doc)
            
        # Sort by relevance score
        return sorted(filtered_docs, key=lambda x: x['relevance_score'], reverse=True)
        
    def update_user_context(self, user_context: UserContext):
        """Update user context in database"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO user_context 
            (user_id, current_case, active_documents, recent_queries, user_preferences, session_data)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            user_context.user_id,
            user_context.current_case,
            json.dumps(user_context.active_documents),
            json.dumps(user_context.recent_queries),
            json.dumps(user_context.user_preferences),
            json.dumps(user_context.session_data)
        ))
        
        conn.commit()
        conn.close()
        
    def get_user_context(self, user_id: str) -> Optional[UserContext]:
        """Retrieve user context from database"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM user_context WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        
        conn.close()
        
        if result:
            return UserContext(
                user_id=result[0],
                current_case=result[1] or "",
                active_documents=json.loads(result[2]) if result[2] else [],
                recent_queries=json.loads(result[3]) if result[3] else [],
                user_preferences=json.loads(result[4]) if result[4] else {},
                session_data=json.loads(result[5]) if result[5] else {}
            )
        return None

class LawMatrixContextualAI:
    """
    Main contextual AI orchestrator that combines RAG with user observation
    """
    
    def __init__(self):
        self.rag_system = LawMatrixRAGSystem()
        
    def process_query_with_context(self, query: str, user_id: str) -> Dict[str, Any]:
        """
        Process user query with full contextual awareness
        """
        
        # Get user context
        user_context = self.rag_system.get_user_context(user_id)
        if not user_context:
            user_context = UserContext(
                user_id=user_id,
                current_case="",
                active_documents=[],
                recent_queries=[],
                user_preferences={},
                session_data={}
            )
            
        # Update recent queries
        user_context.recent_queries.append(query)
        if len(user_context.recent_queries) > 10:
            user_context.recent_queries = user_context.recent_queries[-10:]
            
        # Retrieve relevant documents
        relevant_docs = self.rag_system.retrieve_relevant_documents(query, user_context)
        
        # Update user context
        self.rag_system.update_user_context(user_context)
        
        return {
            'query': query,
            'user_context': user_context,
            'relevant_documents': relevant_docs,
            'contextual_prompt': self._build_contextual_prompt(query, user_context, relevant_docs)
        }
        
    def _build_contextual_prompt(self, query: str, user_context: UserContext, relevant_docs: List[Dict]) -> str:
        """Build contextual prompt with retrieved information"""
        
        prompt_parts = [
            "LAW Matrix v4.0 Bulletproof Enterprise Edition - Contextual AI Response",
            f"User ID: {user_context.user_id}",
            f"Current Case: {user_context.current_case}",
            "",
            "RELEVANT LEGAL INFORMATION:"
        ]
        
        for i, doc in enumerate(relevant_docs[:3], 1):
            prompt_parts.extend([
                f"{i}. {doc['title']} ({doc['document_type']})",
                f"   Jurisdiction: {doc['jurisdiction']}",
                f"   Content: {doc['content'][:500]}...",
                f"   Relevance Score: {doc['relevance_score']:.3f}",
                ""
            ])
            
        prompt_parts.extend([
            "USER QUERY:",
            query,
            "",
            "INSTRUCTIONS:",
            "Provide a comprehensive legal analysis using the retrieved information above.",
            "Consider the user's current case context and provide actionable legal guidance.",
            "Cite specific legal sources and provide practical next steps."
        ])
        
        return "\n".join(prompt_parts)

if __name__ == "__main__":
    # Initialize RAG system
    rag_system = LawMatrixRAGSystem()
    
    # Add sample legal document
    sample_doc = LegalDocument(
        id="utah_custody_law",
        title="Utah Child Custody Guidelines",
        content="Utah Code § 30-3-10 establishes the factors for determining child custody in divorce proceedings. The court must consider the best interests of the child, including but not limited to: the child's relationship with each parent, each parent's ability to provide for the child's physical and emotional needs, the child's adjustment to home, school, and community, and any history of domestic violence.",
        document_type="statute",
        jurisdiction="Utah",
        date_created=datetime.now(),
        tags=["custody", "family_law", "best_interests"],
        metadata={"section": "30-3-10", "title": "30"}
    )
    
    rag_system.add_document(sample_doc)
    
    # Test contextual AI
    contextual_ai = LawMatrixContextualAI()
    result = contextual_ai.process_query_with_context(
        "What factors should I consider for child custody in my Utah divorce case?",
        "user_123"
    )
    
    print("✅ LAW Matrix v4.0 - RAG System operational!")
    print(f"Retrieved {len(result['relevant_documents'])} relevant documents")
    print(f"Contextual prompt length: {len(result['contextual_prompt'])} characters")
