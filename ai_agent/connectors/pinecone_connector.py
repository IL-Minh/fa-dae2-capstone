"""
Pinecone connector for PDF document retrieval
"""

import logging
from typing import Any, Dict, List, Optional

import pinecone
from openai import OpenAI


class PineconeConnector:
    """Connector for Pinecone vector database operations"""

    def __init__(self, config: Dict[str, str]):
        """Initialize Pinecone connector"""
        self.config = config
        self.index = None
        self.openai_client = None
        self.logger = logging.getLogger(__name__)

        # Initialize OpenAI client for embeddings
        if config.get("openai_api_key"):
            self.openai_client = OpenAI(api_key=config["openai_api_key"])

        # Initialize Pinecone (2024+ - New API)
        try:
            self.pinecone_client = pinecone.Pinecone(api_key=config["api_key"])
            self.logger.info("Successfully initialized Pinecone")
        except Exception as e:
            self.logger.error(f"Failed to initialize Pinecone: {e}")

    def connect(self) -> bool:
        """Connect to Pinecone index"""
        try:
            # Import ServerlessSpec at the function level
            from pinecone import ServerlessSpec

            if (
                self.config["index_name"]
                not in self.pinecone_client.list_indexes().names()
            ):
                self.logger.info(
                    f"Index {self.config['index_name']} not found, creating new index..."
                )
                try:
                    # Create index with settings for llama-text-embed-v2 (1024 dimensions)
                    # Using serverless spec for easier setup
                    self.pinecone_client.create_index(
                        name=self.config["index_name"],
                        dimension=1024,
                        metric="cosine",
                        spec=ServerlessSpec(cloud="aws", region="us-east-1"),
                    )
                    self.logger.info(
                        f"Successfully created index: {self.config['index_name']}"
                    )
                    # Wait a moment for index to be ready
                    import time

                    time.sleep(10)
                except Exception as e:
                    self.logger.error(
                        f"Failed to create index {self.config['index_name']}: {e}"
                    )
                    return False

            self.index = self.pinecone_client.Index(self.config["index_name"])
            self.logger.info(
                f"Successfully connected to Pinecone index: {self.config['index_name']}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to Pinecone index: {e}")
            return False

    def search_documents(
        self,
        query: str,
        top_k: int = 5,
        filter_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """Search for documents using vector similarity"""
        if not self.index:
            if not self.connect():
                return None

        try:
            # Generate embedding for the query
            if not self.openai_client:
                self.logger.error("OpenAI client not initialized for embeddings")
                return None

            # Use Pinecone's built-in embedding model
            response = self.pinecone_client.inference.embed(
                model="llama-text-embed-v2",
                inputs=[query],
                parameters={"input_type": "query"},
            )
            query_embedding = response.data[0].values

            # Search Pinecone
            search_results = self.index.query(
                vector=query_embedding,
                top_k=top_k,
                include_metadata=True,
                filter=filter_metadata,
            )

            # Format results
            results = []
            for match in search_results.matches:
                results.append(
                    {"id": match.id, "score": match.score, "metadata": match.metadata}
                )

            return results

        except Exception as e:
            self.logger.error(f"Failed to search documents: {e}")
            return None

    def get_document_by_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific document by ID"""
        if not self.index:
            if not self.connect():
                return None

        try:
            fetch_results = self.index.fetch(ids=[document_id])

            if document_id in fetch_results.vectors:
                vector_data = fetch_results.vectors[document_id]
                return {"id": vector_data.id, "metadata": vector_data.metadata}
            else:
                self.logger.warning(f"Document {document_id} not found")
                return None

        except Exception as e:
            self.logger.error(f"Failed to fetch document {document_id}: {e}")
            return None

    def get_index_stats(self) -> Optional[Dict[str, Any]]:
        """Get index statistics"""
        if not self.index:
            if not self.connect():
                return None

        try:
            stats = self.index.describe_index_stats()
            return {
                "total_vector_count": stats.total_vector_count,
                "dimension": stats.dimension,
                "index_fullness": stats.index_fullness,
                "namespaces": stats.namespaces,
            }

        except Exception as e:
            self.logger.error(f"Failed to get index stats: {e}")
            return None

    def search_by_metadata(
        self, metadata_filter: Dict[str, Any], top_k: int = 10
    ) -> Optional[List[Dict[str, Any]]]:
        """Search documents by metadata filters"""
        if not self.index:
            if not self.connect():
                return None

        try:
            # Use a dummy vector (all zeros) for metadata-only search
            dummy_vector = [0.0] * 1536  # OpenAI ada-002 embedding dimension

            search_results = self.index.query(
                vector=dummy_vector,
                top_k=top_k,
                include_metadata=True,
                filter=metadata_filter,
            )

            # Format results
            results = []
            for match in search_results.matches:
                results.append(
                    {"id": match.id, "score": match.score, "metadata": match.metadata}
                )

            return results

        except Exception as e:
            self.logger.error(f"Failed to search by metadata: {e}")
            return None

    def test_connection(self) -> bool:
        """Test the Pinecone connection"""
        try:
            if not self.index:
                return self.connect()

            # Try to get index stats
            stats = self.get_index_stats()
            return stats is not None

        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
