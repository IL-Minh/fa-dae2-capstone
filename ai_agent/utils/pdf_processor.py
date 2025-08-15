"""
PDF processing utility for chunking and uploading to Pinecone
"""

import hashlib
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import pinecone
import PyPDF2


class PDFProcessor:
    """Utility for processing PDFs and uploading to Pinecone"""

    def __init__(self, config: Dict[str, str]):
        """Initialize PDF processor"""
        self.config = config
        self.openai_client = None
        self.logger = logging.getLogger(__name__)

        # OpenAI client not needed for Pinecone embeddings
        self.openai_client = None

        # Initialize Pinecone
        try:
            self.pinecone_client = pinecone.Pinecone(api_key=config["pinecone_api_key"])
            self.logger.info("Successfully initialized Pinecone for PDF processing")
        except Exception as e:
            self.logger.error(f"Failed to initialize Pinecone: {e}")

    def extract_text_from_pdf(self, pdf_path: str) -> Optional[str]:
        """Extract text content from a PDF file"""
        try:
            with open(pdf_path, "rb") as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""

                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text += page.extract_text() + "\n"

                self.logger.info(f"Successfully extracted text from {pdf_path}")
                return text

        except Exception as e:
            self.logger.error(f"Failed to extract text from {pdf_path}: {e}")
            return None

    def chunk_text(
        self, text: str, chunk_size: int = 1000, overlap: int = 200
    ) -> List[str]:
        """Split text into overlapping chunks"""
        if len(text) <= chunk_size:
            return [text]

        chunks = []
        start = 0

        while start < len(text):
            end = start + chunk_size

            # If this isn't the last chunk, try to break at a sentence boundary
            if end < len(text):
                # Look for sentence endings near the chunk boundary
                for i in range(end, max(start + chunk_size - 100, start), -1):
                    if text[i] in ".!?":
                        end = i + 1
                        break

            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)

            start = end - overlap
            if start >= len(text):
                break

        self.logger.info(f"Created {len(chunks)} chunks from text")
        return chunks

    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """Generate embedding for text using Pinecone's llama-text-embed-v2"""
        try:
            # Use Pinecone's built-in embedding model
            response = self.pinecone_client.inference.embed(
                model="llama-text-embed-v2",
                inputs=[text],
                parameters={"input_type": "passage"},
            )
            return response.data[0].values

        except Exception as e:
            self.logger.error(f"Failed to generate embedding: {e}")
            return None

    def upload_chunks_to_pinecone(
        self, chunks: List[str], metadata: Dict[str, Any], index_name: str
    ) -> bool:
        """Upload text chunks to Pinecone with metadata"""
        try:
            # Import ServerlessSpec at the function level
            from pinecone import ServerlessSpec

            # Get or create index
            if index_name not in self.pinecone_client.list_indexes().names():
                self.logger.info(f"Index {index_name} not found, creating new index...")
                try:
                    # Create index with settings for llama-text-embed-v2 (1024 dimensions)
                    # Using serverless spec for easier setup
                    self.pinecone_client.create_index(
                        name=index_name,
                        dimension=1024,
                        metric="cosine",
                        spec=ServerlessSpec(cloud="aws", region="us-east-1"),
                    )
                    self.logger.info(f"Successfully created index: {index_name}")
                    # Wait a moment for index to be ready
                    import time

                    time.sleep(10)
                except Exception as e:
                    self.logger.error(f"Failed to create index {index_name}: {e}")
                    return False
            else:
                # Check if existing index has correct dimensions
                try:
                    index_info = self.pinecone_client.describe_index(index_name)
                    if index_info.dimension != 1024:
                        self.logger.info(
                            f"Existing index {index_name} has wrong dimensions ({index_info.dimension}), deleting and recreating..."
                        )
                        self.pinecone_client.delete_index(index_name)
                        # Wait for deletion to complete
                        import time

                        time.sleep(10)

                        # Recreate with correct dimensions
                        self.pinecone_client.create_index(
                            name=index_name,
                            dimension=1024,
                            metric="cosine",
                            spec=ServerlessSpec(cloud="aws", region="us-east-1"),
                        )
                        self.logger.info(f"Successfully recreated index: {index_name}")
                        time.sleep(10)
                except Exception as e:
                    self.logger.error(
                        f"Failed to check/recreate index {index_name}: {e}"
                    )
                    return False

            index = self.pinecone_client.Index(index_name)

            # Prepare vectors for upload
            vectors = []
            for i, chunk in enumerate(chunks):
                # Generate embedding
                embedding = self.generate_embedding(chunk)
                if not embedding:
                    continue

                # Create unique ID for the chunk
                chunk_id = f"{metadata.get('filename', 'unknown')}_{i}_{hashlib.md5(chunk.encode()).hexdigest()[:8]}"

                # Prepare metadata for this chunk
                chunk_metadata = metadata.copy()
                chunk_metadata.update(
                    {
                        "chunk_index": i,
                        "total_chunks": len(chunks),
                        "chunk_size": len(chunk),
                        "text_preview": chunk[:200] + "..."
                        if len(chunk) > 200
                        else chunk,
                    }
                )

                vectors.append(
                    {"id": chunk_id, "values": embedding, "metadata": chunk_metadata}
                )

            if vectors:
                # Upload in batches
                batch_size = 100
                for i in range(0, len(vectors), batch_size):
                    batch = vectors[i : i + batch_size]
                    index.upsert(vectors=batch)

                self.logger.info(
                    f"Successfully uploaded {len(vectors)} chunks to Pinecone"
                )
                return True
            else:
                self.logger.warning("No valid chunks to upload")
                return False

        except Exception as e:
            self.logger.error(f"Failed to upload chunks to Pinecone: {e}")
            return False

    def process_pdf(
        self, pdf_path: str, index_name: str, chunk_size: int = 1000, overlap: int = 200
    ) -> bool:
        """Complete PDF processing pipeline: extract, chunk, and upload"""
        try:
            # Validate file exists
            if not os.path.exists(pdf_path):
                self.logger.error(f"PDF file not found: {pdf_path}")
                return False

            # Extract text
            text = self.extract_text_from_pdf(pdf_path)
            if not text:
                return False

            # Create chunks
            chunks = self.chunk_text(text, chunk_size, overlap)
            if not chunks:
                self.logger.warning("No chunks created from text")
                return False

            # Prepare metadata
            metadata = {
                "filename": os.path.basename(pdf_path),
                "file_path": pdf_path,
                "file_size": os.path.getsize(pdf_path),
                "total_chunks": len(chunks),
                "chunk_size": chunk_size,
                "overlap": overlap,
                "processing_timestamp": str(datetime.now()),
            }

            # Upload to Pinecone
            success = self.upload_chunks_to_pinecone(chunks, metadata, index_name)

            if success:
                self.logger.info(f"Successfully processed PDF: {pdf_path}")
                return True
            else:
                self.logger.error(
                    f"Failed to upload PDF chunks to Pinecone: {pdf_path}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Failed to process PDF {pdf_path}: {e}")
            return False

    def get_processing_stats(self, index_name: str) -> Optional[Dict[str, Any]]:
        """Get statistics about processed PDFs in Pinecone"""
        try:
            if index_name not in pinecone.list_indexes():
                return None

            index = pinecone.Index(index_name)
            stats = index.describe_index_stats()

            # Analyze metadata to get PDF processing stats
            pdf_stats = {
                "total_vectors": stats.total_vector_count,
                "unique_pdfs": 0,
                "total_chunks": 0,
                "pdfs": {},
            }

            # Get sample vectors to analyze metadata
            sample_query = index.query(
                vector=[0.0] * 1536,  # Dummy vector
                top_k=1000,
                include_metadata=True,
            )

            for match in sample_query.matches:
                metadata = match.metadata
                filename = metadata.get("filename", "unknown")

                if filename not in pdf_stats["pdfs"]:
                    pdf_stats["pdfs"][filename] = {
                        "chunks": 0,
                        "file_size": metadata.get("file_size", 0),
                        "chunk_size": metadata.get("chunk_size", 0),
                        "overlap": metadata.get("overlap", 0),
                    }

                pdf_stats["pdfs"][filename]["chunks"] += 1
                pdf_stats["total_chunks"] += 1

            pdf_stats["unique_pdfs"] = len(pdf_stats["pdfs"])

            return pdf_stats

        except Exception as e:
            self.logger.error(f"Failed to get processing stats: {e}")
            return None
