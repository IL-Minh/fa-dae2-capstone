"""
Tools for the Smart Hybrid Agent
"""

import logging
from typing import Any, Dict

from openai import OpenAI


class AgentTools:
    """Collection of tools for the agent"""

    def __init__(self, snowflake_connector, pinecone_connector, config: Dict[str, str]):
        """Initialize agent tools"""
        self.snowflake = snowflake_connector
        self.pinecone = pinecone_connector
        self.config = config
        self.openai_client = OpenAI(api_key=config["openai_api_key"])
        self.logger = logging.getLogger(__name__)

    def query_snowflake_users(self, query: str, limit: int = 10) -> Dict[str, Any]:
        """Query user data from Snowflake based on natural language"""
        try:
            # Simple keyword-based routing for now
            if any(
                word in query.lower()
                for word in ["user", "users", "customer", "customers", "registration"]
            ):
                results = self.snowflake.get_user_data(limit=limit)
                if results:
                    return {
                        "success": True,
                        "tool": "snowflake_users",
                        "data": results,
                        "summary": f"Retrieved {len(results)} user records from Snowflake",
                    }
                else:
                    return {
                        "success": False,
                        "tool": "snowflake_users",
                        "error": "No user data found",
                    }
            else:
                return {
                    "success": False,
                    "tool": "snowflake_users",
                    "error": "Query not related to user data",
                }

        except Exception as e:
            self.logger.error(f"Error querying Snowflake users: {e}")
            return {"success": False, "tool": "snowflake_users", "error": str(e)}

    def query_snowflake_transactions(
        self, query: str, limit: int = 10
    ) -> Dict[str, Any]:
        """Query transaction data from Snowflake based on natural language"""
        try:
            # Simple keyword-based routing for now
            if any(
                word in query.lower()
                for word in [
                    "transaction",
                    "transactions",
                    "payment",
                    "payments",
                    "amount",
                    "merchant",
                ]
            ):
                results = self.snowflake.get_transaction_data(limit=limit)
                if results:
                    return {
                        "success": True,
                        "tool": "snowflake_transactions",
                        "data": results,
                        "summary": f"Retrieved {len(results)} transaction records from Snowflake",
                    }
                else:
                    return {
                        "success": False,
                        "tool": "snowflake_transactions",
                        "error": "No transaction data found",
                    }
            else:
                return {
                    "success": False,
                    "tool": "snowflake_transactions",
                    "error": "Query not related to transaction data",
                }

        except Exception as e:
            self.logger.error(f"Error querying Snowflake transactions: {e}")
            return {"success": False, "tool": "snowflake_transactions", "error": str(e)}

    def search_pinecone_documents(self, query: str, top_k: int = 5) -> Dict[str, Any]:
        """Search PDF documents in Pinecone based on query"""
        try:
            results = self.pinecone.search_documents(query, top_k=top_k)
            if results:
                return {
                    "success": True,
                    "tool": "pinecone_search",
                    "data": results,
                    "summary": f"Found {len(results)} relevant documents in Pinecone",
                }
            else:
                return {
                    "success": False,
                    "tool": "pinecone_search",
                    "error": "No relevant documents found",
                }

        except Exception as e:
            self.logger.error(f"Error searching Pinecone: {e}")
            return {"success": False, "tool": "pinecone_search", "error": str(e)}

    def get_snowflake_summary(self, query: str) -> Dict[str, Any]:
        """Get summary statistics from Snowflake based on query"""
        try:
            if any(
                word in query.lower()
                for word in ["summary", "statistics", "stats", "overview", "total"]
            ):
                if any(
                    word in query.lower()
                    for word in ["user", "users", "customer", "customers"]
                ):
                    results = self.snowflake.get_user_summary()
                    tool_name = "snowflake_user_summary"
                elif any(
                    word in query.lower()
                    for word in ["transaction", "transactions", "payment", "payments"]
                ):
                    results = self.snowflake.get_transaction_summary()
                    tool_name = "snowflake_transaction_summary"
                else:
                    # Default to transaction summary
                    results = self.snowflake.get_transaction_summary()
                    tool_name = "snowflake_transaction_summary"

                if results:
                    return {
                        "success": True,
                        "tool": tool_name,
                        "data": results,
                        "summary": "Retrieved summary statistics from Snowflake",
                    }
                else:
                    return {
                        "success": False,
                        "tool": tool_name,
                        "error": "No summary data found",
                    }
            else:
                return {
                    "success": False,
                    "tool": "snowflake_summary",
                    "error": "Query not requesting summary information",
                }

        except Exception as e:
            self.logger.error(f"Error getting Snowflake summary: {e}")
            return {"success": False, "tool": "snowflake_summary", "error": str(e)}

    def synthesize_memory(
        self, query: str, memory_contexts: Dict[str, str]
    ) -> Dict[str, Any]:
        """Synthesize information from memory contexts to answer complex queries"""
        try:
            # Prepare context for LLM
            context_parts = []
            if memory_contexts.get("pdf_context"):
                context_parts.append(
                    f"PDF Information: {memory_contexts['pdf_context']}"
                )
            if memory_contexts.get("user_data_context"):
                context_parts.append(
                    f"User Data: {memory_contexts['user_data_context']}"
                )
            if memory_contexts.get("transaction_context"):
                context_parts.append(
                    f"Transaction Data: {memory_contexts['transaction_context']}"
                )

            if not context_parts:
                return {
                    "success": False,
                    "tool": "memory_synthesis",
                    "error": "No memory contexts available for synthesis",
                }

            # Create prompt for synthesis
            prompt = f"""
            Based on the following information from different sources, please provide a coherent answer to the user's question.

            User Question: {query}

            Available Information:
            {"\n\n".join(context_parts)}

            Please synthesize this information to provide a comprehensive and coherent answer that connects insights from all sources.
            """

            # Get LLM response
            response = self.openai_client.chat.completions.create(
                model=self.config.get("openai_model", "gpt-4"),
                messages=[
                    {
                        "role": "system",
                        "content": "You are an AI assistant that synthesizes information from multiple sources to provide coherent, insightful answers.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=1000,
                temperature=0.1,
            )

            synthesis = response.choices[0].message.content

            return {
                "success": True,
                "tool": "memory_synthesis",
                "data": {
                    "synthesis": synthesis,
                    "sources_used": list(memory_contexts.keys()),
                },
                "summary": f"Synthesized information from {len(memory_contexts)} memory contexts",
            }

        except Exception as e:
            self.logger.error(f"Error synthesizing memory: {e}")
            return {"success": False, "tool": "memory_synthesis", "error": str(e)}

    def route_query(self, query: str) -> str:
        """Determine which tool(s) to use based on the query content"""
        query_lower = query.lower()

        # Check for hybrid queries that need multiple sources
        hybrid_indicators = [
            "compare",
            "relationship",
            "correlation",
            "how does",
            "connect",
            "synthesis",
        ]
        if any(indicator in query_lower for indicator in hybrid_indicators):
            return "hybrid"

        # Check for PDF/document queries
        pdf_indicators = [
            "policy",
            "procedure",
            "document",
            "pdf",
            "file",
            "text",
            "content",
        ]
        if any(indicator in query_lower for indicator in pdf_indicators):
            return "pinecone"

        # Check for Snowflake data queries
        snowflake_indicators = [
            "user",
            "transaction",
            "data",
            "table",
            "database",
            "snowflake",
        ]
        if any(indicator in query_lower for indicator in snowflake_indicators):
            return "snowflake"

        # Default to general
        return "general"

    def execute_tool(self, tool_name: str, query: str, **kwargs) -> Dict[str, Any]:
        """Execute a specific tool based on name"""
        try:
            if tool_name == "snowflake_users":
                return self.query_snowflake_users(query, **kwargs)
            elif tool_name == "snowflake_transactions":
                return self.query_snowflake_transactions(query, **kwargs)
            elif tool_name == "pinecone_search":
                return self.search_pinecone_documents(query, **kwargs)
            elif tool_name == "snowflake_summary":
                return self.get_snowflake_summary(query)
            elif tool_name == "memory_synthesis":
                return self.synthesize_memory(query, kwargs.get("memory_contexts", {}))
            else:
                return {
                    "success": False,
                    "tool": tool_name,
                    "error": f"Unknown tool: {tool_name}",
                }

        except Exception as e:
            self.logger.error(f"Error executing tool {tool_name}: {e}")
            return {"success": False, "tool": tool_name, "error": str(e)}
