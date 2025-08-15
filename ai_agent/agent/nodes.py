"""
LangGraph nodes for the Smart Hybrid Agent
"""

import logging
from datetime import datetime
from typing import Any, Dict

from openai import OpenAI


class AgentNodes:
    """Collection of LangGraph nodes for the agent"""

    def __init__(self, tools, config: Dict[str, str]):
        """Initialize agent nodes"""
        self.tools = tools
        self.config = config
        self.openai_client = OpenAI(api_key=config["openai_api_key"])
        self.logger = logging.getLogger(__name__)

    def input_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Process user input and initialize conversation"""
        try:
            # Add user message to conversation history
            state["conversation_history"].append(
                {
                    "role": "user",
                    "content": state["user_message"],
                    "timestamp": str(datetime.now()),
                }
            )

            # Route the query to determine which tools to use
            route = self.tools.route_query(state["user_message"])
            state["current_context"] = route

            self.logger.info(f"Query routed to: {route}")

            return state

        except Exception as e:
            self.logger.error(f"Error in input node: {e}")
            state["llm_response"] = f"Error processing input: {str(e)}"
            return state

    def context_router_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Route to appropriate processing based on context"""
        try:
            context = state["current_context"]

            if context == "pinecone":
                # Route to Pinecone processing
                return self.pinecone_node(state)
            elif context == "snowflake":
                # Route to Snowflake processing
                return self.snowflake_node(state)
            elif context == "hybrid":
                # Route to hybrid processing
                return self.hybrid_node(state)
            else:
                # General conversation
                return self.general_node(state)

        except Exception as e:
            self.logger.error(f"Error in context router node: {e}")
            state["llm_response"] = f"Error routing context: {str(e)}"
            return state

    def pinecone_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Process Pinecone document search queries"""
        try:
            query = state["user_message"]

            # Search Pinecone
            result = self.tools.search_pinecone_documents(query, top_k=5)

            if result["success"]:
                # Store PDF context in memory
                pdf_info = []
                for doc in result["data"]:
                    metadata = doc.get("metadata", {})
                    text_preview = metadata.get("text_preview", "No preview available")
                    filename = metadata.get("filename", "Unknown file")
                    pdf_info.append(f"File: {filename}\nContent: {text_preview}")

                state["pdf_context"] = "\n\n".join(pdf_info)

                # Generate response using LLM
                response = self._generate_pinecone_response(query, result["data"])
                state["llm_response"] = response

                # Add tool result
                state["tool_results"].append(
                    {
                        "tool_name": "pinecone_search",
                        "success": True,
                        "data": result["summary"],
                    }
                )

            else:
                state["llm_response"] = (
                    f"Sorry, I couldn't find relevant documents. {result.get('error', '')}"
                )
                state["tool_results"].append(
                    {
                        "tool_name": "pinecone_search",
                        "success": False,
                        "error": result.get("error", "Unknown error"),
                    }
                )

            return state

        except Exception as e:
            self.logger.error(f"Error in Pinecone node: {e}")
            state["llm_response"] = f"Error searching documents: {str(e)}"
            return state

    def snowflake_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Process Snowflake data queries"""
        try:
            query = state["user_message"]

            # Determine what type of Snowflake data to query
            if any(
                word in query.lower()
                for word in ["user", "users", "customer", "customers"]
            ):
                result = self.tools.query_snowflake_users(query, limit=10)
                data_type = "user_data"
            elif any(
                word in query.lower()
                for word in ["transaction", "transactions", "payment", "payments"]
            ):
                result = self.tools.query_snowflake_transactions(query, limit=10)
                data_type = "transaction_data"
            else:
                # Default to transaction data
                result = self.tools.query_snowflake_transactions(query, limit=10)
                data_type = "transaction_data"

            if result["success"]:
                # Store data context in memory
                if data_type == "user_data":
                    state["user_data_context"] = str(result["data"])
                else:
                    state["transaction_context"] = str(result["data"])

                # Generate response using LLM
                response = self._generate_snowflake_response(
                    query, result["data"], data_type
                )
                state["llm_response"] = response

                # Add tool result
                state["tool_results"].append(
                    {
                        "tool_name": f"snowflake_{data_type}",
                        "success": True,
                        "data": result["summary"],
                    }
                )

            else:
                state["llm_response"] = (
                    f"Sorry, I couldn't retrieve the requested data. {result.get('error', '')}"
                )
                state["tool_results"].append(
                    {
                        "tool_name": f"snowflake_{data_type}",
                        "error": result.get("error", "Unknown error"),
                    }
                )

            return state

        except Exception as e:
            self.logger.error(f"Error in Snowflake node: {e}")
            state["llm_response"] = f"Error querying data: {str(e)}"
            return state

    def hybrid_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Process hybrid queries that need multiple sources"""
        try:
            query = state["user_message"]

            # Check if we have memory contexts to synthesize
            memory_contexts = {}
            if state.get("pdf_context"):
                memory_contexts["pdf_context"] = state["pdf_context"]
            if state.get("user_data_context"):
                memory_contexts["user_data_context"] = state["user_data_context"]
            if state.get("transaction_context"):
                memory_contexts["transaction_context"] = state["transaction_context"]

            if len(memory_contexts) >= 2:
                # Synthesize from memory
                result = self.tools.synthesize_memory(query, memory_contexts)

                if result["success"]:
                    state["combined_context"] = result["data"]["synthesis"]
                    state["llm_response"] = result["data"]["synthesis"]

                    # Add tool result
                    state["tool_results"].append(
                        {
                            "tool_name": "memory_synthesis",
                            "success": True,
                            "data": result["summary"],
                        }
                    )
                else:
                    state["llm_response"] = (
                        f"Sorry, I couldn't synthesize the information. {result.get('error', '')}"
                    )

            else:
                # Not enough context, ask user to provide more information
                state["llm_response"] = (
                    "I need more context to answer this question. Please ask me about specific data or documents first, then I can help you connect the information."
                )

            return state

        except Exception as e:
            self.logger.error(f"Error in hybrid node: {e}")
            state["llm_response"] = f"Error processing hybrid query: {str(e)}"
            return state

    def general_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Process general conversation queries"""
        try:
            query = state["user_message"]

            # Generate general response using LLM
            response = self.openai_client.chat.completions.create(
                model=state.get("selected_model", "gpt-4"),
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful AI assistant that can answer general questions and help users with various tasks.",
                    },
                    {"role": "user", "content": query},
                ],
                max_tokens=1000,
                temperature=0.1,
            )

            state["llm_response"] = response.choices[0].message.content

            return state

        except Exception as e:
            self.logger.error(f"Error in general node: {e}")
            state["llm_response"] = f"Error generating response: {str(e)}"
            return state

    def output_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Format and prepare final output"""
        try:
            # Add assistant response to conversation history
            state["conversation_history"].append(
                {
                    "role": "assistant",
                    "content": state["llm_response"],
                    "timestamp": str(datetime.now()),
                }
            )

            # Update last updated timestamp
            state["last_updated"] = str(datetime.now())

            return state

        except Exception as e:
            self.logger.error(f"Error in output node: {e}")
            return state

    def _generate_pinecone_response(self, query: str, documents: list) -> str:
        """Generate response for Pinecone search results"""
        try:
            # Prepare document summaries
            doc_summaries = []
            for doc in documents:
                metadata = doc.get("metadata", {})
                filename = metadata.get("filename", "Unknown file")
                text_preview = metadata.get("text_preview", "No preview available")
                doc_summaries.append(f"File: {filename}\nContent: {text_preview}")

            prompt = f"""
            Based on the following search results from documents, please provide a helpful answer to the user's question.

            User Question: {query}

            Relevant Documents:
            {"\n\n".join(doc_summaries)}

            Please provide a clear, helpful answer based on the document content found.
            """

            response = self.openai_client.chat.completions.create(
                model=self.config.get("openai_model", "gpt-4"),
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful AI assistant that answers questions based on document content.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=800,
                temperature=0.1,
            )

            return response.choices[0].message.content

        except Exception as e:
            self.logger.error(f"Error generating Pinecone response: {e}")
            return f"Error generating response: {str(e)}"

    def _generate_snowflake_response(
        self, query: str, data: list, data_type: str
    ) -> str:
        """Generate response for Snowflake data results"""
        try:
            # Prepare data summary
            if data_type == "user_data":
                data_summary = f"Found {len(data)} user records"
            else:
                data_summary = f"Found {len(data)} transaction records"

            # Sample some data for context
            sample_data = data[:3] if len(data) > 3 else data
            data_preview = str(sample_data)

            prompt = f"""
            Based on the following data from the database, please provide a helpful answer to the user's question.

            User Question: {query}

            Data Summary: {data_summary}
            Sample Data: {data_preview}

            Please provide a clear, helpful answer based on the data retrieved.
            """

            response = self.openai_client.chat.completions.create(
                model=self.config.get("openai_model", "gpt-4"),
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful AI assistant that answers questions based on database data.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=800,
                temperature=0.1,
            )

            return response.choices[0].message.content

        except Exception as e:
            self.logger.error(f"Error generating Snowflake response: {e}")
            return f"Error generating response: {str(e)}"
