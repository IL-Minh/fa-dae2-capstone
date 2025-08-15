#!/usr/bin/env python3
"""
Smart Hybrid Agent - Main CLI Interface
"""

import argparse
import logging
import sys
from pathlib import Path

# Add the current directory to the path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from agent.nodes import AgentNodes
from agent.state import AgentState
from agent.tools import AgentTools
from config.settings import get_config
from connectors.pinecone_connector import PineconeConnector
from connectors.snowflake_connector import SnowflakeConnector
from utils.pdf_processor import PDFProcessor


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/ai_agent.log"),
        ],
    )


def initialize_connectors(config: dict) -> tuple:
    """Initialize Snowflake and Pinecone connectors"""
    try:
        # Initialize Snowflake connector
        snowflake_config = {
            "account": config.snowflake_account,
            "user": config.snowflake_user,
            "database": config.snowflake_database,
            "warehouse": config.snowflake_warehouse,
            "role": config.snowflake_role,
            "schema": config.snowflake_schema,
            "private_key_file": config.snowflake_private_key_file,
            "private_key_passphrase": config.snowflake_private_key_passphrase,
        }

        snowflake_connector = SnowflakeConnector(snowflake_config)

        # Initialize Pinecone connector
        pinecone_config = {
            "api_key": config.pinecone_api_key,
            "index_name": config.pinecone_index_name,
            "openai_api_key": config.openai_api_key,
        }

        pinecone_connector = PineconeConnector(pinecone_config)

        return snowflake_connector, pinecone_connector

    except Exception as e:
        logging.error(f"Failed to initialize connectors: {e}")
        raise


def process_pdf(pdf_path: str, config: dict) -> bool:
    """Process a PDF file and upload to Pinecone"""
    try:
        logging.info(f"Processing PDF: {pdf_path}")

        # Initialize PDF processor
        processor_config = {"pinecone_api_key": config.pinecone_api_key}

        processor = PDFProcessor(processor_config)

        # Process the PDF
        success = processor.process_pdf(
            pdf_path=pdf_path,
            index_name=config.pinecone_index_name,
            chunk_size=1000,
            overlap=200,
        )

        if success:
            logging.info(f"Successfully processed PDF: {pdf_path}")
            return True
        else:
            logging.error(f"Failed to process PDF: {pdf_path}")
            return False

    except Exception as e:
        logging.error(f"Error processing PDF {pdf_path}: {e}")
        return False


def interactive_chat(config: dict, conversation_id: str, model: str) -> None:
    """Interactive chat mode"""
    try:
        logging.info(f"Starting interactive chat session: {conversation_id}")
        logging.info(f"Using model: {model}")

        # Initialize connectors
        snowflake_connector, pinecone_connector = initialize_connectors(config)

        # Test connections
        if not snowflake_connector.test_connection():
            logging.error("Failed to connect to Snowflake")
            return

        if not pinecone_connector.test_connection():
            logging.error("Failed to connect to Pinecone")
            return

        logging.info("All connections successful")

        # Initialize agent components
        agent_config = {"openai_api_key": config.openai_api_key, "openai_model": model}

        tools = AgentTools(snowflake_connector, pinecone_connector, agent_config)
        nodes = AgentNodes(tools, agent_config)

        # Initialize state
        state = AgentState(conversation_id=conversation_id, selected_model=model)

        print(f"\n🤖 Smart Hybrid Agent - Conversation: {conversation_id}")
        print("📊 Connected to Snowflake and Pinecone")
        print(f"🧠 Using model: {model}")
        print("💬 Type 'quit' to exit, 'help' for commands\n")

        while True:
            try:
                # Get user input
                user_input = input("👤 You: ").strip()

                if user_input.lower() in ["quit", "exit", "q"]:
                    print("👋 Goodbye!")
                    break

                if user_input.lower() == "help":
                    print_help()
                    continue

                if user_input.lower() == "status":
                    print_status(state)
                    continue

                if user_input.lower() == "clear":
                    state.clear_memory()
                    print("🧹 Memory cleared")
                    continue

                if not user_input:
                    continue

                # Update state with user message
                state.user_message = user_input

                # Process through nodes (simplified workflow for now)
                state = nodes.input_node(state)
                state = nodes.context_router_node(state)
                state = nodes.output_node(state)

                # Display response
                print(f"\n🤖 Assistant: {state.llm_response}\n")

            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                logging.error(f"Error in chat loop: {e}")
                print(f"❌ Error: {str(e)}")

        # Cleanup
        snowflake_connector.disconnect()

    except Exception as e:
        logging.error(f"Error in interactive chat: {e}")
        print(f"❌ Fatal error: {str(e)}")


def print_help() -> None:
    """Print help information"""
    help_text = """
Available Commands:
- help: Show this help message
- status: Show current conversation status and memory
- clear: Clear all stored memory contexts
- quit/exit/q: Exit the chat session

The agent can:
- Answer questions about PDF documents (using Pinecone)
- Query user and transaction data (using Snowflake)
- Synthesize information from multiple sources
- Remember context across questions
- Switch between different OpenAI models
    """
    print(help_text)


def print_status(state: AgentState) -> None:
    """Print current conversation status"""
    print("\n📊 Conversation Status:")
    print(f"   ID: {state.conversation_id}")
    print(f"   Model: {state.selected_model}")
    print(f"   Messages: {len(state.conversation_history)}")
    print(f"   Tool Results: {len(state.tool_results)}")
    print(f"   Last Updated: {state.last_updated}")

    print("\n🧠 Memory Contexts:")
    print(f"   PDF Context: {'✅' if state.pdf_context else '❌'}")
    print(f"   User Data Context: {'✅' if state.user_data_context else '❌'}")
    print(f"   Transaction Context: {'✅' if state.transaction_context else '❌'}")
    print(f"   Combined Context: {'✅' if state.combined_context else '❌'}")

    if state.pdf_context:
        print(f"   PDF Info: {state.pdf_context[:100]}...")
    if state.user_data_context:
        print(f"   User Data: {state.user_data_context[:100]}...")
    if state.transaction_context:
        print(f"   Transaction Data: {state.transaction_context[:100]}...")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Smart Hybrid Agent - AI Agent with LangGraph, OpenAI, Pinecone, and Snowflake"
    )

    parser.add_argument(
        "--conversation-id",
        default="default_session",
        help="Conversation ID for session management (default: default_session)",
    )

    parser.add_argument(
        "--model",
        default="gpt-4",
        choices=["gpt-4", "gpt-3.5-turbo", "gpt-4-turbo"],
        help="OpenAI model to use (default: gpt-4)",
    )

    parser.add_argument(
        "--process-pdf", help="Process a PDF file and upload to Pinecone"
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)

    try:
        # Load configuration
        config = get_config()

        if not config.validate():
            print(
                "❌ Configuration validation failed. Please check your environment variables."
            )
            sys.exit(1)

        # Handle PDF processing mode
        if args.process_pdf:
            success = process_pdf(args.process_pdf, config)
            sys.exit(0 if success else 1)

        # Interactive chat mode
        interactive_chat(config, args.conversation_id, args.model)

    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        print(f"❌ Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
