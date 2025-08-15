# Smart Hybrid Agent

An intelligent AI agent that combines multiple knowledge sources using LangGraph, OpenAI, Pinecone, and Snowflake to provide comprehensive answers to complex queries.

## Features

- **Multi-Source Intelligence**: Combines structured data (Snowflake) with unstructured documents (PDFs via Pinecone)
- **Conversation Memory**: Maintains context across questions and synthesizes information coherently
- **Model Switching**: Choose between different OpenAI models (GPT-4, GPT-3.5-turbo, GPT-4-turbo)
- **Intelligent Routing**: Automatically determines which tools to use based on query content
- **PDF Processing**: Optional chunking and embedding of new PDFs to Pinecone
- **Conversation Management**: Switch between different conversation sessions

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   User Input    │───▶│  Context Router │───▶│  Tool Selection │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Pinecone      │    │   Snowflake     │
                       │  (PDF Search)   │    │  (Data Query)   │
                       └─────────────────┘    └─────────────────┘
                                │                       │
                                ▼                       ▼
                       ┌─────────────────────────────────────────┐
                       │        Memory Synthesis                 │
                       │    (Connect Information)                │
                       └─────────────────────────────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │  LLM Response   │
                       └─────────────────┘
```

## Installation

1. **Install dependencies**:
   ```bash
   cd ai_agent
uv add openai pinecone-client snowflake-connector-python PyPDF2 python-dotenv
   ```

2. **Set environment variables**:
   ```bash
   export OPENAI_API_KEY="your-openai-api-key"
   export PINECONE_API_KEY="your-pinecone-api-key"
   export PINECONE_INDEX_NAME="your-pinecone-index"
   export PINECONE_NAMESPACE="your-namespace"  # Optional
   export SNOWFLAKE_ACCOUNT="your-snowflake-account"
   export SNOWFLAKE_USER="your-snowflake-user"
   export SNOWFLAKE_DATABASE="your-snowflake-database"
   export SNOWFLAKE_WAREHOUSE="your-snowflake-warehouse"
   export SNOWFLAKE_ROLE="your-snowflake-role"
   export SNOWFLAKE_SCHEMA="your-snowflake-schema"
   export SNOWFLAKE_PRIVATE_KEY_FILE_PATH="path/to/private/key"
   export SNOWFLAKE_PRIVATE_KEY_FILE_PWD="your-private-key-passphrase"
   ```

## Usage

### Interactive Chat Mode

Start an interactive chat session:

```bash
# Basic chat
uv run ai_agent/main.py

# With specific conversation ID
uv run ai_agent/main.py --conversation-id "user_analysis_001"

# With specific model
uv run ai_agent/main.py --model "gpt-3.5-turbo" --conversation-id "session_001"
```

### PDF Processing Mode

Process and upload a PDF to Pinecone:

```bash
uv run ai_agent/main.py --process-pdf "path/to/document.pdf"
```

### Chat Commands

During interactive chat, use these commands:
- `help` - Show available commands
- `status` - Show conversation status and memory
- `clear` - Clear all stored memory contexts
- `quit`/`exit`/`q` - Exit the chat session

## Example Conversation Flow

1. **Question 1**: "What are the company policies on user data?" (Pinecone)
   - Agent searches PDF documents and stores context

2. **Question 2**: "Show me top 10 users by transaction volume" (Snowflake)
   - Agent queries user data and stores context

3. **Question 3**: "What are the recent transaction patterns?" (Snowflake)
   - Agent queries transaction data and stores context

4. **Question 4**: "How do our user policies align with actual transaction behavior?" (Hybrid)
   - Agent synthesizes all contexts for coherent answer

## Configuration

The agent automatically detects the query type and routes to appropriate tools:

- **Pinecone**: Document content, policies, procedures
- **Snowflake**: User data, transactions, metrics
- **Hybrid**: Complex queries requiring multiple sources
- **General**: General conversation and questions

## Memory Management

The agent maintains several memory contexts:
- `pdf_context`: Information from PDF documents
- `user_data_context`: User data from Snowflake
- `transaction_context`: Transaction data from Snowflake
- `combined_context`: Synthesized information

## Dependencies

- **OpenAI**: LLM capabilities and embeddings
- **Pinecone**: Vector database for PDF search
- **Snowflake**: Structured data warehouse
- **PyPDF2**: PDF text extraction
- **LangGraph**: Workflow orchestration (future)

## Development

### Project Structure

```
ai_agent/
├── __init__.py
├── main.py                 # CLI entry point
├── README.md              # This file
├── agent/
│   ├── __init__.py
│   ├── state.py           # State management
│   ├── nodes.py           # LangGraph nodes
│   └── tools.py           # Tool implementations
├── connectors/
│   ├── __init__.py
│   ├── snowflake_connector.py
│   └── pinecone_connector.py
├── utils/
│   ├── __init__.py
│   └── pdf_processor.py   # PDF processing
└── config/
    ├── __init__.py
    └── settings.py        # Configuration
```

### Adding New Tools

1. Extend `AgentTools` class in `agent/tools.py`
2. Add routing logic in `route_query()` method
3. Create corresponding node in `agent/nodes.py`
4. Update tool execution in `execute_tool()` method

### Adding New Data Sources

1. Create connector in `connectors/` directory
2. Add configuration in `config/settings.py`
3. Integrate with tools and nodes
4. Update memory context handling

## Troubleshooting

### Common Issues

1. **Connection Errors**: Check environment variables and credentials
2. **PDF Processing Failures**: Ensure PyPDF2 is installed and file is accessible
3. **Memory Issues**: Use `clear` command to reset memory contexts
4. **Model Errors**: Verify OpenAI API key and model availability

### Logging

Set log level for debugging:
```bash
uv run ai_agent/main.py --log-level DEBUG
```

Logs are saved to `logs/ai_agent.log` in the logs directory.

## Future Enhancements

- **LangGraph Integration**: Full workflow orchestration
- **Advanced Memory**: Vector-based memory retrieval
- **Tool Chaining**: Multi-step tool execution
- **Web Interface**: Web-based chat interface
- **API Endpoints**: REST API for integration
- **Advanced PDF Processing**: Better text extraction and chunking

## License

This project is part of the FoundryAI Academy capstone project.
