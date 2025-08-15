"""
Configuration settings for the Smart Hybrid Agent
"""

import os
from dataclasses import dataclass


@dataclass
class AgentConfig:
    """Configuration for the agent"""

    # OpenAI Configuration
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4")

    # Pinecone Configuration
    pinecone_api_key: str = os.getenv("PINECONE_API_KEY", "")
    pinecone_index_name: str = os.getenv("PINECONE_INDEX_NAME", "")
    pinecone_namespace: str = os.getenv("PINECONE_NAMESPACE", "")  # Optional

    # Snowflake Configuration
    snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    snowflake_user: str = os.getenv("SNOWFLAKE_USER", "")
    snowflake_database: str = os.getenv("SNOWFLAKE_DATABASE", "")
    snowflake_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")
    snowflake_role: str = os.getenv("SNOWFLAKE_ROLE", "")
    snowflake_schema: str = os.getenv("SNOWFLAKE_SCHEMA", "")
    snowflake_private_key_file: str = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH", "")
    snowflake_private_key_passphrase: str = os.getenv(
        "SNOWFLAKE_PRIVATE_KEY_FILE_PWD", ""
    )

    # Agent Configuration
    max_conversation_history: int = 50
    max_tokens: int = 4000
    temperature: float = 0.1

    def validate(self) -> bool:
        """Validate that required configuration is present"""
        required_fields = [
            "openai_api_key",
            "pinecone_api_key",
            "pinecone_index_name",
            "snowflake_account",
            "snowflake_user",
            "snowflake_database",
            "snowflake_warehouse",
            "snowflake_role",
            "snowflake_schema",
            "snowflake_private_key_file",
            "snowflake_private_key_passphrase",
        ]

        missing_fields = [
            field for field in required_fields if not getattr(self, field)
        ]

        if missing_fields:
            print(f"Missing required configuration: {missing_fields}")
            return False

        return True


def get_config() -> AgentConfig:
    """Get agent configuration"""
    return AgentConfig()
