"""
State management for the Smart Hybrid Agent
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class Message:
    """A message in the conversation"""

    role: str  # "user" or "assistant"
    content: str
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    """Result from a tool execution"""

    tool_name: str
    success: bool
    data: Any
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class AgentState:
    """State for the LangGraph agent"""

    # Core conversation state
    conversation_id: str = ""
    user_message: str = ""
    conversation_history: List[Message] = field(default_factory=list)

    # Current context and routing
    current_context: str = "general"  # "snowflake", "pinecone", "hybrid", or "general"
    selected_model: str = "gpt-4"

    # Tool execution results
    tool_results: List[ToolResult] = field(default_factory=list)

    # Memory for connecting information across questions
    pdf_context: str = ""  # Remember PDF information
    user_data_context: str = ""  # Remember user data from Snowflake
    transaction_context: str = ""  # Remember transaction data from Snowflake
    combined_context: str = ""  # Synthesized context for coherent responses

    # LLM response
    llm_response: str = ""

    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)

    def add_message(
        self, role: str, content: str, metadata: Optional[Dict[str, Any]] = None
    ):
        """Add a message to the conversation history"""
        message = Message(role=role, content=content, metadata=metadata or {})
        self.conversation_history.append(message)
        self.last_updated = datetime.now()

        # Keep only recent history
        if len(self.conversation_history) > 50:
            self.conversation_history = self.conversation_history[-50:]

    def add_tool_result(
        self, tool_name: str, success: bool, data: Any, error: Optional[str] = None
    ):
        """Add a tool execution result"""
        result = ToolResult(
            tool_name=tool_name, success=success, data=data, error=error
        )
        self.tool_results.append(result)
        self.last_updated = datetime.now()

    def update_context(self, context: str):
        """Update the current context"""
        self.current_context = context
        self.last_updated = datetime.now()

    def get_recent_tool_results(
        self, tool_name: Optional[str] = None, limit: int = 5
    ) -> List[ToolResult]:
        """Get recent tool results, optionally filtered by tool name"""
        results = self.tool_results[-limit:] if limit > 0 else self.tool_results

        if tool_name:
            results = [r for r in results if r.tool_name == tool_name]

        return results

    def get_conversation_summary(self) -> str:
        """Get a summary of the conversation for context"""
        if not self.conversation_history:
            return "No conversation history yet."

        # Get last few messages for context
        recent_messages = self.conversation_history[-5:]
        summary = "Recent conversation:\n"

        for msg in recent_messages:
            role = "User" if msg.role == "user" else "Assistant"
            summary += f"{role}: {msg.content[:100]}{'...' if len(msg.content) > 100 else ''}\n"

        return summary

    def get_memory_summary(self) -> str:
        """Get a summary of stored memory contexts"""
        memory_parts = []

        if self.pdf_context:
            memory_parts.append(f"PDF Context: {self.pdf_context[:200]}...")

        if self.user_data_context:
            memory_parts.append(f"User Data Context: {self.user_data_context[:200]}...")

        if self.transaction_context:
            memory_parts.append(
                f"Transaction Context: {self.transaction_context[:200]}..."
            )

        if self.combined_context:
            memory_parts.append(f"Combined Context: {self.combined_context[:200]}...")

        if not memory_parts:
            return "No memory contexts stored yet."

        return "\n".join(memory_parts)

    def clear_memory(self):
        """Clear all memory contexts"""
        self.pdf_context = ""
        self.user_data_context = ""
        self.transaction_context = ""
        self.combined_context = ""
        self.last_updated = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert state to dictionary for serialization"""
        return {
            "conversation_id": self.conversation_id,
            "user_message": self.user_message,
            "current_context": self.current_context,
            "selected_model": self.selected_model,
            "pdf_context": self.pdf_context,
            "user_data_context": self.user_data_context,
            "transaction_context": self.transaction_context,
            "combined_context": self.combined_context,
            "llm_response": self.llm_response,
            "created_at": self.created_at.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "message_count": len(self.conversation_history),
            "tool_result_count": len(self.tool_results),
        }
