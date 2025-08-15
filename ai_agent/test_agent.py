#!/usr/bin/env python3
"""
Test script for Smart Hybrid Agent components
"""

import sys
from pathlib import Path

# Add the current directory to the path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from agent.state import AgentState
from config.settings import get_config


def test_config():
    """Test configuration loading"""
    print("Testing configuration...")
    try:
        config = get_config()
        print("✅ Configuration loaded successfully")
        print(f"   OpenAI Model: {config.openai_model}")
        print(f"   Max History: {config.max_conversation_history}")
        return True
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False


def test_state():
    """Test state management"""
    print("\nTesting state management...")
    try:
        state = AgentState(conversation_id="test_session", selected_model="gpt-4")

        # Test message addition
        state.add_message("user", "Hello, how are you?")
        state.add_message("assistant", "I'm doing well, thank you!")

        # Test tool results
        state.add_tool_result("test_tool", True, "Test data")

        # Test context updates
        state.update_context("test_context")

        print("✅ State management test passed")
        print(f"   Messages: {len(state.conversation_history)}")
        print(f"   Tool Results: {len(state.tool_results)}")
        print(f"   Context: {state.current_context}")

        return True

    except Exception as e:
        print(f"❌ State management test failed: {e}")
        return False


def test_memory():
    """Test memory functionality"""
    print("\nTesting memory functionality...")
    try:
        state = AgentState()

        # Test memory storage
        state.pdf_context = "PDF information about user policies"
        state.user_data_context = "User data from Snowflake"
        state.transaction_context = "Transaction data from Snowflake"

        # Test memory summary
        memory_summary = state.get_memory_summary()
        print("✅ Memory test passed")
        print(f"   Memory Summary: {memory_summary[:100]}...")

        # Test memory clearing
        state.clear_memory()
        print(
            f"   Memory cleared: {not any([state.pdf_context, state.user_data_context, state.transaction_context])}"
        )

        return True

    except Exception as e:
        print(f"❌ Memory test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("🧪 AI Agent - Component Tests\n")

    tests = [test_config, test_state, test_memory]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1

    print(f"\n📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! The agent components are working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
