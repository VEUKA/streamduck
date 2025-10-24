"""
Test script to validate Azure OpenAI integration with smart retry.

This script tests the LLM analyzer with Azure OpenAI configuration.
"""

import asyncio
import os
import sys
from pathlib import Path

import pytest

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv

from utils.smart_retry import ExceptionAnalyzer, RetryDecision


@pytest.mark.asyncio
async def test_azure_openai():
    """Test Azure OpenAI LLM analyzer."""
    print("🧪 Testing Azure OpenAI LLM Integration\n")
    
    # Load environment from .env.test
    env_file = Path(__file__).parent.parent / ".env.test"
    if not env_file.exists():
        print("❌ .env.test file not found!")
        return False
    
    load_dotenv(env_file)
    
    # Get configuration
    provider = os.getenv("SMART_RETRY_LLM_PROVIDER", "azure")
    model = os.getenv("SMART_RETRY_LLM_MODEL", "gpt-5-mini")
    api_key = os.getenv("SMART_RETRY_LLM_API_KEY")
    endpoint = os.getenv("SMART_RETRY_LLM_ENDPOINT")
    
    print(f"📋 Configuration:")
    print(f"   Provider: {provider}")
    print(f"   Model: {model}")
    print(f"   Endpoint: {endpoint[:60]}..." if endpoint else "   Endpoint: None")
    print(f"   API Key: {'*' * 20}{api_key[-4:] if api_key else 'None'}\n")
    
    if not api_key:
        print("❌ No API key found in environment!")
        return False
    
    # Create analyzer
    try:
        analyzer = ExceptionAnalyzer(
            llm_provider=provider,
            llm_model=model,
            llm_api_key=api_key,
            llm_endpoint=endpoint,
            timeout_seconds=30,
            enable_caching=True,
        )
        print("✅ ExceptionAnalyzer created successfully\n")
    except Exception as e:
        print(f"❌ Failed to create ExceptionAnalyzer: {e}")
        return False
    
    # Test 1: Network timeout (should retry)
    print("🔬 Test 1: Network Timeout Error")
    print("   Expected: Should recommend RETRY\n")
    
    try:
        exception = TimeoutError("Connection timed out after 30 seconds")
        context = {
            "operation": "ingest_batch",
            "attempt": 1,
            "max_attempts": 3,
            "table": "test_table",
        }
        
        decision = await analyzer.analyze_exception(exception, context)
        
        print(f"   Decision: {'✅ RETRY' if decision.should_retry else '❌ STOP'}")
        print(f"   Reasoning: {decision.reasoning}")
        print(f"   Confidence: {decision.confidence:.2f}")
        print(f"   Suggested wait: {decision.suggested_wait_seconds}s")
        print(f"   Result: {'✅ PASS' if decision.should_retry else '❌ FAIL'}\n")
        
        test1_pass = decision.should_retry
    except Exception as e:
        print(f"   ❌ Test failed with error: {e}\n")
        test1_pass = False
    
    # Test 2: Authentication error (should NOT retry)
    print("🔬 Test 2: Authentication Error")
    print("   Expected: Should recommend STOP\n")
    
    try:
        exception = PermissionError("Authentication failed: Invalid credentials")
        context = {
            "operation": "ingest_batch",
            "attempt": 1,
            "max_attempts": 3,
        }
        
        decision = await analyzer.analyze_exception(exception, context)
        
        print(f"   Decision: {'✅ RETRY' if decision.should_retry else '❌ STOP'}")
        print(f"   Reasoning: {decision.reasoning}")
        print(f"   Confidence: {decision.confidence:.2f}")
        print(f"   Suggested wait: {decision.suggested_wait_seconds}s")
        print(f"   Result: {'✅ PASS' if not decision.should_retry else '❌ FAIL'}\n")
        
        test2_pass = not decision.should_retry
    except Exception as e:
        print(f"   ❌ Test failed with error: {e}\n")
        test2_pass = False
    
    # Test 3: Caching (should use cached decision)
    print("🔬 Test 3: Caching Test")
    print("   Expected: Should use cached decision from Test 1\n")
    
    try:
        # Same exception as Test 1
        exception = TimeoutError("Connection timed out after 30 seconds")
        context = {
            "operation": "ingest_batch",
            "attempt": 2,
            "max_attempts": 3,
        }
        
        stats_before = analyzer.get_stats()
        api_calls_before = stats_before["api_calls"]
        
        decision = await analyzer.analyze_exception(exception, context)
        
        stats_after = analyzer.get_stats()
        api_calls_after = stats_after["api_calls"]
        
        cached = api_calls_before == api_calls_after
        
        print(f"   Decision: {'✅ RETRY' if decision.should_retry else '❌ STOP'}")
        print(f"   API calls before: {api_calls_before}")
        print(f"   API calls after: {api_calls_after}")
        print(f"   Cached: {'✅ YES' if cached else '❌ NO'}")
        print(f"   Result: {'✅ PASS' if cached else '❌ FAIL'}\n")
        
        test3_pass = cached
    except Exception as e:
        print(f"   ❌ Test failed with error: {e}\n")
        test3_pass = False
    
    # Summary
    print("="*60)
    print("📊 Test Summary")
    print("="*60)
    print(f"   Test 1 (Network Timeout): {'✅ PASS' if test1_pass else '❌ FAIL'}")
    print(f"   Test 2 (Auth Error): {'✅ PASS' if test2_pass else '❌ FAIL'}")
    print(f"   Test 3 (Caching): {'✅ PASS' if test3_pass else '❌ FAIL'}")
    
    # Get final stats
    final_stats = analyzer.get_stats()
    print(f"\n📈 LLM Statistics:")
    print(f"   Total API calls: {final_stats['api_calls']}")
    print(f"   Cached decisions: {final_stats['cached_decisions']}")
    print(f"   Cache enabled: {final_stats['cache_enabled']}")
    
    all_pass = test1_pass and test2_pass and test3_pass
    print(f"\n{'✅' if all_pass else '❌'} Overall: {'ALL TESTS PASSED' if all_pass else 'SOME TESTS FAILED'}\n")
    
    return all_pass


if __name__ == "__main__":
    try:
        result = asyncio.run(test_azure_openai())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Test failed with unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
