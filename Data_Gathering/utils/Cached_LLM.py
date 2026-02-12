"""
LLM caching and structured output wrapper for production environments.

This class provides intelligent caching for LLM structured output calls using
context-based MD5 hashing. It eliminates redundant API calls for identical
input contexts while maintaining structured Pydantic schema validation.

Key Features:
- Persistent JSON cache with automatic serialization/deserialization
- MD5 context hashing for deterministic cache lookup
- Configurable prompt/schema mapping for different extraction tasks
- Graceful cache miss handling with API fallback
- Force-refresh capability for cache invalidation
"""

from typing import Dict, Any
import json, hashlib
from langchain_core.messages import HumanMessage

class Cached_LLM:
    """Cached LLM wrapper for efficient structured output extraction.

    Wraps any LangChain LLM with intelligent caching based on input context hash.
    Supports structured Pydantic schema output with automatic cache persistence.

    Attributes:
        llm: Underlying LangChain LLM instance.
        SYSTEM_MESSAGE (SystemMessage): Global system prompt for all interactions.
        prompts (Dict[str, Dict]): Prompt/schema configuration mapping.
        cache_file (Path): Path to persistent JSON cache file.
        cache (Dict[str, Dict[str, Any]]): In-memory cache dictionary.
    """
    def __init__(self, llm, SYSTEM_MESSAGE, prompts, cache_file: str = "cache.json"):
        """Initialize cached LLM wrapper.
        
        Args:
            llm: LangChain LLM instance (ChatOpenAI, etc.).
            SYSTEM_MESSAGE (SystemMessage): System prompt for structured extraction.
            prompts (Dict[str, Dict]): Prompt configuration dictionary with structure:
                {
                    "field_name": {
                        "prompt": string.Template,  # Prompt template
                        "schema": Pydantic model     # Output schema
                    }
                }
            cache_file (str): Path to JSON cache file. Defaults to "cache.json".
        
        Raises:
            json.JSONDecodeError: If cache file contains invalid JSON.
        """
        self.llm = llm
        self.SYSTEM_MESSAGE = SYSTEM_MESSAGE
        self.prompts = prompts
        self.cache_file = cache_file
        try:
            with open(cache_file, 'r') as f:
                self.cache: Dict[str, Dict[str, Any]] = json.load(f)
        except FileNotFoundError:
            self.cache = {}
    
    def _get(self, context_hash: str, schema):
        if context_hash in self.cache:
            data = self.cache[context_hash]
            return schema(**data)
        return None
    
    def _set(self, context_hash: str, result):
        self.cache[context_hash] = result.model_dump()
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f)

    def prompt(self, context: str, schema, force_run : bool = False):
        """Execute LLM prompt with intelligent caching.
        
        Args:
            context (str): Input text context for LLM processing.
            schema: Pydantic schema for structured output validation.
            force_run (bool): Bypass cache and force API call. Defaults to False.
        
        Returns:
            Pydantic model instance with structured LLM response.
        
        Raises:
            Exception: Any LLM invocation or parsing errors.
        """
        context_hash = hashlib.md5(context.encode()).hexdigest()
        
        if not force_run :
            cached = self._get(context_hash, schema)
            if cached:
                print(f"✅ Cache HIT for context hash: {context_hash[:8]}")
                return cached
        
        print(f"🔄 Cache MISS - calling API for hash: {context_hash[:8]}")
        
        messages = [
            self.SYSTEM_MESSAGE,
            HumanMessage(content=context)
        ]

        structured_llm = self.llm.with_structured_output(schema, include_raw=True)
        result = structured_llm.invoke(messages)
        
        self._set(context_hash, result["parsed"])
        return result["parsed"]
    
    def ask_ai(self, field: str, context: str):   
        """Execute predefined prompt for specific extraction field.
        
        Simplified interface for common extraction tasks using preconfigured
        prompt/schema pairs.
        
        Args:
            field (str): Extraction field key from self.prompts dictionary.
            context (str): Input text context for extraction.
        
        Returns:
            Extracted field value (str, int, or structured data).
        """
        if field not in self.prompts.keys():
            print(f"<MISSING> Prompting Cached LLM for MISSING Field : {field}")
            return "<AI> To Prompt"

        selectedPrompt = self.prompts[field]["prompt"].substitute(context=context)
        selectedSchema = self.prompts[field]["schema"]
        print(f"Prompting Cached LLM for Field : {field}")
        response = self.prompt(selectedPrompt, selectedSchema).response

        return response