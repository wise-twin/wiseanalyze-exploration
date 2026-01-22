from typing import Dict, Any
import json, hashlib
from langchain_core.messages import HumanMessage

class Cached_LLM:
    def __init__(self, llm, SYSTEM_MESSAGE, prompts, cache_file: str = "cache.json"):
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
        if field not in self.prompts.keys():
            return "<AI> To Prompt"

        selectedPrompt = self.prompts[field]["prompt"].substitute(context=context)
        selectedSchema = self.prompts[field]["schema"]
        response = self.prompt(selectedPrompt, selectedSchema).response

        return response