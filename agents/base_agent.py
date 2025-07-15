from abc import ABC, abstractmethod

class BaseAgent(ABC):
    @abstractmethod
    def run(self, quote_row: dict, context: dict = {}) -> dict:
        """
        Run the agent's logic on a single quote.

        Parameters:
        - quote_row: dict representing a single enriched quote
        - context: dict with previous agent outputs (optional)

        Returns:
        A dict with:
        - agent: name of the agent
        - decision: pass/fail/score/etc.
        - explanation: reasoning (LLM or rule-based)
        - flags: optional list of flags
        - metadata: optional extra info
        """
        pass