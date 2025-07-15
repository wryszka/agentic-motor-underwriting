from agents.base_agent import BaseAgent

class EligibilityAgent(BaseAgent):
    def run(self, quote_row: dict, context: dict = {}) -> dict:
        declared = quote_row.get("claims_declared")
        verified = quote_row.get("verified_claims")

        if declared is None or verified is None:
            return {
                "agent": "EligibilityAgent",
                "decision": "refer",
                "explanation": "Missing claim info, requires review.",
                "flags": ["missing_data"]
            }

        if declared != verified:
            return {
                "agent": "EligibilityAgent",
                "decision": "fail",
                "explanation": f"Declared {declared} claims but verified {verified}. Possible misrepresentation.",
                "flags": ["claims_mismatch"]
            }

        return {
            "agent": "EligibilityAgent",
            "decision": "pass",
            "explanation": "Declared claims match verified data.",
            "flags": []
        }