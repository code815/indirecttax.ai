import re, yaml

class Classifier:
    def __init__(self, path="rules/topic_rules.yaml"):
        self.cfg = yaml.safe_load(open(path, "r", encoding="utf-8"))
        self.negatives = [re.compile(n, re.I) for n in self.cfg.get("negatives", [])]
        self.weights = {"Rates":4,"Forms":3,"Exemptions":2,"Freight":2,"Marketplace":2,"Deadlines":2}

    def topic_and_score(self, text: str):
        txt = text.lower()
        for pat in self.negatives:
            if pat.search(txt): return "General", 0
        best = ("General",1)
        for topic, rule in self.cfg["topics"].items():
            for clause in rule.get("any", []):
                if all(re.search(term, txt) for term in clause):
                    return topic, self.weights.get(topic,1)
        return best

def derive_title(text: str) -> str:
    # first non-empty line as a title-ish string
    for line in text.splitlines():
        L=line.strip()
        if len(L)>10: return (L[:140]+"…") if len(L)>140 else L
    return "Untitled"
