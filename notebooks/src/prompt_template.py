class PromptTemplate:
    def __init__(self, name: str, meta: dict, sections: dict):
        """
        name: template name (e.g. "column_added_flat")
        meta: meta‑header fields like max_tokens, tags, etc.
        sections: dict mapping section names → section content (with placeholders)
        """
        self.name = name
        self.meta = meta
        self.sections = sections

    def render(self, values: dict) -> str:
        """
        Fill placeholders in each section, then join into single prompt text
        """
        filled = []
        for sec, text in self.sections.items():
            try:
                filled_text = text.format(**values)
            except KeyError as e:
                filled_text = text  # leave placeholder unresolved if missing
            filled.append(f"<!-- {sec.upper()} -->\n{text.format(**values)}")
        return "\n\n".join(filled)