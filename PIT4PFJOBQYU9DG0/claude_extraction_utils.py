"""
claude_extraction_utils.py — Claude extraction toolkit for AI_EXTRACT POC.

Adapted from Anthropic Cookbook summarization patterns, optimized for
Snowflake-native document processing.

Provides:
  - build_guided_prompt():     Config-driven extraction prompt with field schema
  - build_multishot_prompt():  Few-shot prompt with example extractions
  - extract_with_prefill():    API call with assistant prefill + stop sequences
  - chunk_and_synthesize():    Two-pass extraction for long documents
  - parse_xml_sections():      Parse XML-tagged output into dict
  - parse_json_safe():         Robust JSON parsing with fence stripping
  - DocumentIndex:             Summary-indexed search across extracted docs

Usage in Streamlit pages or stored procedures:
    from claude_extraction_utils import (
        build_guided_prompt,
        extract_with_prefill,
        chunk_and_synthesize,
        parse_json_safe,
    )

All functions work with raw text or dicts — no Snowflake session dependency
except DocumentIndex.search_from_snowflake().
"""

import json
import re
from typing import Optional


# ══════════════════════════════════════════════════════════════════════════════
# PROMPT BUILDERS
# ══════════════════════════════════════════════════════════════════════════════


def build_guided_prompt(
    text: str,
    doc_type: str,
    field_schema: dict[str, str],
    extra_instructions: str = "",
) -> str:
    """Build a guided extraction prompt from a doc type config's field schema.

    Args:
        text:               Document text (or PDF content description)
        doc_type:           e.g. "INVOICE", "UTILITY_BILL", "CONTRACT"
        field_schema:       {field_name: description} from DOCUMENT_TYPE_CONFIG
                            e.g. {"vendor_name": "vendor or sender?",
                                  "total": "total amount? number only"}
        extra_instructions: Additional prompt text appended before the document

    Returns:
        Formatted prompt string ready for the messages API
    """
    field_list = "\n".join(
        f"  {i+1}. {name}: {desc}"
        for i, (name, desc) in enumerate(field_schema.items())
    )

    prompt = f"""Extract the following fields from this {doc_type.replace('_', ' ').lower()} document.

Fields to extract:
{field_list}

Rules:
- Return ONLY valid JSON with field names as keys
- Dates in YYYY-MM-DD format
- Numbers as plain digits (no $, no commas, no units)
- If a field is not found, use null
- Do not include any explanation or markdown formatting
"""

    if extra_instructions:
        prompt += f"\n{extra_instructions}\n"

    prompt += f"\nDocument text:\n{text}"

    return prompt


def build_multishot_prompt(
    text: str,
    doc_type: str,
    field_schema: dict[str, str],
    examples: list[dict],
) -> str:
    """Build a few-shot prompt with example document/extraction pairs.

    Args:
        text:          Document text to extract from
        doc_type:      Document type label
        field_schema:  {field_name: description}
        examples:      List of {"document": str, "extraction": dict} pairs
                       Provide 2-3 for best results.

    Returns:
        Formatted prompt with examples
    """
    field_list = "\n".join(
        f"  {i+1}. {name}: {desc}"
        for i, (name, desc) in enumerate(field_schema.items())
    )

    example_blocks = []
    for i, ex in enumerate(examples, 1):
        doc_text = ex["document"][:2000]  # truncate examples to save tokens
        ext_json = json.dumps(ex["extraction"], indent=2)
        example_blocks.append(
            f"<example_{i}>\n"
            f"  <document_{i}>\n    {doc_text}\n  </document_{i}>\n"
            f"  <extraction_{i}>\n    {ext_json}\n  </extraction_{i}>\n"
            f"</example_{i}>"
        )

    examples_str = "\n\n".join(example_blocks)

    prompt = f"""Extract the following fields from this {doc_type.replace('_', ' ').lower()} document.

Fields to extract:
{field_list}

Use these examples for guidance:

{examples_str}

Rules:
- Return ONLY valid JSON with field names as keys
- Follow the exact format shown in the examples
- Dates in YYYY-MM-DD format
- Numbers as plain digits (no $, no commas, no units)
- If a field is not found, use null

Document text:
{text}
"""

    return prompt


# ══════════════════════════════════════════════════════════════════════════════
# API CALL HELPERS
# ══════════════════════════════════════════════════════════════════════════════


def extract_with_prefill(
    client,
    prompt: str,
    model: str = "claude-sonnet-4-20250514",
    max_tokens: int = 4096,
    system: str = "You are a document extraction specialist. You extract structured data from documents with high accuracy.",
    prefill: str = '{"',
    stop_sequences: Optional[list[str]] = None,
    temperature: float = 0.0,
) -> dict:
    """Call the Anthropic API with assistant prefill for clean JSON output.

    The prefill trick starts the assistant's response mid-JSON, forcing
    structured output without preamble. Combined with stop_sequences,
    this gives you parseable JSON every time.

    Args:
        client:          anthropic.Anthropic instance
        prompt:          Full prompt text
        model:           Model identifier
        max_tokens:      Max response tokens
        system:          System prompt
        prefill:         Assistant message prefix (default: start of JSON object)
        stop_sequences:  Stop generation at these strings
        temperature:     0.0 for deterministic extraction

    Returns:
        Dict with keys: extraction (dict), model, input_tokens, output_tokens
    """
    messages = [
        {"role": "user", "content": prompt},
    ]
    if prefill:
        messages.append({"role": "assistant", "content": prefill})

    kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages,
        "temperature": temperature,
    }
    if stop_sequences:
        kwargs["stop_sequences"] = stop_sequences

    response = client.messages.create(**kwargs)

    raw_text = response.content[0].text
    # Reconstruct full JSON by prepending the prefill
    full_text = (prefill or "") + raw_text

    extraction = parse_json_safe(full_text)

    return {
        "extraction": extraction,
        "raw_text": full_text,
        "model": response.model,
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
        "stop_reason": response.stop_reason,
    }


def extract_with_xml(
    client,
    prompt: str,
    model: str = "claude-sonnet-4-20250514",
    max_tokens: int = 4096,
    system: str = "You are a document extraction specialist.",
) -> dict:
    """Call the Anthropic API with XML tag output format.

    Uses the assistant prefill + stop_sequences pattern from the
    Anthropic Cookbook to force clean XML output.

    Returns:
        Dict with keys: sections (dict of parsed XML), raw_text, model, tokens
    """
    messages = [
        {"role": "user", "content": prompt},
        {"role": "assistant", "content": "Here is the extraction: <extraction>"},
    ]

    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=messages,
        stop_sequences=["</extraction>"],
        temperature=0.0,
    )

    raw_text = response.content[0].text
    sections = parse_xml_sections(raw_text)

    return {
        "sections": sections,
        "raw_text": raw_text,
        "model": response.model,
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
    }


# ══════════════════════════════════════════════════════════════════════════════
# CHUNKING + META-SYNTHESIS (for long documents)
# ══════════════════════════════════════════════════════════════════════════════


def chunk_text(text: str, chunk_size: int = 8000, overlap: int = 200) -> list[str]:
    """Split text into overlapping chunks for multi-pass extraction.

    Args:
        text:        Full document text
        chunk_size:  Approximate characters per chunk
        overlap:     Character overlap between chunks to avoid split entities

    Returns:
        List of text chunks
    """
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        # Try to break at a sentence boundary
        if end < len(text):
            # Look for sentence end within last 200 chars of chunk
            search_start = max(end - 200, start)
            last_period = text.rfind(".", search_start, end)
            last_newline = text.rfind("\n", search_start, end)
            break_at = max(last_period, last_newline)
            if break_at > search_start:
                end = break_at + 1
        chunks.append(text[start:end])
        start = end - overlap if end < len(text) else end

    return chunks


def chunk_and_synthesize(
    client,
    text: str,
    doc_type: str,
    field_schema: dict[str, str],
    chunk_model: str = "claude-haiku-4-5-20251001",
    synthesis_model: str = "claude-sonnet-4-20250514",
    chunk_size: int = 8000,
    max_tokens: int = 4096,
) -> dict:
    """Two-pass extraction for documents too long for a single call.

    Pass 1: Haiku extracts from each chunk (fast + cheap)
    Pass 2: Sonnet synthesizes chunk extractions into final result

    Returns:
        Dict with keys: extraction, chunk_count, total_input_tokens, total_output_tokens
    """
    chunks = chunk_text(text, chunk_size)

    if len(chunks) == 1:
        # Short enough for a single pass — use the synthesis model directly
        prompt = build_guided_prompt(text, doc_type, field_schema)
        return extract_with_prefill(client, prompt, model=synthesis_model, max_tokens=max_tokens)

    # Pass 1: Extract from each chunk with Haiku
    chunk_results = []
    total_in = 0
    total_out = 0

    for i, chunk in enumerate(chunks):
        prompt = build_guided_prompt(
            chunk, doc_type, field_schema,
            extra_instructions=f"This is chunk {i+1} of {len(chunks)} from a larger document. "
                               f"Extract whatever fields are visible in this chunk. "
                               f"Use null for fields not present in this chunk.",
        )
        result = extract_with_prefill(
            client, prompt, model=chunk_model, max_tokens=2048,
        )
        chunk_results.append(result["extraction"])
        total_in += result.get("input_tokens", 0)
        total_out += result.get("output_tokens", 0)

    # Pass 2: Synthesize with Sonnet
    field_list = "\n".join(f"  - {name}" for name in field_schema)
    chunks_json = "\n\n".join(
        f"Chunk {i+1}:\n{json.dumps(cr, indent=2)}"
        for i, cr in enumerate(chunk_results)
    )

    synthesis_prompt = f"""You are reviewing partial extractions from {len(chunks)} chunks of a single {doc_type.replace('_', ' ').lower()} document.

Combine these partial results into one final extraction. For each field, use the most complete and accurate value across all chunks. If multiple chunks extracted the same field with different values, prefer the value that appears most specific or complete.

Fields:
{field_list}

Partial extractions:
{chunks_json}

Return ONLY valid JSON with the final merged extraction."""

    synthesis = extract_with_prefill(
        client, synthesis_prompt, model=synthesis_model, max_tokens=max_tokens,
    )
    total_in += synthesis.get("input_tokens", 0)
    total_out += synthesis.get("output_tokens", 0)

    return {
        "extraction": synthesis["extraction"],
        "chunk_count": len(chunks),
        "chunk_results": chunk_results,
        "total_input_tokens": total_in,
        "total_output_tokens": total_out,
        "model": synthesis_model,
    }


# ══════════════════════════════════════════════════════════════════════════════
# PARSING HELPERS
# ══════════════════════════════════════════════════════════════════════════════


def parse_json_safe(text: str) -> dict:
    """Parse JSON from text, handling markdown fences and partial output.

    Strips ```json fences, attempts to close unclosed braces,
    and falls back to {"raw_text": text} if parsing fails.
    """
    if not text or not text.strip():
        return {}

    cleaned = text.strip()

    # Strip markdown code fences
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    # Try direct parse
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Try to find JSON object in the text
    brace_start = cleaned.find("{")
    brace_end = cleaned.rfind("}")
    if brace_start >= 0 and brace_end > brace_start:
        try:
            return json.loads(cleaned[brace_start:brace_end + 1])
        except json.JSONDecodeError:
            pass

    # Try closing unclosed braces (partial output from stop_sequence)
    if cleaned.startswith("{") and not cleaned.endswith("}"):
        open_count = cleaned.count("{") - cleaned.count("}")
        if 0 < open_count <= 3:
            try:
                return json.loads(cleaned + "}" * open_count)
            except json.JSONDecodeError:
                pass

    return {"raw_text": text}


def parse_xml_sections(text: str) -> dict[str, list[str]]:
    """Parse XML-tagged sections from Claude output into a dict.

    Handles output like:
        <parties involved>
        - Sublessor: John Smith
        - Sublessee: Jane Doe
        </parties involved>

    Returns:
        {"parties involved": ["Sublessor: John Smith", "Sublessee: Jane Doe"]}
    """
    pattern = r"<(.*?)>(.*?)</\1>"
    matches = re.findall(pattern, text, re.DOTALL)

    sections = {}
    for tag, content in matches:
        items = [
            item.strip("- ").strip()
            for item in content.strip().split("\n")
            if item.strip() and item.strip() != "-"
        ]
        sections[tag.strip()] = items

    return sections


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY-INDEXED DOCUMENT SEARCH
# ══════════════════════════════════════════════════════════════════════════════


class DocumentIndex:
    """Summary-indexed document search across extracted documents.

    Workflow:
      1. Add documents (raw text or pre-extracted summaries)
      2. Generate summaries (if not pre-provided)
      3. Rank documents against a query using Haiku (cheap + fast)
      4. Extract relevant clauses from top-ranked docs using Sonnet

    Adapted from Anthropic Cookbook's LegalSummaryIndexedDocuments pattern.

    Usage:
        idx = DocumentIndex(client)
        idx.add_document("inv_001.pdf", raw_text, doc_type="INVOICE")
        idx.add_document("inv_002.pdf", raw_text, doc_type="INVOICE")
        idx.generate_summaries()

        ranked = idx.rank_documents("Which invoice has the highest total?")
        clauses = idx.extract_relevant_sections(ranked[0][0], "What is the total?")
    """

    def __init__(self, client, summary_model="claude-haiku-4-5-20251001"):
        self.client = client
        self.summary_model = summary_model
        self.documents: list[dict] = []
        self.summaries: list[str] = []

    def add_document(
        self, doc_id: str, content: str, doc_type: str = "DOCUMENT",
        pre_summary: Optional[str] = None,
    ):
        """Add a document. If pre_summary is provided, skip summary generation."""
        self.documents.append({
            "id": doc_id,
            "content": content,
            "doc_type": doc_type,
        })
        if pre_summary:
            self.summaries.append(pre_summary)

    def generate_summaries(self, max_content_chars: int = 3000):
        """Generate summaries for documents that don't have one yet."""
        while len(self.summaries) < len(self.documents):
            idx = len(self.summaries)
            doc = self.documents[idx]
            summary = self._summarize(
                doc["content"][:max_content_chars],
                doc["doc_type"],
            )
            self.summaries.append(summary)

    def _summarize(self, content: str, doc_type: str) -> str:
        prompt = (
            f"Summarize this {doc_type.replace('_', ' ').lower()} document in 3-5 bullet points. "
            f"Focus on: parties, dates, amounts, key terms.\n\n{content}"
        )
        response = self.client.messages.create(
            model=self.summary_model,
            max_tokens=500,
            temperature=0.2,
            messages=[
                {"role": "user", "content": prompt},
                {"role": "assistant", "content": "Summary:\n-"},
            ],
            stop_sequences=["\n\n\n"],
        )
        return "-" + response.content[0].text

    def rank_documents(
        self, query: str, top_k: int = 3,
    ) -> list[tuple[str, float]]:
        """Rank documents by relevance to a query. Returns [(doc_id, score)]."""
        if not self.summaries:
            self.generate_summaries()

        scores = []
        for i, summary in enumerate(self.summaries):
            prompt = (
                f"Document summary:\n{summary}\n\n"
                f"Query: {query}\n\n"
                f"Rate relevance 0-10. Output ONLY the number:"
            )
            response = self.client.messages.create(
                model=self.summary_model,
                max_tokens=3,
                temperature=0.0,
                messages=[{"role": "user", "content": prompt}],
            )
            try:
                score = float(response.content[0].text.strip())
            except (ValueError, TypeError):
                score = 0.0
            scores.append(score)

        ranked = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)
        return [
            (self.documents[i]["id"], scores[i])
            for i in ranked[:top_k]
        ]

    def extract_relevant_sections(
        self, doc_id: str, query: str,
        model: str = "claude-sonnet-4-20250514",
    ) -> list[str]:
        """Extract sections from a specific document that answer a query."""
        doc = next((d for d in self.documents if d["id"] == doc_id), None)
        if not doc:
            return [f"Document {doc_id} not found"]

        prompt = (
            f"Given this query and document, extract the most relevant "
            f"sections. Preserve original language. Separate sections with '---'.\n\n"
            f"Query: {query}\n\n"
            f"Document:\n{doc['content'][:8000]}"
        )

        response = self.client.messages.create(
            model=model,
            max_tokens=2000,
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}],
        )

        sections = re.split(r"\n\s*---\s*\n", response.content[0].text.strip())
        return [s.strip() for s in sections if s.strip()]

    @classmethod
    def from_snowflake(cls, session, db: str, client, doc_type: str = None, limit: int = 50):
        """Build an index from EXTRACTED_FIELDS data in Snowflake.

        Uses raw_extraction JSON as pre-computed summaries to avoid
        re-summarizing documents that have already been extracted.
        """
        where = f"AND r.doc_type = '{doc_type}'" if doc_type else ""
        rows = session.sql(f"""
            SELECT r.file_name, r.doc_type, e.raw_extraction::VARCHAR AS raw_json
            FROM {db}.RAW_DOCUMENTS r
            JOIN {db}.EXTRACTED_FIELDS e ON r.file_name = e.file_name
            WHERE e.raw_extraction IS NOT NULL {where}
            ORDER BY e.extracted_at DESC
            LIMIT {limit}
        """).collect()

        idx = cls(client)
        for row in rows:
            file_name = row["FILE_NAME"] if isinstance(row, dict) else row[0]
            doc_t = row["DOC_TYPE"] if isinstance(row, dict) else row[1]
            raw_json = row["RAW_JSON"] if isinstance(row, dict) else row[2]

            # Use the extraction JSON as a pre-computed summary
            try:
                raw = json.loads(raw_json)
                skip = {"_confidence", "_validation_warnings"}
                fields = {k: v for k, v in raw.items() if k not in skip}
                summary = "\n".join(f"- {k}: {v}" for k, v in fields.items() if v)
            except (json.JSONDecodeError, TypeError):
                summary = raw_json[:500] if raw_json else ""

            idx.add_document(
                doc_id=str(file_name),
                content=raw_json or "",
                doc_type=str(doc_t),
                pre_summary=summary,
            )

        return idx
