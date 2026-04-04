"""
Patch agent.py:
  1. Fix update_expected_field — detect 'name' as a word (handles Tamil/mixed responses)
  2. Fix _clean_name — strip email/digits bleeding into name (same as server.py)
  3. Fix consume_caller_text — allow name overwrite + guard bad use_case (same as server.py)
  4. Fix prompt — make save_lead tool call mandatory (model was saying "I'll log" without calling it)
  5. Fix greeting trigger — still said "Maya", must say "Jake"
"""

content = open("agent.py", encoding="utf-8").read()

# ── 1. Fix update_expected_field ─────────────────────────────────────────────
OLD_UPDATE = '''    def update_expected_field(self, agent_text: str):
        text = agent_text.lower()
        if any(phrase in text for phrase in ["full name", "your name", "who am i speaking with", "who's this"]):
            self.expected_field = "name"
        elif any(phrase in text for phrase in ["phone number", "best number", "reach you at"]):
            self.expected_field = "phone"
        elif "email" in text:
            self.expected_field = "email"
        elif any(phrase in text for phrase in ["use case", "what do you want", "what would you like", "what should the voice agent do"]):
            self.expected_field = "use_case"'''

NEW_UPDATE = '''    def update_expected_field(self, agent_text: str):
        text = agent_text.lower()
        # Word-boundary match for "name" catches Tamil/mixed responses like "unga name sollunga"
        name_phrases = [
            "full name", "your name", "who am i speaking with", "who's this",
            "can i grab your name", "may i have your name", "what's your name",
            "first name", "get your name",
        ]
        if any(phrase in text for phrase in name_phrases) or re.search(r"\\bname\\b", text):
            self.expected_field = "name"
        elif any(phrase in text for phrase in [
            "phone number", "best number", "reach you at", "contact number",
            "phone", "number sollunga",
        ]):
            self.expected_field = "phone"
        elif "email" in text:
            self.expected_field = "email"
        elif any(phrase in text for phrase in [
            "use case", "what do you want", "what would you like",
            "what should the voice agent do",
        ]):
            self.expected_field = "use_case"'''

assert OLD_UPDATE in content, "update_expected_field anchor not found"
content = content.replace(OLD_UPDATE, NEW_UPDATE, 1)

# ── 2. Fix _clean_name — stop at email, strip digits ─────────────────────────
OLD_CLEAN_NAME = '''def _clean_name(text: str) -> str:
    cleaned = re.sub(
        r"^(my name is|this is|i am|i'm|im|it is|it's)\\s+",
        "",
        text.strip(),
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"[^\\w\\s'.-]", "", cleaned)
    return _normalize_spaces(cleaned)'''

NEW_CLEAN_NAME = '''def _clean_name(text: str) -> str:
    cleaned = re.sub(
        r"^(my name is|this is|i am|i'm|im|it is|it's|hi i'm|hello i'm|hey i'm)\\s+",
        "",
        text.strip(),
        flags=re.IGNORECASE,
    )
    # Stop before email address so it doesn't bleed into the name
    cleaned = re.split(r"\\s*@|\\s+at\\s+\\w+\\.\\w+", cleaned, maxsplit=1)[0]
    # Strip digits — names don't have numbers
    cleaned = re.sub(r"\\d+", "", cleaned)
    cleaned = re.sub(r"[^\\w\\s'.-]", "", cleaned)
    return _normalize_spaces(cleaned)'''

assert OLD_CLEAN_NAME in content, "_clean_name anchor not found"
content = content.replace(OLD_CLEAN_NAME, NEW_CLEAN_NAME, 1)

# ── 3. Add _is_greeting_or_question helper + fix consume_caller_text ─────────
OLD_CLEAN_USE = '''def _clean_use_case(text: str) -> str:
    cleaned = re.sub(
        r"^(we need|i need|we want|i want|it's for|it is for|we are looking for)\\s+",
        "",
        text.strip(),
        flags=re.IGNORECASE,
    )
    return _normalize_spaces(cleaned)'''

NEW_CLEAN_USE = '''_GREETING_RE = re.compile(
    r"^(hi\\b|hello\\b|hey\\b|yeah\\s+(hi|hello|okay)|good\\s+(morning|afternoon|evening)|how\\s+are\\s+you)",
    re.IGNORECASE,
)


def _is_greeting_or_question(text: str) -> bool:
    if _GREETING_RE.match(text):
        return True
    if text.endswith("?") and len(text.split()) <= 15:
        return True
    return False


def _clean_use_case(text: str) -> str:
    cleaned = re.sub(
        r"^(we need|i need|we want|i want|it's for|it is for|we are looking for)\\s+",
        "",
        text.strip(),
        flags=re.IGNORECASE,
    )
    return _normalize_spaces(cleaned)'''

assert OLD_CLEAN_USE in content, "_clean_use_case anchor not found"
content = content.replace(OLD_CLEAN_USE, NEW_CLEAN_USE, 1)

OLD_CONSUME = '''    def consume_caller_text(self, caller_text: str):
        text = _normalize_spaces(caller_text)
        if not text:
            return

        email = _extract_email(text)
        phone = _extract_phone(text)
        if email and not self.email:
            self.email = email
        if phone and not self.phone:
            self.phone = phone

        if self.expected_field == "name" and not self.name:
            self.name = _clean_name(text)
        elif self.expected_field == "phone" and not self.phone and phone:
            self.phone = phone
        elif self.expected_field == "email" and not self.email and email:
            self.email = email
        elif self.expected_field == "use_case" and not self.use_case:
            self.use_case = _clean_use_case(text)'''

NEW_CONSUME = '''    def consume_caller_text(self, caller_text: str):
        text = _normalize_spaces(caller_text)
        if not text:
            return

        email = _extract_email(text)
        phone = _extract_phone(text)
        if email and not self.email:
            self.email = email
        if phone and not self.phone:
            self.phone = phone

        if self.expected_field == "name":
            candidate = _clean_name(text)
            # Always overwrite so a later accurate utterance replaces an earlier bad one
            if candidate and len(candidate) >= 2 and re.search(r"[a-zA-Z]", candidate):
                self.name = candidate
        elif self.expected_field == "phone" and not self.phone and phone:
            self.phone = phone
        elif self.expected_field == "email" and not self.email and email:
            self.email = email
        elif self.expected_field == "use_case" and not self.use_case:
            cleaned = _clean_use_case(text)
            # Don't store a greeting or short question as the use-case
            if cleaned and not _is_greeting_or_question(cleaned):
                self.use_case = cleaned'''

assert OLD_CONSUME in content, "consume_caller_text anchor not found"
content = content.replace(OLD_CONSUME, NEW_CONSUME, 1)

# ── 4. Fix prompt — make save_lead call mandatory ─────────────────────────────
OLD_WEBHOOK = '''CRITICAL WEBHOOK FLOW
- Once ALL 4 are collected (name, phone, email, use_case):

STEP 1 — SAY THIS FIRST (VERY IMPORTANT, NATURAL):
"Give me a minute — I'll just log your details so my sales team can reach out to you."

STEP 2 — CALL FUNCTION:
Call save_lead with all collected details.

- Do NOT say "webhook" or "tool"

STEP 3 — AFTER SUCCESS:
"Perfect, you're all set. My sales team will get in touch with you soon."

- Keep it short and natural.

ERROR HANDLING
- If tool fails:
"I'm sorry about that — let me try that again."'''

NEW_WEBHOOK = '''CRITICAL WEBHOOK FLOW
- Once ALL 4 are collected (name, phone, email, use_case):

STEP 1 — SAY THIS FIRST:
"Give me a minute — I'll just log your details so my sales team can reach out to you."

STEP 2 — YOU MUST CALL THE save_lead FUNCTION NOW. THIS IS MANDATORY.
Do NOT skip this. Do NOT say you saved it without actually calling the function.
Call save_lead with: name, phone, email, use_case.

STEP 3 — ONLY AFTER the function returns success, say:
"Perfect, you're all set. My sales team will get in touch with you within 24 hours."

- Do NOT say "webhook" or "tool" to the caller.
- Do NOT proceed to step 3 without completing step 2.

ERROR HANDLING
- If tool fails:
"I'm sorry about that — let me try that again." then retry once.'''

assert OLD_WEBHOOK in content, "webhook section anchor not found"
content = content.replace(OLD_WEBHOOK, NEW_WEBHOOK, 1)

# ── 5. Fix greeting trigger (still said Maya) ─────────────────────────────────
OLD_GREET = "The call has connected. Greet the caller now as Jake from TrueAI Lab."
if OLD_GREET not in content:
    # try old Maya version
    OLD_GREET = "The call has connected. Greet the caller now as Maya from TrueAILab."
assert OLD_GREET in content, "greeting trigger not found"
content = content.replace(OLD_GREET, "The call has connected. Greet the caller now as Jake from TrueAI Lab.", 1)

open("agent.py", "w", encoding="utf-8").write(content)
print("agent.py patched successfully")
