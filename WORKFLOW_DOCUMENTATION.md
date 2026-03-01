# Fyl.la Database Manager — Logic & Workflow Documentation

> Complete technical documentation of the end-to-end data pipeline: from raw input ingestion through selection, cleaning, normalization, deduplication, human review, and parameterization.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture & Data Stores](#2-architecture--data-stores)
3. [Phase 1 — Ingestion & Pre-Cleaning](#3-phase-1--ingestion--pre-cleaning)
4. [Phase 2 — Selection (Random Sampling)](#4-phase-2--selection-random-sampling)
5. [Phase 3 — LLM Cleaning](#5-phase-3--llm-cleaning)
6. [Phase 4 — Post-Clean Processing](#6-phase-4--post-clean-processing)
7. [Phase 5 — Normalization](#7-phase-5--normalization)
8. [Phase 6 — Deduplication](#8-phase-6--deduplication)
9. [Phase 7 — Human Review (Selection Tab)](#9-phase-7--human-review-selection-tab)
10. [Phase 8 — Parameterization](#10-phase-8--parameterization)
11. [Phase 9 — Runtime Placeholder Fill](#11-phase-9--runtime-placeholder-fill)
12. [Concurrency Control](#12-concurrency-control)
13. [Complete Pipeline Flow Diagram](#13-complete-pipeline-flow-diagram)

---

## 1. System Overview

The Fyl.la Database Manager is a Streamlit-based application that manages a database of Norwegian drinking-game prompts. The system implements an **eight-phase data pipeline** that transforms raw, unstructured text into cleaned, deduplicated, human-reviewed, and parametrically classified database entries.

**Core principle:** Every piece of text enters the system as raw, untrusted input and must pass through a deterministic sequence of transformations and gates before it can be considered a valid database entry.

### Technology Stack

| Component | Technology |
|---|---|
| Frontend | Streamlit (Python) |
| Backend Storage | Google Cloud Storage (GCS) — JSON files |
| LLM Provider | xAI Grok (`grok-4-fast-reasoning`) via OpenAI SDK |
| Concurrency Model | Async/await with `nest_asyncio` bridge |
| Concurrency Control | Optimistic locking via GCS `if_generation_match` |

---

## 2. Architecture & Data Stores

### Persistent Data Stores (GCS)

The system operates on five distinct files stored in Google Cloud Storage:

| File | Format | Purpose |
|---|---|---|
| `raw_stripped.txt` | Plain text (one line per entry) | Pool of unprocessed raw prompts awaiting ingestion |
| `USER_SELECTION.json` | JSON array of `{prompt}` | Queue of LLM-cleaned items awaiting human review |
| `DATABASE.json` | JSON array of `{prompt, occurrences, craziness?, isSexual?, madeFor?}` | The canonical, approved prompt database |
| `DISCARDS.json` | JSON array of `{prompt, occurrences}` | Rejected prompts (kept for dedup reference) |
| `REMOVE_LINES.txt` | Plain text (one word/phrase per line) | Blocklist of filter terms applied during ingestion |

### Data Model

The core data model is the `Item` dataclass (`models.py`):

```python
@dataclass
class Item:
    raw: str                    # Original unprocessed string (transient)
    prompt: Optional[str] = None  # Cleaned string from LLM (persisted)
```

Once persisted in `DATABASE.json`, each entry conforms to:

```json
{
  "prompt": "string",          // Required — the cleaned prompt text
  "occurrences": 1,            // Required — how many times this prompt was encountered
  "craziness": 2,              // Optional — intensity rating 1-4 (set by parameterization)
  "isSexual": false,           // Optional — sexual content flag (set by parameterization)
  "madeFor": "boys"            // Optional — gender-specific targeting (set by parameterization)
}
```

### Store Classes (`database.py`)

The database layer is organized into three store classes, unified by a facade:

- **`UserSelectionStore`** — Manages `USER_SELECTION.json` (the human review queue)
- **`DiscardedItemsStore`** — Manages `DISCARDS.json` (the rejection archive)
- **`GlobalDatabaseStore`** — Manages `DATABASE.json` (the canonical database) with an in-memory dedup cache
- **`DatabaseManager`** — Facade that coordinates operations across all three stores

---

## 3. Phase 1 — Ingestion & Pre-Cleaning

**Entry point:** `ui/pages/input_tab.py` → `InputTab._handle_file_upload()`

**Purpose:** Accept raw `.txt` file uploads, strip metadata noise, deduplicate against existing content, apply blocklist filters, and store the cleaned lines in the GCS raw pool.

### Step 1.1 — File Upload & Temporary Storage

The user uploads a `.txt` file through the Streamlit file uploader. The file content is decoded from UTF-8 and written to a temporary file on the server filesystem.

```
User uploads .txt → decode UTF-8 → write to temp file
```

### Step 1.2 — Text Stripping (`text_utils.strip_file()`)

**Source:** `text_utils.py`, lines 55–77

The raw file is passed through `strip_file()`, which applies three rejection filters to each line:

1. **Minimum length gate:** Lines shorter than 6 characters are discarded (eliminates empty lines, single-word fragments, and noise).
2. **Date detection (`is_date()`):** Lines containing any recognizable date/time pattern are discarded. This removes chat export metadata. Supported patterns include:
   - ISO-8601 (`2024-10-19T19:34:10+0000`)
   - RFC-1123 (`Sat, 19 Oct 2024 19:34:10 +0000`)
   - JavaScript `Date.toString()` format
   - Numeric dates with slashes/dots (multiple orderings)
   - Month-name formats in both English and Norwegian
3. **Name-line detection (`is_navn_line()`):** Lines starting with `Navn:` (case-insensitive) are discarded. This removes participant header lines from chat exports.

**Output:** A `_stripped.txt` file containing only valid content lines.

### Step 1.3 — Line-Level Deduplication Against Existing Pool

**Source:** `ui/pages/input_tab.py`, lines 106–113

Before appending to the raw pool, the system:

1. Downloads the current `raw_stripped.txt` from GCS
2. Builds a `set` of all existing lines (stripped, exact-match)
3. Filters the new lines, keeping only those **not already present** in the existing set

```python
existing_lines = set(line.strip() for line in current_content.split('\n') if line.strip())
unique_new_lines = [line for line in new_lines_list if line not in existing_lines]
```

This is an **exact-match dedup** (case-sensitive, whitespace-stripped). It prevents the same raw line from entering the pool twice but does not catch semantic duplicates — that is handled later by the normalization-based dedup.

### Step 1.4 — Blocklist Filtering

**Source:** `ui/pages/input_tab.py`, lines 264–313

Immediately after appending new content, the system applies the `REMOVE_LINES.txt` blocklist. For each line in `raw_stripped.txt`:

- Each blocklist term is checked as a **case-insensitive substring match**
- If any blocklist term appears anywhere in the line, the line is removed
- This runs automatically after every upload and can also be triggered manually

### Step 1.5 — Upload to GCS

The surviving lines are appended to `raw_stripped.txt` on GCS using optimistic concurrency (generation-based preconditions). The temporary files are cleaned up.

**Result:** `raw_stripped.txt` now contains new, unique, filtered raw prompts ready for the processing pipeline.

---

## 4. Phase 2 — Selection (Random Sampling)

**Entry point:** `workflow.py` → `Workflow._select_and_remove_items()`

**Purpose:** Randomly sample a batch of items from the raw pool and atomically remove them from the source to prevent reprocessing.

### Trigger Mechanism

Selection is triggered by the `SelectionService.auto_populate_user_selection_if_needed()` method (`ui/services/data_service.py`, lines 108–144):

1. The system checks the current count of items in `USER_SELECTION.json`
2. If the count falls below the **populate threshold** (20 items), auto-population is triggered
3. The target queue size is 50 items; the system calculates `items_needed = 50 - current_count`
4. A `Workflow` instance is created with the calculated batch size

### Selection Algorithm

**Source:** `workflow.py`, lines 62–93

```
1. Download raw_stripped.txt from GCS (with generation number for concurrency)
2. Parse into non-empty lines
3. IF available lines < requested count:
       Select ALL remaining lines (drain the pool)
   ELSE:
       Use random.sample(items, X) for uniform random sampling without replacement
4. Compute remaining lines = original - selected
5. Upload the reduced content back to GCS (with generation precondition)
6. Return the selected items
```

**Key design decisions:**

- **Atomic read-modify-write:** The selection and removal happen in a single logical operation protected by optimistic concurrency. This prevents two concurrent workflows from selecting the same items.
- **Random sampling:** `random.sample()` provides uniform selection without replacement, ensuring no bias toward items at any particular position in the file.
- **Destructive read:** Selected items are removed from the source, guaranteeing they will not be processed again in a future batch.

---

## 5. Phase 3 — LLM Cleaning

**Entry point:** `workflow.py` → `Workflow._process_single_item()` → `llm.call_llm()`

**Purpose:** Transform raw, potentially noisy user-submitted text into a standardized, anonymized prompt suitable for a game database.

### LLM Configuration

| Parameter | Value |
|---|---|
| Provider | xAI Grok |
| Model | `grok-4-fast-reasoning` |
| Temperature | 0.0 (deterministic output) |
| Max Tokens | 1000 |
| Retry Strategy | Exponential backoff, up to 3 retries |

### System Prompt Rules (`prompts/clean.prompt`)

The LLM operates under the `CleanBot` persona with four primary directives:

1. **Typo correction:** Fix obvious keyboard slips, repeated characters, and misspellings while preserving slang and original tone.

2. **Name anonymization:** Detect personal names (first names, last names) and replace each with the placeholder `[PLAYER]`. Celebrity and public figure names are explicitly preserved (for "Fuck marry kill"-style prompts).

3. **Drink reference replacement:** Detect drink-related phrases (e.g., "drikk 3 slurker", "chug") and replace with `[DRINKS]`. Numerical amounts are absorbed into the placeholder. References to drink types as game variables are excluded.

4. **Language gate:** If the entire prompt is NOT in Norwegian, return `[OTHER]` to flag it for removal.

### Examples from the System Prompt

| Input | Output |
|---|---|
| `Markus kan dele ut 5 hvis han liker å danse 💃` | `[PLAYER] kan dele ut [DRINKS] hvis han liker å danse 💃` |
| `Pkelek, den som får flest pek drikker 5.` | `Pekelek, den som får flest pek [DRINKS].` |
| `hatt klamma?` | `hatt klamma?` (already clean) |

### Retry Logic (`llm.py`, lines 47–83)

The LLM call implements tiered retry with exponential backoff:

- **Rate limit errors (429):** Wait `2^attempt × 60` seconds (1min, 2min, 4min)
- **Other errors:** Wait `2^attempt × 10` seconds (10s, 20s, 40s)
- **No fallback:** If all retries are exhausted, the item fails entirely — there is no "return original text" fallback, ensuring only LLM-verified content enters the pipeline

### Concurrency

All items in a batch are processed concurrently via `asyncio.gather()`:

```python
results = await asyncio.gather(*[self._process_single_item(item) for item in item_objs])
```

This maximizes throughput by parallelizing LLM API calls across the batch.

---

## 6. Phase 4 — Post-Clean Processing

**Entry point:** `workflow.py` → `Workflow._process_single_item()`, lines 117–130

**Purpose:** Sanitize the LLM output to enforce strict single-line format constraints before further processing.

### Step 4.1 — Whitespace Stripping

```python
cleaned = cleaned.strip()
```

Remove leading and trailing whitespace from the LLM response.

### Step 4.2 — Empty Result Gate

```python
if not cleaned:
    return {"success": False, "message": "LLM returned empty result"}
```

If the LLM returned an empty or whitespace-only string, the item is marked as failed.

### Step 4.3 — Multi-Line Collapse

```python
if "\n" in cleaned:
    cleaned = cleaned.splitlines()[0].strip()
```

If the LLM erroneously returned multiple lines, only the first line is kept. This enforces the "one line in, one line out" contract.

### Step 4.4 — Quote Stripping

```python
if (cleaned.startswith('"') and cleaned.endswith('"')) or \
   (cleaned.startswith("'") and cleaned.endswith("'")):
    cleaned = cleaned[1:-1].strip()
```

If the LLM wrapped its output in matching quotes (a common LLM artifact), the quotes are stripped. Only simple matching pairs are handled — nested or mismatched quotes are left intact.

### Step 4.5 — Assignment

```python
item.prompt = cleaned
```

The cleaned text is assigned to the `Item.prompt` field, graduating it from raw to cleaned status.

---

## 7. Phase 5 — Normalization

**Entry point:** `workflow.py`, lines 132–135; `text_utils.py`, lines 81–126

**Purpose:** Transform cleaned text into a canonical form suitable for equality-based deduplication.

### Two-Stage Normalization Architecture

Normalization is deliberately split into two composable functions:

#### Stage 5a — Character Normalization (`text_utils.normalize()`)

```python
def normalize(text: str) -> str:
    return re.sub(r'[^\w\s]', '', text)
```

**Logic:** Remove ALL characters that are not word characters (`\w` = `[a-zA-Z0-9_]`) or whitespace (`\s`).

**Critical design insight:** This deliberately strips brackets from `[PLAYER]` and `[DRINKS]`, turning them into bare words `PLAYER` and `DRINKS`. These bare words are then removed in the next stage. This means all placeholder variants — `[PLAYER]`, `[DRINKS]`, `Player`, `Drinks` — become invisible to deduplication, which is the correct behavior for a drinking game database where placeholder usage varies.

**Validation gate in `workflow.py`:**

```python
normalized = normalize(cleaned).strip()
if not normalized:
    return {"success": False, "message": "Normalization resulted in empty string"}
```

If normalization produces an empty string (meaning the cleaned text contained only special characters), the item is rejected.

#### Stage 5b — Dedup Key Construction (`text_utils.build_dedup_key()`)

```python
def build_dedup_key(normalized_text: str) -> str:
    text = (normalized_text or '').strip()
    if not text:
        return ''
    # Remove 'player' and 'drinks' as whole words
    text = re.sub(r"\b(?:player|drinks)\b", " ", text, flags=re.IGNORECASE)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Lowercase
    return text.lower()
```

**Three transformations applied sequentially:**

1. **Filler word removal:** The whole words `player` and `drinks` (case-insensitive) are replaced with spaces. Since `normalize()` already stripped brackets, this catches both `[PLAYER]` → `PLAYER` → removed, and the literal word "player" in any case.

2. **Whitespace collapse:** Multiple consecutive whitespace characters are collapsed into a single space, and leading/trailing whitespace is trimmed. This prevents false dedup misses from inconsistent spacing.

3. **Lowercasing:** The entire string is lowercased for case-insensitive comparison.

### Composite Helper (`database.py`)

```python
def _prompt_dedup_key(prompt: str) -> str:
    return build_dedup_key(normalize(str(prompt or '')))
```

The `_prompt_dedup_key()` function chains both stages into a single call, used consistently across all three data stores.

### Example Normalization Chain

```
Raw:      "Markus kan dele ut 5 hvis han liker å danse 💃"
Cleaned:  "[PLAYER] kan dele ut [DRINKS] hvis han liker å danse 💃"
Stage 5a: "PLAYER kan dele ut DRINKS hvis han liker å danse "
Stage 5b: " kan dele ut  hvis han liker å danse " → "kan dele ut hvis han liker å danse"
```

---

## 8. Phase 6 — Deduplication

**Entry point:** `workflow.py`, lines 137–156; `database.py`

**Purpose:** Prevent duplicate prompts from entering the review queue by checking the normalized dedup key against all three data stores.

### Three-Store Cross-Check

**Source:** `database.py` → `DatabaseManager.exists_in_database()`, lines 551–556

```python
async def exists_in_database(self, prompt: str) -> bool:
    exists_in_global = await self.globalStore.exists_in_database(prompt)
    exists_in_discards = await self.discardsStore.exists_in_discards(prompt)
    exists_in_user_selection = await self.userSelection.exists_in_user_selection(prompt)
    return exists_in_global or exists_in_discards or exists_in_user_selection
```

The dedup check spans **all three stores**, meaning:

- An item already in the approved database will not be re-queued
- An item previously discarded by a human reviewer will not be re-queued
- An item already pending review will not be duplicated in the queue

### Dedup Mechanisms Per Store

#### GlobalDatabaseStore — In-Memory Cache

The global database maintains an in-memory `Set[str]` of dedup keys (`_dedupCache`), refreshed from GCS on every check:

```python
async def _refresh_cache(self) -> None:
    data, generation = downloadJson(...)
    self._dedupCache = set()
    for item in data:
        key = _prompt_dedup_key(item.get('prompt', ''))
        if key:
            self._dedupCache.add(key)
```

Existence check is an O(1) set membership test:

```python
async def exists_in_database(self, prompt: str) -> bool:
    await self._refresh_cache()
    candidateKey = _prompt_dedup_key(prompt)
    return candidateKey in self._dedupCache
```

#### UserSelectionStore & DiscardedItemsStore — Linear Scan

Both stores perform a linear scan of their JSON arrays, computing the dedup key for each existing entry and comparing:

```python
for existing in data:
    existingPrompt = str(existing.get('prompt') or '')
    if _prompt_dedup_key(existingPrompt) == candidateKey:
        return True
```

### Duplicate Handling

When a duplicate is detected, the system does NOT silently drop it. Instead, it **increments the occurrence counter** on the existing entry:

```python
if not exists:
    await self.db_manager.add_to_user_selection(item.to_dict())
else:
    await self.db_manager.increment_occurrence_count(item.prompt)
```

The `increment_occurrence_count()` method checks the global database first, then falls back to discards:

```python
async def increment_occurrence_count(self, prompt: str) -> None:
    exists_in_global = await self.globalStore.exists_in_database(prompt)
    if exists_in_global:
        await self.globalStore.increment_database_item_occurrences(prompt)
    else:
        exists_in_discards = await self.discardsStore.exists_in_discards(prompt)
        if exists_in_discards:
            await self.discardsStore.increment_discarded_item_occurrences(prompt)
```

This preserves frequency data, which is valuable for understanding prompt popularity.

### Insert-Time Dedup Guard

Even when adding to a store, a secondary dedup check is performed. For example, `UserSelectionStore.add_to_user_selection()`:

```python
candidateKey = _prompt_dedup_key(promptVal)
for existing in data:
    if _prompt_dedup_key(existing.get('prompt', '')) == candidateKey:
        return  # Silently skip — already present
data.append({'prompt': promptVal})
```

This double-check guards against race conditions where two concurrent workflow executions might both pass the initial dedup check before either writes.

---

## 9. Phase 7 — Human Review (Selection Tab)

**Entry point:** `ui/pages/selection_tab.py` → `SelectionTab`

**Purpose:** Present LLM-cleaned, deduplicated items to a human reviewer in batches for binary accept/reject decisions.

### Queue Management

Items flow through `USER_SELECTION.json` as a FIFO queue:

- **Enqueue:** Items are appended to the end by the workflow pipeline (Phase 6)
- **Dequeue:** Items are popped from the front for batch review via `pop_user_selection_item()`

### Batch Review Process

**Source:** `ui/services/data_service.py` → `SelectionService.fetch_batch_items()`

1. **Queue level check:** The system checks the current queue count
2. **Auto-population trigger:** If the queue falls below the populate threshold (20), the ingestion workflow is automatically triggered to refill toward the target (50)
3. **Batch pop:** Up to 5 items are popped from the front of the queue

```
Queue count < 20? → Trigger Workflow (Phase 2–6) → Refill to ~50
Pop 5 items from front of queue → Present to reviewer
```

### Review Interface

Each item is presented with:
- The cleaned prompt text
- A "Discard" checkbox (unchecked by default — items are kept unless explicitly discarded)

### Decision Routing

**Source:** `ui/services/data_service.py` → `SelectionService.process_batch_items()`

When the user clicks "Fetch Next Batch":

1. **Current batch is processed:**
   - Items **NOT marked as discard** → Added to `DATABASE.json` via `add_to_global_database()` with `occurrences: 1`
   - Items **marked as discard** → Added to `DISCARDS.json` via `add_to_discards()` with `occurrences: 1`
2. **Next batch is fetched** (repeats the batch review process)

```python
for i, item in enumerate(items):
    if discard_key not in discard_actions:
        # KEEP → Global Database
        run_async(db.add_to_global_database({"prompt": prompt_val, "occurrences": 1}))
    else:
        # DISCARD → Discards Store
        run_async(db.add_to_discards({"prompt": prompt_val, "occurrences": 1}))
```

### Manual Input Bypass

The Selection Tab also provides a manual input field that bypasses the entire pipeline:

```
Manual text → strip() → add_to_global_database({prompt, occurrences: 1})
```

This allows administrators to add known-good prompts directly to the database without going through LLM cleaning or the review queue. Note: the global database's `add_to_database()` method still performs dedup on insert, so manual entries won't create duplicates.

---

## 10. Phase 8 — Parameterization

**Entry point:** `llm_parameterization.py` → `ParameterizationWorkflow`; triggered from `ui/pages/database_tab.py`

**Purpose:** Classify approved database entries with parametric metadata (craziness level, sexual content flag, gender targeting) using LLM inference.

### Parameterization Trigger

The parameterization workflow runs as a **subprocess** invoked from the Database Tab UI:

```python
result = subprocess.run(
    [sys.executable, "llm_parameterization.py", str(num_items)],
    capture_output=True, text=True, cwd=".",
)
```

Running as a subprocess isolates the LLM workload from the Streamlit event loop.

### Step 8.1 — Load Database Entries

```python
data, _ = downloadJson(client, self.bucket_name, self.database_object)
```

The entire `DATABASE.json` is downloaded from GCS.

### Step 8.2 — Filter Unparameterized Entries

```python
def _filter_unparameterized(self, database_entries):
    available = []
    for entry in database_entries:
        prompt = entry.get("prompt", "").strip()
        if prompt and "craziness" not in entry:
            available.append(entry)
    return available
```

**Logic:** An entry is considered "unparameterized" if and only if it lacks the `craziness` field. This is the single field used as the parameterization-status sentinel.

### Step 8.3 — Random Selection

```python
def _select_random_items(self, available_items, num_items):
    if len(available_items) <= num_items:
        return available_items
    return random.sample(available_items, num_items)
```

A random subset of unparameterized entries is selected for processing. This allows parameterization to run incrementally over multiple sessions.

### Step 8.4 — LLM Classification

**Source:** `llm_parameterization.py` → `ParameterizationLLM.parameterize()`

Each selected prompt is sent to the xAI Grok LLM with the classification system prompt (`prompts/parameterize.prompt`):

**Classification schema:**

| Field | Type | Values | Required |
|---|---|---|---|
| `prompt` | string | Exact copy of input | Yes |
| `craziness` | integer | 1 (basic) to 4 (highest intensity) | Yes |
| `isSexual` | boolean | true/false | Yes |
| `madeFor` | string | `"boys"` or `"girls"` | No |

**Craziness scale definitions:**

- **1 — Basic:** Normal drinking game content
- **2 — Standard:** Low-pressure challenges, group questions (e.g., "Never Have I Ever"), light personal sharing
- **3 — Higher stakes:** More personal sharing, higher-pressure challenges
- **4 — Highest:** Very personal sharing or physical/creative tasks; uncomfortable but safe

**Retry strategy with temperature escalation:**

```python
for attempt in range(max_retries + 1):
    temp = 0.0 if attempt == 0 else min(0.1 + (attempt * 0.1), 0.3)
```

- Attempt 1: Temperature 0.0 (deterministic)
- Attempt 2: Temperature 0.2
- Attempt 3: Temperature 0.3
- Attempt 4: Temperature 0.3 (capped)

Higher temperature on retries increases variance, potentially breaking out of failure patterns.

### Step 8.5 — Response Validation

**Source:** `llm_parameterization.py` → `ParameterizationLLM._validate_json_schema()`

The LLM response is validated against a strict schema:

1. Must be a dictionary (not array, string, etc.)
2. Must contain required fields: `prompt`, `craziness`, `isSexual`
3. `prompt` must be a non-empty string
4. `craziness` must be an integer in range [1, 4]
5. `isSexual` must be a boolean
6. `madeFor`, if present, must be `"boys"` or `"girls"`
7. No additional/unexpected fields are allowed

### Step 8.6 — Partial JSON Recovery

**Source:** `llm_parameterization.py` → `ParameterizationLLM._try_recover_partial_json()`

If the LLM response is truncated (starts with `{` but doesn't end with `}`), the system attempts recovery:

1. **Extract craziness from partial:** If only `craziness` was parsed, construct a minimal valid response with `isSexual: false`
2. **Close truncated JSON:** If the brace count is unbalanced, try appending `}`
3. **Add missing required fields:** If `isSexual` is missing but other fields are present, append `"isSexual": false` and close

### Step 8.7 — Incremental Database Update

**Source:** `llm_parameterization.py` → `ParameterizationWorkflow._apply_updates_to_database()`

Updates are batched and written every 5 items (plus a final flush):

```python
if len(pending_updates) % 5 == 0:
    await self._apply_updates_to_database(pending_updates)
    pending_updates = []
```

The update process:

1. Download `DATABASE.json` with current generation
2. Build a lookup map from pending updates keyed by prompt text
3. For each database entry whose prompt matches an update, merge the parametric fields (`craziness`, `isSexual`, `madeFor`)
4. Upload with optimistic concurrency precondition
5. Retry with exponential backoff on conflict

### Inline Parametric Editing

The Database Tab also supports inline editing of parametric fields via the Streamlit data editor. When a user modifies a cell:

```
Detect changed cells → Build update dict → downloadJson → apply changes → uploadJsonWithPreconditions
```

This allows human override of LLM classification when the automated result is incorrect.

---

## 11. Phase 9 — Runtime Placeholder Fill

**Configuration:** `prompts/master.prompt`, `prompts/vibe_map.json`, `prompts/drinking_map.json`, `prompts/parametrics_selection_probability_map.json`

**Purpose:** At game runtime, transform database entries with placeholders into fully realized game prompts by filling `[PLAYER]` and `[DRINKS]` based on game parameters.

### Prompt Selection by Craziness

The `parametrics_selection_probability_map.json` defines a probability distribution for selecting prompts based on the game's craziness setting:

```json
{
  "1": [75, 25, 0, 0],
  "2": [25, 50, 25, 0],
  "3": [0, 25, 60, 10],
  "4": [0, 15, 65, 20]
}
```

**Interpretation:** At game craziness level 2, there is a 25% chance of selecting a level-1 prompt, 50% for level-2, 25% for level-3, and 0% for level-4. This creates a weighted distribution that biases toward the selected difficulty while maintaining variety.

### Placeholder Replacement

The `master.prompt` template instructs the LLM to:

1. Replace `[PLAYER]` with actual player names from the provided player list
2. Replace `[DRINKS]` with appropriate drink amounts based on the drinking level context
3. Output everything in Norwegian
4. Return one finished line per base prompt with no numbering or extra text

### Runtime Parameters

| Parameter | Source |
|---|---|
| `vibe` | Game vibe setting → maps to context via `vibe_map.json` |
| `drinking_level` | Drinking intensity → maps to context via `drinking_map.json` |
| `players_csv` | Comma-separated list of player names |
| `free_text` | Custom user instructions |

---

## 12. Concurrency Control

### Optimistic Concurrency via GCS Generations

Every GCS object has a `generation` number that increments on each write. The system uses this for **optimistic locking**:

```python
def uploadJsonWithPreconditions(client, bucketName, objectName, data, ifGenerationMatch):
    blob.upload_from_string(
        jsonText,
        content_type="application/json",
        if_generation_match=ifGenerationMatch,
    )
```

**Behavior:** If another process modified the object between our read and write, the generation will have changed and the upload fails with a precondition error.

### Retry with Exponential Backoff

All write operations implement retry logic:

```python
attempt = 0
backoffSeconds = 0.2
while True:
    try:
        # read → modify → write with precondition
        return
    except Exception:
        attempt += 1
        if attempt > maxRetries:
            raise
        await asyncio.sleep(backoffSeconds)
        backoffSeconds = min(backoffSeconds * 2, 2.0)
        # Re-read to get fresh generation
```

**Backoff schedule:** 0.2s → 0.4s → 0.8s → 1.6s → 2.0s (capped)

### Async Locks

The `UserSelectionStore` and `DiscardedItemsStore` use `asyncio.Lock()` to serialize concurrent operations within the same process:

```python
async with self._lock:
    data = await self._load_json()
    # ... modify data ...
    await self._save_json(data)
```

This prevents intra-process race conditions where multiple coroutines might try to modify the same store simultaneously.

### Streamlit Async Bridge

Since Streamlit runs in a synchronous context but the database layer is async, a bridge function handles event loop management:

```python
def run_async(coro):
    try:
        loop = asyncio.get_running_loop()
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)
```

---

## 13. Complete Pipeline Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PHASE 1: INGESTION & PRE-CLEANING                      │
│                                                                                 │
│  .txt Upload → strip_file() ─┬─ Length < 6?    → REJECT                        │
│                               ├─ Is date?       → REJECT                        │
│                               ├─ Starts "Navn:" → REJECT                        │
│                               └─ Valid line      → Dedup against existing pool   │
│                                                         │                       │
│                                    Exact-match dedup ───┤                       │
│                                                         │                       │
│                               Apply REMOVE_LINES.txt ───┤                       │
│                                                         ▼                       │
│                                              raw_stripped.txt (GCS)              │
└─────────────────────────────────────────────────────────────────────────────────┘
                                                          │
                          ┌───────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PHASE 2: SELECTION                                     │
│                                                                                 │
│  raw_stripped.txt → Download → random.sample(items, X) → Remove selected        │
│                                      │                    → Upload remaining     │
│                                      ▼                                          │
│                              Selected raw items (in memory)                     │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PHASE 3: LLM CLEANING                                  │
│                                                                                 │
│  For each item (concurrent via asyncio.gather):                                 │
│    Raw text → xAI Grok (CleanBot) → Cleaned text                               │
│                                                                                 │
│    Rules applied:                                                               │
│    ├─ Fix typos (preserve slang)                                                │
│    ├─ Names → [PLAYER] (preserve celebrities)                                   │
│    ├─ Drink references → [DRINKS] (preserve drink-as-variable)                  │
│    └─ Non-Norwegian → [OTHER]                                                   │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PHASE 4: POST-CLEAN PROCESSING                         │
│                                                                                 │
│  LLM output → strip() → empty? REJECT                                          │
│             → multi-line? Keep first line only                                  │
│             → quoted? Strip matching quotes                                     │
│             → assign to Item.prompt                                             │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PHASE 5: NORMALIZATION                                 │
│                                                                                 │
│  Item.prompt → normalize()                                                      │
│                ├─ Remove all non-word, non-whitespace chars                     │
│                ├─ [PLAYER] → PLAYER, [DRINKS] → DRINKS                         │
│                └─ empty? REJECT                                                 │
│             → build_dedup_key()                                                 │
│                ├─ Remove "player" and "drinks" as whole words                   │
│                ├─ Collapse whitespace                                           │
│                └─ Lowercase                                                     │
│             → Dedup key (used in Phase 6)                                       │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PHASE 6: DEDUPLICATION                                 │
│                                                                                 │
│  Dedup key checked against THREE stores:                                        │
│  ┌────────────────────────┐  ┌──────────────────────┐  ┌─────────────────────┐  │
│  │  GlobalDatabaseStore   │  │  DiscardedItemsStore │  │ UserSelectionStore  │  │
│  │  (in-memory set cache) │  │  (linear scan)       │  │ (linear scan)       │  │
│  └────────────────────────┘  └──────────────────────┘  └─────────────────────┘  │
│                                                                                 │
│  Match found? → INCREMENT occurrence count on existing entry                    │
│  No match?    → Add to USER_SELECTION.json for human review                     │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                              (no match path)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PHASE 7: HUMAN REVIEW                                  │
│                                                                                 │
│  USER_SELECTION.json → Pop batch of 5 → Present to reviewer                    │
│                                                                                 │
│  For each item:                                                                 │
│    ☐ (unchecked) → KEEP  → DATABASE.json   {prompt, occurrences: 1}            │
│    ☑ (checked)   → DISCARD → DISCARDS.json  {prompt, occurrences: 1}           │
│                                                                                 │
│  Queue < 20 items? → Auto-trigger Phases 2–6 to refill to ~50                  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                              (keep path)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PHASE 8: PARAMETERIZATION                              │
│                                                                                 │
│  DATABASE.json → Filter entries where 'craziness' is absent                     │
│               → Random sample of N entries                                      │
│               → For each entry:                                                 │
│                   LLM classifies → {craziness: 1-4, isSexual: bool,            │
│                                     madeFor?: "boys"|"girls"}                   │
│                   Validate JSON schema                                          │
│                   (Attempt partial recovery on truncated responses)              │
│               → Merge parametric fields back into DATABASE.json                 │
│               → Incremental save every 5 items (optimistic concurrency)         │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          PHASE 9: RUNTIME FILL                                  │
│                                                                                 │
│  Game session:                                                                  │
│    1. Select prompts by craziness probability distribution                      │
│    2. [PLAYER] → actual player names                                            │
│    3. [DRINKS] → appropriate amounts per drinking_level                         │
│    4. Apply vibe context and custom instructions                                │
│    5. Output: 5 ready-to-play Norwegian game prompts                            │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Summary of Guarantees

| Guarantee | Mechanism |
|---|---|
| No duplicate raw lines in pool | Exact-match set dedup on upload (Phase 1) |
| No metadata noise in pool | Date detection, name-line detection, min-length (Phase 1) |
| No blocked content in pool | Substring-match blocklist filter (Phase 1) |
| Items processed exactly once | Destructive read from raw pool (Phase 2) |
| Consistent text format | LLM cleaning + post-processing (Phases 3–4) |
| Semantic dedup across all stores | Normalized dedup key checked against 3 stores (Phases 5–6) |
| Human quality gate | Batch review with explicit accept/reject (Phase 7) |
| Structured classification | LLM parameterization with JSON schema validation (Phase 8) |
| No lost writes | Optimistic concurrency via GCS generations (Phase 12) |
| Frequency tracking | Occurrence counter incremented on duplicates (Phase 6) |
