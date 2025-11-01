# Prompts Directory

This directory contains all prompt templates and configurations used throughout the Prompt Cleaner Global application.

## Organization

- `clean.prompt` - Prompt template for cleaning/filtering prompts
- `paramaterize.prompt` - Prompt template for parameterization
- Add new prompt files here as needed

## Usage

Prompts in this directory are accessible from any part of the application using relative paths:

```python
# From root level files
with open("prompts/clean.prompt", "r") as f:
    prompt = f.read()

# From ui/ subdirectories
import os
prompts_dir = os.path.join(os.path.dirname(__file__), "..", "..", "prompts")
with open(os.path.join(prompts_dir, "clean.prompt"), "r") as f:
    prompt = f.read()
```

## Guidelines

- Store all prompt templates and configurations in this directory
- Use descriptive filenames with `.prompt` extension
- Document the purpose of each prompt file
- Keep prompts version-controlled for reproducibility
