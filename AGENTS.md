## About this system

### Data

- Be VERY judicious loading .srt or audio files into memory as they can be very long. Consider using regex, tail/head, etc. to load only what you need.

### System
Assume the system is running on Linux or macOS.
Shell is zsh; system has installed (not limited to):
- fzf
- rg
- jq

So make frequent use of these tools, e.g. jq to page through test results.

Assume multicore and 32+GB RAM.

## Rust

Please liberally use `cargo test` where necessary.

> NOTE: This is a bit of a pedagogical project for me, so as you work, please do a literate programming style. In particular, for major methods and functions, add liberal docstrings and make it so that `cargo doc` will help me understand the codebase better. No need to dumb down your own code, but explain the 'clever bits' and core design decisions as you go along.

UNLIKE python (see below), this will be a longer-running service, so I'd prefer this be fault tolerant.

Other code style requests:
- Be suspicious of any file where the core logic is >200-300 lines. This probably means it should be broken up into smaller functions or modules. As a rule of thumb, a file or abstraction is 'doing work' if it's about that long - ~300-400 is the ceiling, ~50-75 is the floor for a 'good chunk of work' in a file.
- Where possible, colocate _unit_ tests (in a test module) with the code they test, rather than using tests/. Integration tests should go in tests/ though instead of lib/ or src/.

## Python

**ALWAYS** use Astral UV commands! 
- NEVER do `pip install` directly, or use any `pip` commands.
- To add a dependency, do `uv add` or `uv sync`.  
- Run Python scripts with `uv run`, e.g. `uv run pytest`, `uv run foo.py`
- If you do not do this, NOTHING WILL WORK! DO NOT **EVER** try managing python yourself. This is the surest way to have me churn to a different provider.

### Code Quality Guidelines

==NOTE ON DEFENSIVE PROGRAMMING==
This benchmark is being used by technical users who are comfortable debugging and fixing errors. If something important is missing (i.e. a package added as non-optional dependency in pyproject is missing, or an argparse option we know we added is missing), it is MUCH better to crash with a clear traceback than to silently fail, produce incorrect results, or roll your own inefficent workaround. Errors are not scary - silent errors are scary!
- Don't fix issues with dependencies by rolling your own, unless it's clearly necessary.
  - E.g. If we try to run concurrently with aiolimiter, if aiolimiter is missing, this probably means ALL imports are broken and the environment is invalid, rather than that `uv` made a mistake. Rather than muddling on with our own implementation of rate limiting, we should crash with a clear traceback.
- Prefer leveraging Pydantic for data validation and parsing, rather than getattr.
  - E.g. If we need to validate a configuration option, use Pydantic's `BaseModel` and `Field` classes to define a schema for the configuration. Use field validators. Don't try to compensate downstream for missing data.
- `assert()` with clear error messages is your friend. Use it.
- Prefer adding class defaults rather than getattr. 
  - If you find yourself having to use getattr too much, it is a clear sign you probably want a union type, a stronger contract, or a different abstraction rather than patching.
  - For argparse, if you need getattr it is probably a sign that your defaults for arguments are bad.
- ESPECIALLY if issues might have algorithmic correctness issues, it is MUCH better to crash with a clear traceback than to silently fail or produce incorrect results.

Don't try to muddle through! I will tell you if we want to be fault tolerant.
(Do this within reason though. There's no need to overcompensate).

**Dependencies:**
- If a library is in `pyproject.toml` dependencies (not optional/extras), **assume it's installed**.
- Never wrap imports of required dependencies in `try/except ImportError`. If the import fails, the program SHOULD crash with a clear traceback - that's a deployment issue, not a code issue.
- Only use `try/except` for imports when dealing with truly optional extras or platform-specific modules.

**Attribute Access:**
- **Never use `getattr()` for attributes on classes you control.** This is an antipattern that hides type errors and makes code harder to understand.
- If an attribute might not exist, either:
  1. Ensure it always exists with a sane default (e.g., `Optional[T] = None` in the class definition)
  2. Use `hasattr()` + explicit type checking (e.g., `isinstance()`)
  3. Use proper inheritance/ABC to guarantee the interface
- Prefer early assertions over defensive `getattr(obj, "attr", None)` patterns.

**Examples:**
```python
# BAD - defensive, hides errors
return getattr(backend, "sample_rate", None)

# GOOD - direct access, type safe
return backend.sample_rate  # Backend always has this property

# BAD - try/except for required dependency in pyproject.toml
try:
    import numpy
except ImportError:
    return None

# GOOD - import directly, fail fast if missing
import numpy
```
