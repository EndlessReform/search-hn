## Git guidelines

Please do not complain about a dirty worktree unless there are genuinely a lot of substantial changes. You can check what's in the changes yourself, so do so. I will be very annoyed if you come to me complaining about it and weaseling out of doing work without a genuinely good reason (e.g. "we touched 17 files and ~800 LOC, I don't want to blow it away".

## About this system

### Analytics jobs

- Derived data from the DB (eg, all stories above 10 upvotes) will usually be in `data/`. ==DO NOT== assume these are full DB backups or contain all columns; you should generally only use them if I specifically tell you what they are. Don't use this for reference about DB migrations or application logic.
- For one-off simple analytical queries, assume the system has `duckdb` available on CLI - see if you can use DuckDB or jq to solve this, before reaching for Python. Use DuckDB via CLI first for one-off queries rather than through python library, unless you genuinely need data transformation or Python functions. Unless I say otherwise or it's contextually obvious (eg I want a persistent script), you should presume I want `.parquet` files to be inspected with DuckDB.
- Persistent scripts can use whatever tech stack, but I would much prefer DuckDB or polars-based solutions over pandas.
- Be VERY judicious reading text-based data files like `csv`, `jsonl` into memory as they can be very long. Unless you have a very good reason to assume these files are short, be careful using your native file read tools, vs just using DuckDB or code to summarize. 
    - Consider checking length with `wc`.
    - Consider using tail/head, etc. to load only sample rows.
    - Consider using `jq`, `rg`, or astgrep to load only what you need.


### System
Assume the system is running on Linux or macOS.
Shell is zsh; system has installed (not limited to):
- fzf
- rg
- jq
- duckdb

So make frequent use of these tools, e.g. jq to page through test results.

Assume multicore and 32+GB RAM.

## Rust

Please liberally use `cargo test` where necessary.
**Network access and sandboxing:** 
- Do `--locked` for operations like `cargo test`, `cargo run` unless you JUST added a dependency with `cargo add` / modified dependency requirements in `cargo.toml`.and need to be sure we have the new data. We want to avoid spurious sandbox network errors.
- Prefer explicit `cargo add` and remove (ask for elevated perms if not in full-access mode) over editing Cargo.toml for this, unless there is no better way (e.g. adding features should still require TOML edit).

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
