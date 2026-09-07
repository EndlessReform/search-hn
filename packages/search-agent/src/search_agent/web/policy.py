"""Publisher policy loaded from the repository's reviewed domain lists."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from search_agent.web.security import hostname_for_url


@dataclass(frozen=True)
class PolicyDecision:
    """A pre-network publisher-policy decision."""

    status: str
    reason: str


def _default_policy_directory() -> Path:
    """Locate policy data when running from this source checkout."""

    repository_root = Path(__file__).resolve().parents[5]
    return repository_root / "docs"


def _load_domains(path: Path) -> frozenset[str]:
    """Load normalized domain entries while ignoring comments and blanks."""

    assert path.is_file(), f"web publisher policy file is missing: {path}"
    entries: set[str] = set()
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.partition("#")[0].strip().lower().rstrip(".")
        if not line:
            continue
        assert "://" not in line and "/" not in line, (
            f"invalid domain in {path}:{line_number}: {line!r}"
        )
        entries.add(line.removeprefix("www."))
    return frozenset(entries)


def _matches(hostname: str, domains: frozenset[str]) -> str | None:
    """Return the matching apex entry, including matches from subdomains."""

    for domain in domains:
        if hostname == domain or hostname.endswith(f".{domain}"):
            return domain
    return None


@dataclass(frozen=True)
class PublisherPolicy:
    """Reviewed hard and editorial-skip publisher policy sets."""

    hard_blacklist: frozenset[str]
    comment_only_blacklist: frozenset[str]

    @classmethod
    def load(cls, directory: Path | None = None) -> "PublisherPolicy":
        """Load the two authoritative text files."""

        policy_directory = directory or _default_policy_directory()
        hard = _load_domains(policy_directory / "web-hard-blacklist.txt")
        comment_only = _load_domains(
            policy_directory / "web-comment-only-blacklist.txt"
        )
        overlap = hard & comment_only
        assert not overlap, f"publisher policy lists overlap: {sorted(overlap)}"
        return cls(hard_blacklist=hard, comment_only_blacklist=comment_only)

    def evaluate(self, normalized_url: str) -> PolicyDecision | None:
        """Return a short-circuit result for a listed publisher, if any."""

        hostname = hostname_for_url(normalized_url)
        hard_match = _matches(hostname, self.hard_blacklist)
        if hard_match:
            return PolicyDecision(
                status="blocked_domain",
                reason=f"{hard_match} is on the access/paywall blacklist",
            )
        comment_match = _matches(hostname, self.comment_only_blacklist)
        if comment_match:
            return PolicyDecision(
                status="news_skipped",
                reason=f"{comment_match} is configured as comment-only",
            )
        return None
