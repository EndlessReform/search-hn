"""Semver choices come from published releases and tags, never manual arithmetic."""
from semver import Version


def versions(tags: list[str]) -> list[Version]:
    """Ignore unrelated tags while retaining prereleases as reserved versions."""
    return sorted({Version.parse(tag[1:]) for tag in tags
                   if tag.startswith("v") and Version.is_valid(tag[1:])})


def choices(tags: list[str]) -> dict[str, str]:
    """Offer stable bumps, next patch previews, and continuation/promotion of previews.

    A canary never advances the stable baseline. Existing prerelease series also
    get continuation choices, including a minor/major series started previously.
    """
    known = versions(tags)
    stable = [v for v in known if v.prerelease is None]
    if not stable:
        raise ValueError("No stable baseline release; establish v0.2.0 first.")
    base = stable[-1]
    used = {str(v) for v in known}
    result = {}
    for bump in ("patch", "minor", "major"):
        target = getattr(base, f"bump_{bump}")()
        if str(target) not in used:
            result[f"{bump.capitalize()} → {target}"] = str(target)
        for channel in ("canary", "pre"):
            n = 1
            while f"{target}-{channel}.{n}" in used:
                n += 1
            value = f"{target}-{channel}.{n}"
            result[f"{bump.capitalize()} {channel} → {value}"] = value
    for v in known:
        if v.prerelease and v > base:
            target = v.finalize_version()
            channel = v.prerelease.split(".")[0]
            if channel not in ("pre", "canary"):
                continue
            n = 1
            while f"{target}-{channel}.{n}" in used:
                n += 1
            value = f"{target}-{channel}.{n}"
            result[f"Continue {target} {channel} → {value}"] = value
            if str(target) not in used:
                result[f"Promote {target} → {target}"] = str(target)
    return result
