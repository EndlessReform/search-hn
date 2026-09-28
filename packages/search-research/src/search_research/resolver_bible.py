"""Collapse biblical-book references to one explicit, catalog-independent identity.

Title matching only enables the exception. The selector must establish biblical
meaning from the full comment; names such as Job or Genesis are not automatic
matches. This is a product rule, not a claim that the catalog records are equal.
"""

import re

BIBLE_ID = "special:bible"
BIBLE_DOCUMENT = {"id": BIBLE_ID, "title": "The Bible", "authors": []}
BOOKS = set(
    """genesis|exodus|leviticus|numbers|deuteronomy|joshua|judges|ruth|
1 samuel|2 samuel|1 kings|2 kings|1 chronicles|2 chronicles|ezra|nehemiah|
esther|job|psalms|psalm|proverbs|ecclesiastes|song of solomon|song of songs|
isaiah|jeremiah|lamentations|ezekiel|daniel|hosea|joel|amos|obadiah|jonah|
micah|nahum|habakkuk|zephaniah|haggai|zechariah|malachi|matthew|mark|luke|
john|acts|acts of the apostles|romans|1 corinthians|2 corinthians|galatians|
ephesians|philippians|colossians|1 thessalonians|2 thessalonians|1 timothy|
2 timothy|titus|philemon|hebrews|james|1 peter|2 peter|1 john|2 john|3 john|
jude|revelation|revelations|apocalypse|tobit|judith|wisdom|wisdom of solomon|
sirach|ecclesiasticus|baruch|1 maccabees|2 maccabees|3 maccabees|4 maccabees|
1 esdras|2 esdras|prayer of manasseh|susanna|bel and the dragon|letter of jeremiah|
bible|holy bible|old testament|new testament""".replace("\n", "").split("|")
)


def bible_candidate(title: str) -> bool:
    """Recognize book names and ordinary numbered/prefixed biblical references."""
    title = re.sub(r"[^\w\s]", " ", title.casefold())
    title = " ".join(title.split())
    title = re.sub(r"^(?:the )?(?:book of |gospel (?:according to |of ))?", "", title)
    for word, number in [
        ("first", "1"),
        ("second", "2"),
        ("third", "3"),
        ("fourth", "4"),
        ("iii", "3"),
        ("ii", "2"),
        ("iv", "4"),
        ("i", "1"),
    ]:
        title = re.sub(r"^" + word + r"\s+", number + " ", title)
    title = re.sub(r"\s+\d+(?:\s+\d+)*$", "", title)
    return title in BOOKS


BIBLE_PROMPT = (
    "\nSpecial resolution rule: if the marked mention means the Bible, a biblical "
    'book, or a biblical testament, return work_id "special:bible", even when '
    "no retrieved candidate fits. This rule deliberately groups biblical books "
    "under The Bible. Determine this from the full comment, not the title alone. "
    "Do not apply it to an unrelated novel with the same title, a person, a "
    "commentary about a biblical book, or other non-biblical uses. For those, "
    "follow the ordinary candidate selection rules. "
)
