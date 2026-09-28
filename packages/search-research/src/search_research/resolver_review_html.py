"""Escaped HTML fragments for the saved resolver review interface."""

import json
from html import escape
from urllib.parse import urlencode

from search_research.resolver_bible import BIBLE_ID
from search_research.resolver_review import MODELS, selected_ids

LABELS = {
    "disagree": "All disagreements",
    "different_work": "Different work IDs",
    "luna_only": "Luna only selects",
    "ds_only": "DS only selects",
    "missing": "Unpaired / missing output",
    "same_work": "Same work ID",
    "both_abstain": "Both abstain",
    "all": "All references",
}


def e(value):
    return escape(str(value), quote=True)


def link(params, label, css=""):
    url = "/?" + urlencode(params)
    return (
        f'<a class="{css}" href="{e(url)}" hx-get="{e(url)}" '
        f'hx-target="#review" hx-swap="innerHTML show:top" hx-push-url="true">{label}</a>'
    )


def book(doc):
    return (
        f'<a target="_blank" rel="noopener" href="https://openlibrary.org{e(doc["id"])}">'
        f'{e(doc["title"])}</a><div class="authors">{e("; ".join(doc["authors"]) or "Author unknown")}'
        f"</div><code>{e(doc['id'])}</code>"
        f'<details class="catalog-details"><summary hx-get="/source/work/{e(doc["id"].split("/")[-1])}" hx-trigger="click once" hx-target="next .catalog-record">Full catalog record + editions</summary><div class="catalog-record">Loading catalog…</div></details>'
    )


def choice(detail, model):
    selection = detail["selections"].get(model)
    if selection is None:
        return '<strong class="missing">Missing output — not an abstention</strong>'
    ids = selected_ids(selection)
    if not ids:
        if any(
            r["action"] == "unresolved" for r in selection.get("resolution_items", [])
        ):
            return "<strong>Unresolved — repeated repair request</strong>"
        return "<strong>Abstained</strong>"
    output = []
    for key in ids:
        if key == BIBLE_ID:
            output.append(
                "<strong>The Bible</strong><p>Biblical works resolve to the Bible by explicit rule.</p>"
            )
        else:
            doc = (
                detail["selected_documents"][key]
                if "selected_documents" in detail
                else next(c for c in detail["candidates"] if c["id"] == key)
            )
            output.append(book(doc))
    return "<hr>".join(output)


def candidate_table(candidates, selections):
    rows = []
    for c in candidates:
        badges = [name for name, key in selections.items() if c["id"] in key]
        if c.get("author_match"):
            badges.append("AUTHOR MATCH")
        if c["shortlisted"]:
            badges.insert(0, "TOP 3")
        copies = (
            f" · {c['metadata_copies']} identical metadata"
            if c["metadata_copies"] > 1
            else ""
        )
        rows.append(
            f'<tr class="{"shortlisted" if c["shortlisted"] else ""}">'
            f"<td>{c['search_rank']}</td><td>{c['bm25']:.3f}</td>"
            f"<td>{c['rerank_rank']}</td><td>{c['rerank_score']:.3f}<br><small>Raw: {c['original_score']:.3f} · Readers: {c.get('readinglog_count', '—')}</small></td>"
            f'<td>{book(c)}<div class="flags">{e(" · ".join(badges) + copies)}</div></td></tr>'
        )
    return (
        '<div class="table-scroll"><table><thead><tr><th>Search #</th><th>BM25</th>'
        "<th>Rerank #</th><th>Score</th><th>Catalog candidate / final choices</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>"
    )


def detail_html(detail, model):
    ref = detail["reference"]
    text, start, end = ref["context"], ref["start"], ref["end"]
    assert text[start:end] == ref["title"], "Saved mention offsets do not match comment"
    passage = (
        e(text[:start]) + "<mark>" + e(text[start:end]) + "</mark>" + e(text[end:])
    )
    selected = {
        name: selected_ids(detail["selections"][key])
        for name, key in [("Luna", "luna"), (MODELS[model], model)]
        if key in detail["selections"]
    }
    candidates = detail["candidates"]
    ranked = sorted(candidates, key=lambda c: c["rerank_rank"])
    presentations = []
    for key in ("luna", model):
        if key in detail["selections"]:
            receipt = detail["selections"][key]
            messages = receipt["request"]["messages"]
            presentations.append(
                f"<h4>{e(key)} · exact saved messages</h4><pre>{e(json.dumps(messages, indent=2, ensure_ascii=False))}</pre>"
            )
    repairs = repair_panels(detail)
    shortlist_heading = (
        "INITIAL SELECTOR SHORTLIST"
        if "resolution_items" in ref
        else "SELECTOR SHORTLIST"
    )
    search_heading = "TITLE + AUTHOR SEARCH" if "person_spans" in ref else "BM25 SEARCH"
    retrieval = ""
    if "person_spans" in ref:
        names = (
            "; ".join(f"{s['text']} ({s['score']:.2f})" for s in ref["person_spans"])
            or "None detected"
        )
        retrieval = f'<section class="panel"><h2>RETRIEVAL INPUTS</h2><div class="panel-body"><p>Person names: {e(names)}</p><p>Search title: <b>{e(ref["query_title"])}</b></p><p>Author-match bonus: +{ref["author_bonus"]} BM25 points · applied before top 50</p>'
        if ref.get("retry_title"):
            retrieval += f"<p>Luna requested a second search: {e(ref['title'])} → {e(ref['retry_title'])}</p>"
        if detail.get("retrieval_history"):
            retrieval += f"<details><summary>First-pass candidates and decision</summary><pre>{e(json.dumps(detail['retrieval_history'], indent=2))}</pre></details>"
        retrieval += "</div></section>"
    return f"""{retrieval}<section class="panel source"><h2>SOURCE PASSAGE <a href="https://news.ycombinator.com/item?id={ref["comment_id"]}" target="_blank" rel="noopener">Open HN comment ↗</a></h2>
    <div class="panel-body"><p><b>{e(ref["title"])}</b> · <code>{e(ref["id"])}</code> · NER {ref["score"]:.3f} · span {start}–{end}</p>
    <p class="footnote">Complete frozen comment supplied to the selectors:</p><div class="comment-text">{passage}</div>
    <details><summary hx-get="/source/hn/{ref["comment_id"]}" hx-trigger="click once" hx-target="next .live-source">Show parent thread context (not supplied to selectors)</summary><div class="live-source">Loading parent context…</div></details></div></section>
    <div class="choices"><section class="panel"><h2>LUNA · CURRENT RUN</h2><div class="panel-body">{choice(detail, "luna")}</div></section>
    <section class="panel"><h2>{e(MODELS[model])} · FINAL CHOICE</h2><div class="panel-body">{choice(detail, model)}</div></section></div>
    {repairs}
    <section class="panel"><h2>{shortlist_heading} · TOP 3</h2>
    <div class="panel-body"><p>Selectors received these records in shuffled order, without scores. Identical metadata flags are not canonical book identities.</p>
    {candidate_table([c for c in ranked if c["shortlisted"]], selected)}</div></section>
    <div class="stages"><section class="panel"><h2>{search_heading} · ALL {len(candidates)} SAVED CANDIDATES</h2>
    {candidate_table(candidates, selected)}</section><section class="panel"><h2>RANKING · ALL {len(candidates)} SCORED CANDIDATES</h2>
    {candidate_table(ranked, selected)}</section></div>
    <details class="panel receipts"><summary>Exact selector inputs / presentation order</summary>{"".join(presentations)}</details>
    <p><a href="/api/reference/{e(ref["id"])}" target="_blank">Complete saved case JSON ↗</a></p>"""


def review_html(store, model, category, q, duplicates, page, ident, case=None):
    labels = LABELS | (
        {"luna_only": "New run only selects", "ds_only": "Original only selects"}
        if model == "luna-original"
        else {}
    )
    rows = store.filtered(model, category, q, duplicates)
    params = {
        "model": model,
        "category": category,
        "q": q,
        "duplicates": int(duplicates),
    }
    if case is not None and rows:
        ident = rows[min(case, len(rows)) - 1]["id"]
    if ident and ident in {r["id"] for r in rows}:
        position = next(i for i, r in enumerate(rows) if r["id"] == ident)
        page = position // 30 + 1
    else:
        page = min(page, max(1, (len(rows) + 29) // 30))
        position = (page - 1) * 30
        ident = rows[position]["id"] if rows else None
    counts = store.summary(model)
    items = []
    for r in rows[(page - 1) * 30 : page * 30]:
        label = (
            f"<b>{e(r['title'])}</b><small>{e(labels[store.category(r, model)])}"
            f"{' · duplicate shortlist' if r['duplicates'] else ''}</small>"
        )
        items.append(
            link(
                params | {"id": r["id"]},
                label,
                "case active" if r["id"] == ident else "case",
            )
        )
    total_pages = max(1, (len(rows) + 29) // 30)
    pagination = []
    for p, label in [(page - 1, "← Previous page"), (page + 1, "Next page →")]:
        if 0 < p <= (len(rows) + 29) // 30:
            pagination.append(link(params | {"page": p}, label, "queue-button"))
        else:
            pagination.append(
                f'<span class="queue-button disabled" aria-disabled="true">{label}</span>'
            )
    hidden = "".join(
        f'<input type="hidden" name="{key}" value="{e(value)}">'
        for key, value in params.items()
    )
    pager = f'''<nav class="queue-pager" aria-label="Queue pages">
    <div class="page-buttons">{"".join(pagination)}</div>
    <form action="/" hx-get="/" hx-target="#review" hx-push-url="true">
    {hidden}<label for="queue-page">Page</label><input id="queue-page" name="page" type="number" min="1" max="{total_pages}" value="{page}">
    <span>of {total_pages:,}</span><button type="submit">Go</button></form></nav>'''
    browse = link(
        params | {"q": "", "category": "all", "duplicates": 0},
        "Browse full queue / clear filters",
        "queue-button browse-all",
    )
    adjacent = []
    for pos, label in [
        (position - 1, "← Previous case"),
        (position + 1, "Next case →"),
    ]:
        if 0 <= pos < len(rows):
            adjacent.append(link(params | {"id": rows[pos]["id"]}, label, "case-step"))
        else:
            adjacent.append(
                f'<span class="case-step disabled" aria-disabled="true">{label}</span>'
            )
    case_navigation = f'''<nav class="case-nav" aria-label="Individual case navigation">
    <div class="case-buttons">{"".join(adjacent)}</div>
    <form action="/" hx-get="/" hx-target="#review" hx-push-url="true">{hidden}
    <label for="case-number">Case</label><input id="case-number" type="number" name="case" min="1" max="{max(1, len(rows))}" value="{position + 1 if rows else 1}">
    <span>of {len(rows):,}</span><button type="submit">Go to case</button></form>
    <div class="queue-scope">{"Search results for: " + e(q) if q else "Browsing " + e(labels[category].lower())} · {browse}</div></nav>'''
    options = "".join(
        f'<option value="{key}" {"selected" if key == category else ""}>{value}</option>'
        for key, value in labels.items()
    )
    models = "".join(
        f'<option value="{key}" {"selected" if key == model else ""}>{value}</option>'
        for key, value in MODELS.items()
    )
    body = (
        detail_html(store.detail(ident), model)
        if ident
        else '<div class="empty">No matching references.</div>'
    )
    return f'''<div class="overview">{counts["total"]:,} references · {counts.get("missing", 0):,} unpaired ·
    {counts.get("different_work", 0) + counts.get("luna_only", 0) + counts.get("ds_only", 0):,} disagreements ·
    <b>{counts["duplicate_shortlists"]:,} shortlists contain identical metadata; {counts["one_group_shortlists"]:,} have 3 slots / 1 metadata group</b></div>
    <div class="review-layout"><aside class="panel sidebar"><h2>REVIEW QUEUE</h2><div class="panel-body">
    <form action="/" hx-get="/" hx-target="#review" hx-push-url="true">
    <label for="q">Mention / comment ID / reference ID</label><input id="q" name="q" value="{e(q)}" placeholder="Pachinko, 42566137…">
    <label for="model">Compare Luna with</label><select id="model" name="model">{models}</select>
    <label for="category">Decision outcome</label><select id="category" name="category">{options}</select>
    <label class="checkbox"><input type="checkbox" name="duplicates" value="1" {"checked" if duplicates else ""}> Duplicate metadata in shortlist</label>
    <button class="wide" type="submit">Apply filters</button></form>{browse}<p>{len(rows):,} matches · 30 per page</p>
    </div>{pager}<div class="case-list">{"".join(items)}</div></aside>
    <main>{case_navigation}{body}</main></div>'''


def repair_panels(detail):
    """Render every split target and its own candidate pool, not a merged shortlist."""
    ref = detail["reference"]
    if "resolution_items" not in ref:
        return ""
    items = "".join(
        f"<li><b>{e(r['title'])}</b>{' · ' + e(r['author']) if r['author'] else ''} — {e(r['action'])}<br>{e(r['reason'])}</li>"
        for r in ref["resolution_items"]
    )
    result = f'<section class="panel"><h2>RESOLVED TARGETS</h2><div class="panel-body"><ul>{items}</ul></div></section>'
    history = detail["retrieval_history"] or {}
    for repair in history.get("repairs", []):
        case = repair["case"]
        docs = case["candidates"]
        ranks = {
            d["id"]: i for i, d in enumerate(sorted(docs, key=lambda d: -d["score"]), 1)
        }
        candidates = [
            d
            | {
                "search_rank": i,
                "rerank_rank": ranks[d["id"]],
                "rerank_score": d["score"],
                "original_score": d["raw_score"],
                "shortlisted": d["id"] in case["top3"],
                "metadata_copies": 1,
            }
            for i, d in enumerate(docs, 1)
        ]
        selected = {
            "Luna": [
                r["work_id"]
                for r in repair["receipt"]["decision"]["results"]
                if r["action"] == "select"
            ]
        }
        top = sorted(
            [c for c in candidates if c["shortlisted"]], key=lambda c: c["rerank_rank"]
        )
        heading = (
            "FINAL DECISION · EXISTING CANDIDATES"
            if case.get("reused_candidates")
            else f"REPAIR SEARCH · ROUND {case['round']}"
        )
        result += f'<section class="panel"><h2>{heading}</h2><div class="panel-body"><p><b>{e(case["query_title"])}</b> · Author: {e(case["query_author"] or "unspecified")}</p><p>{e(case["target_reason"])}</p>{candidate_table(top, selected)}<details><summary>All {len(candidates)} search candidates</summary>{candidate_table(candidates, selected)}</details><details><summary>Exact selector request and response</summary><pre>{e(json.dumps(repair["receipt"], indent=2))}</pre></details></div></section>'
    return result
