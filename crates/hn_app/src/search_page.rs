//! Search presentation: normal GET navigation remains usable without JavaScript.
use crate::{
    home_page::{escape_html, render_story_row, unix_now_seconds, HOME_STYLES},
    page_shell::render_hn_shell,
    search::{SearchQuery, SearchResponse, Sort},
};
use std::fmt::Write;

/// Render initial, results, empty, fallback and error states in the shared shell.
/// User-controlled text is escaped at every HTML boundary; URLs are encoded first.
pub fn render(query: &SearchQuery, result: Option<&SearchResponse>, error: Option<&str>) -> String {
    let q = escape_html(&query.q);
    let mut content = String::new();
    write!(content, r#"<form action="/search" method="get" id="search-form" role="search">
<label for="query">Search stories</label><div class="search-input-row"><input type="search" id="query" name="q" value="{q}" maxlength="2048" required enterkeyhint="search"><button type="submit" id="search-button">Search</button></div>
<div class="search-options"><p id="search-scope">Stories with 25+ points · Titles &amp; links</p><div class="sort-control"><label for="sort">Order by</label><select id="sort" name="sort">"#).unwrap();
    for (sort, label) in [
        (Sort::Relevance, "Relevance"),
        (Sort::Score, "Points"),
        (Sort::Date, "Newest"),
    ] {
        write!(
            content,
            "<option value=\"{}\"{}>{label}</option>",
            sort.as_str(),
            if sort == query.sort { " selected" } else { "" }
        )
        .unwrap();
    }
    content.push_str("</select></div><button type=\"button\" id=\"details-toggle\" title=\"RRF and tuned scores displayed ×1000\" role=\"switch\" aria-checked=\"false\" hidden><span class=\"toggle-track\" aria-hidden=\"true\"></span>Details</button></div><p id=\"search-progress\" role=\"status\" hidden>Searching…</p></form>");
    content.push_str("<details class=\"ranking-boosts\"><summary>Ranking boosts</summary>");
    write!(content, "<form action=\"/search\" method=\"get\" id=\"tuning-form\"><input type=\"hidden\" name=\"q\" value=\"{q}\"><input type=\"hidden\" name=\"sort\" value=\"relevance\">").unwrap();
    if let Some(result) = result.filter(|r| r.sort == Sort::Relevance) {
        write!(
            content,
            "<input type=\"hidden\" name=\"session\" value=\"{}\">",
            result.session
        )
        .unwrap();
    }
    content.push_str("<fieldset aria-label=\"Ranking boosts\">");
    for (name, label, value) in [
        ("freshness", "Freshness", query.freshness),
        ("votes", "Votes", query.votes),
    ] {
        write!(content, "<label class=\"boost\" for=\"{name}\">{label}<input type=\"range\" id=\"{name}\" name=\"{name}\" min=\"0\" max=\"100\" step=\"5\" value=\"{value}\"><output for=\"{name}\">{value}</output></label>").unwrap();
    }
    content.push_str("<button type=\"submit\">Apply</button> <button type=\"button\" id=\"reset-boosts\" hidden>Reset</button><p>0 = original relevance. Freshness: 30-day half-life. Votes: logarithmic, capped at 1,000. Boosts reorder retrieved matches in relevance mode.</p></fieldset></form></details>");
    if let Some(error) = error {
        write!(
            content,
            "<div class=\"search-notice error\" role=\"alert\">{}</div>",
            escape_html(error)
        )
        .unwrap();
    } else if let Some(result) = result {
        let mode = if result.retrieval_mode == "hybrid" {
            "Related stories"
        } else {
            "Keyword matches"
        };
        if result.retrieval_mode == "keyword-only" {
            content.push_str("<p class=\"search-notice\">Semantic search is unavailable. Showing title keyword matches for this search.</p>");
        }
        write!(content, "<section class=\"search-results\" aria-labelledby=\"results-title\"><div class=\"results-heading\"><h2 id=\"results-title\">{mode}</h2><span>Page {}</span></div>", result.page).unwrap();
        if result.sort != Sort::Relevance {
            content.push_str("<p class=\"ranking-note\">Ordering applies to the best matches retrieved for this query.</p>");
        }
        if result.results.is_empty() {
            content.push_str("<div class=\"search-empty\"><h3>No stories to show.</h3><p>Try another description or a distinctive name. Only stories with 25+ points are included.</p></div>");
        } else {
            content.push_str("<div class=\"story-list\">");
            for (index, story) in result.results.iter().enumerate() {
                render_story_row(
                    story,
                    (result.page - 1) * 20 + index + 1,
                    unix_now_seconds(),
                    Some(&result.ranks[&story.id].inline_label()),
                    &mut content,
                );
            }
            content.push_str("</div>");
        }
        content.push_str("<nav class=\"pager\" aria-label=\"Search pages\">");
        if result.page > 1 {
            page_link(&mut content, result, result.page - 1, "Previous");
        }
        if result.has_more {
            page_link(&mut content, result, result.page + 1, "More");
        }
        content.push_str("</nav></section>");
    }
    let title = if query.q.is_empty() {
        "Search — Search HN".into()
    } else {
        format!("{} — Search HN", escape_html(&query.q))
    };
    render_hn_shell(
        &title,
        &format!("{DETAILS_INIT}{HOME_STYLES}{STYLES}"),
        None,
        &content,
        Some(SCRIPT),
    )
}

fn page_link(out: &mut String, r: &SearchResponse, page: usize, label: &str) {
    write!(
        out,
        "<a href=\"/search?q={}&amp;sort={}&amp;session={}&amp;freshness={}&amp;votes={}&amp;page={page}\">{label}</a>",
        encode(&r.query),
        r.sort.as_str(),
        r.session,
        r.tuning.freshness,
        r.tuning.votes
    )
    .unwrap();
}
/// Encode UTF-8 bytes as a query component, including reserved HTML/URL characters.
fn encode(value: &str) -> String {
    let mut result = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            result.push(byte as char);
        } else {
            write!(result, "%{byte:02X}").unwrap();
        }
    }
    result
}
// Apply the saved preference before rows paint, avoiding a flash of hidden details.
const DETAILS_INIT: &str = r#"<script>
try { document.documentElement.classList.toggle('show-search-details', localStorage.getItem('searchhn.details') === 'true'); } catch (_) {}
</script>"#;
const SCRIPT: &str = r#"<script>
const form = document.getElementById('search-form');
const tuningForm = document.getElementById('tuning-form');
for (const slider of tuningForm.querySelectorAll('input[type="range"]')) {
  slider.addEventListener('input', () => { slider.nextElementSibling.value = slider.value; });
  slider.addEventListener('change', () => tuningForm.requestSubmit());
  const remembered = document.createElement('input');
  remembered.type = 'hidden'; remembered.name = slider.name; remembered.value = slider.value;
  form.appendChild(remembered);
}
const resetBoosts = document.getElementById('reset-boosts');
resetBoosts.hidden = false;
resetBoosts.addEventListener('click', () => {
  for (const slider of tuningForm.querySelectorAll('input[type="range"]')) slider.value = '0';
  tuningForm.requestSubmit();
});
const detailsToggle = document.getElementById('details-toggle');
function syncDetails() {
  detailsToggle.setAttribute('aria-checked', String(document.documentElement.classList.contains('show-search-details')));
}
detailsToggle.hidden = false;
syncDetails();
detailsToggle.addEventListener('click', () => {
  const enabled = document.documentElement.classList.toggle('show-search-details');
  syncDetails();
  try { localStorage.setItem('searchhn.details', String(enabled)); } catch (_) {}
});
function resetSearch() {
  document.getElementById('search-progress').hidden = true;
  document.getElementById('search-button').disabled = false;
  form.removeAttribute('aria-busy');
}
form.addEventListener('submit', () => {
  document.getElementById('search-progress').hidden = false;
  document.getElementById('search-button').disabled = true;
  form.setAttribute('aria-busy', 'true');
});
window.addEventListener('pageshow', resetSearch);
</script>"#;
const STYLES: &str = r#"<style>
.ranking-boosts { max-width: 760px; margin: 0 0 16px; }
.ranking-boosts summary { color: var(--muted); font-size: 11px; cursor: pointer; }
#tuning-form { margin-top: 8px; }
#tuning-form fieldset { border: 1px solid var(--muted); padding: 6px 8px; font-size: 11px; }
.boost { display: inline-flex; align-items: center; gap: 6px; margin-right: 14px; }
.boost input { width: 125px; accent-color: #ff6600; }
.boost output { display: inline-block; min-width: 3ch; font-variant-numeric: tabular-nums; }
#tuning-form p { color: var(--muted); font-size: 10px; margin: 6px 0 0; line-height: 1.4; }
.search-details { display: none; }
.show-search-details .search-details { display: inline; }
#details-toggle { border: 0; background: transparent; padding: 0; font-size: 11px; color: var(--muted); }
#details-toggle:not([hidden]) { display: inline-flex; align-items: center; gap: 5px; }
.toggle-track { display: inline-block; width: 26px; height: 14px; border: 1px solid var(--muted); border-radius: 10px; position: relative; }
.toggle-track::after { content: ''; width: 8px; height: 8px; background: var(--muted); border-radius: 50%; position: absolute; left: 2px; top: 2px; }
#details-toggle[aria-checked="true"] .toggle-track { background: var(--hn-orange); border-color: var(--hn-orange); }
#details-toggle[aria-checked="true"] .toggle-track::after { left: 14px; background: #000; }
#search-form { position: relative; margin: 0 0 20px; max-width: 760px; }
#search-form > label { display: block; font-size: 14px; font-weight: bold; margin-bottom: 8px; }
.search-input-row { display: flex; align-items: center; gap: 6px; }
#query { flex: 1; min-width: 0; height: 28px; padding: 2px 5px; border: 1px solid var(--muted); border-radius: 0; background: var(--panel); color: var(--text); font: 13px Verdana,sans-serif; }
button, select { font: 12px Verdana,sans-serif; color: var(--text); background: var(--panel); border: 1px solid var(--muted); border-radius: 0; }
button { min-height: 28px; padding: 2px 10px; cursor: pointer; }
button:disabled { color: var(--muted); cursor: wait; }
select { min-height: 25px; padding: 1px 3px; }
.search-options { display: flex; align-items: center; flex-wrap: wrap; gap: 8px 20px; margin-top: 7px; }
#search-scope { margin: 0; font-size: 10px; color: var(--muted); }
.sort-control { display: flex; align-items: center; gap: 5px; }
#search-progress { position: absolute; top: 100%; margin: 3px 0 0; }
.sort-control label, #search-progress { font-size: 11px; color: var(--muted); }
a:focus-visible, input:focus-visible, button:focus-visible, select:focus-visible { outline: 2px solid #ff6600; outline-offset: 2px; }
.results-heading { display: flex; align-items: baseline; gap: 12px; margin-bottom: 14px; }
h2 { font-size: 12px; font-weight: normal; margin: 0; }
.results-heading > span, .ranking-note { font-size: 10px; color: var(--muted); }
.search-notice { font-size: 12px; line-height: 1.5; color: var(--muted); }
.error { color: var(--error-text); }
.search-empty { font-size: 12px; line-height: 1.5; color: var(--muted); }
.search-empty h3 { font-size: 12px; font-weight: normal; }
@media(max-width:640px) {
 #query { font-size: 16px; height: 34px; }
 button { min-height: 34px; }
 select { min-height: 30px; }
}
</style>"#;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn escapes_query_in_attribute_and_title() {
        let html = render(
            &SearchQuery {
                q: "\"><script>alert(1)</script>".into(),
                ..Default::default()
            },
            None,
            None,
        );
        assert!(!html.contains("<script>alert(1)"));
        assert!(html.contains("&quot;&gt;&lt;script&gt;"));
        assert_eq!(encode("a&日"), "a%26%E6%97%A5");
    }
}
