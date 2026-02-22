use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::page_shell::render_hn_shell;

/// Number of stories shown per homepage page, matching the requested MVP pagination.
pub const PAGE_SIZE: usize = 30;

/// Minimal row shape needed to render the homepage list.
///
/// This stays separate from `hn_core` ingest models because the homepage only needs
/// presentation data and should not accidentally depend on write-path structs.
#[derive(Debug, Clone)]
pub struct HomePageStory {
    pub id: i64,
    pub title: String,
    pub url: Option<String>,
    pub domain: Option<String>,
    pub by: Option<String>,
    pub time: Option<i64>,
    pub score: Option<i64>,
    pub descendants: Option<i64>,
}

/// View model for the paginated homepage.
#[derive(Debug, Clone)]
pub struct HomePageView {
    pub page_number: usize,
    pub has_more: bool,
    pub stories: Vec<HomePageStory>,
}

const HOME_STYLES: &str = r#"
<style>
  .page {
    --rank-slot-ch: 3;
    --story-line-gap: 4px;
    --rank-slot-width: calc(var(--rank-slot-ch) * 1ch);
  }

  .story-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .story {
    display: grid;
    grid-template-columns: var(--rank-slot-width) minmax(0, 1fr);
    column-gap: var(--story-line-gap);
    row-gap: 1px;
  }

  .story-line {
    font-size: 13px;
    line-height: 1.35;
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: var(--story-line-gap);
    grid-column: 2;
    min-width: 0;
  }

  .rank {
    color: var(--muted);
    font-size: 13px;
    line-height: 1.35;
    text-align: right;
    display: inline-block;
    grid-column: 1;
    grid-row: 1;
  }

  .story-title {
    font-size: 14px;
    font-weight: 400;
  }

  .story-domain {
    color: var(--muted);
    font-size: 12px;
  }

  .story-meta {
    margin: 0;
    padding-left: 0;
    font-size: 10px;
    line-height: 1.35;
    color: var(--muted);
    grid-column: 2;
  }

  .story-meta a {
    color: var(--muted);
  }

  .pager {
    margin-top: 14px;
    padding-left: calc(var(--rank-slot-width) + var(--story-line-gap));
    font-size: 12px;
    display: flex;
    gap: 10px;
    align-items: center;
  }

  .empty-state {
    margin: 0;
    padding-left: calc(var(--rank-slot-width) + var(--story-line-gap));
    color: var(--muted);
    font-size: 12px;
  }

  .page-error {
    font-size: 12px;
    color: var(--error-text);
    background: var(--error-bg);
    border: 1px solid var(--error-border);
    padding: 10px 12px;
  }

  @media (max-width: 640px) {
    .page {
      --story-line-gap: 3px;
    }

    .story-title {
      font-size: 13px;
    }

  }
</style>
"#;

/// Renders the paginated `/` homepage with HN-style compact rows.
///
/// Title links open the original article when available (matching HN behavior),
/// while the comments link in metadata stays on the local `/item` page.
pub fn render_home_page(view: &HomePageView) -> String {
    let mut main_html = String::new();
    if view.stories.is_empty() {
        main_html.push_str("<p class=\"empty-state\">No stories found for this page.</p>");
    } else {
        main_html.push_str("<section class=\"story-list\">");
        let now_seconds = unix_now_seconds();
        let start_rank = (view.page_number.saturating_sub(1) * PAGE_SIZE) + 1;
        for (offset, story) in view.stories.iter().enumerate() {
            render_story_row(story, start_rank + offset, now_seconds, &mut main_html);
        }
        main_html.push_str("</section>");
    }

    render_pager(view.page_number, view.has_more, &mut main_html);
    let main_attrs = format!(
        "style=\"--rank-slot-ch: {};\"",
        rank_slot_char_count(view.page_number, view.stories.len())
    );
    render_hn_shell(
        "Home",
        HOME_STYLES,
        Some(main_attrs.as_str()),
        &main_html,
        None,
    )
}

/// Renders a top-level homepage error page while preserving the same shell and typography.
pub fn render_home_page_error(page_number: usize, message: &str) -> String {
    let view = HomePageView {
        page_number,
        has_more: false,
        stories: Vec::new(),
    };
    let mut html = render_home_page(&view);
    let marker = "<main class=\"page\"";
    if let Some(idx) = html.find(marker) {
        if let Some(open_end) = html[idx..].find('>') {
            let insert_at = idx + open_end + 1;
            html.insert_str(
                insert_at,
                &format!(
                    "<div class=\"page-error\">Could not load homepage: {}</div>",
                    escape_html(message)
                ),
            );
        }
    }
    html
}

fn render_story_row(story: &HomePageStory, rank: usize, now_seconds: i64, out: &mut String) {
    let author = story.by.as_deref().unwrap_or("unknown");
    let age = relative_age_label(story.time, now_seconds);
    let points = story.score.unwrap_or(0).max(0);
    let comments = story.descendants.unwrap_or(0).max(0);
    let item_href = format!("/item?id={}", story.id);
    let title_href = story
        .url
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or(item_href.as_str());

    out.push_str("<article class=\"story\">");
    write!(out, "<span class=\"rank\">{}.</span>", rank)
        .expect("writing to String should not fail");
    out.push_str("<div class=\"story-line\">");
    write!(
        out,
        "<a class=\"story-title\" href=\"{}\">{}</a>",
        escape_html(title_href),
        escape_html(&story.title)
    )
    .expect("writing to String should not fail");
    if let Some(domain) = story.domain.as_deref().filter(|value| !value.is_empty()) {
        if let Some(url) = story.url.as_deref().filter(|value| !value.is_empty()) {
            write!(
                out,
                "<a class=\"story-domain\" href=\"{}\">({})</a>",
                escape_html(url),
                escape_html(domain)
            )
            .expect("writing to String should not fail");
        } else {
            write!(
                out,
                "<span class=\"story-domain\">({})</span>",
                escape_html(domain)
            )
            .expect("writing to String should not fail");
        }
    }
    out.push_str("</div>");

    out.push_str("<p class=\"story-meta\">");
    write!(
        out,
        "{} point{} by {} {} | <a href=\"{}\">{}</a>",
        points,
        if points == 1 { "" } else { "s" },
        escape_html(author),
        escape_html(&age),
        item_href,
        comment_link_label(comments)
    )
    .expect("writing to String should not fail");
    out.push_str("</p>");

    out.push_str("</article>");
}

fn render_pager(page_number: usize, has_more: bool, out: &mut String) {
    let prev_page = page_number.saturating_sub(1);
    out.push_str("<nav class=\"pager\" aria-label=\"Pagination\">");
    if prev_page >= 1 {
        if prev_page == 1 {
            out.push_str("<a href=\"/\">Prev</a>");
        } else {
            write!(out, "<a href=\"/?p={prev_page}\">Prev</a>")
                .expect("writing to String should not fail");
        }
    }
    if has_more {
        write!(
            out,
            "<a href=\"/?p={}\">More</a>",
            page_number.saturating_add(1)
        )
        .expect("writing to String should not fail");
    }
    out.push_str("</nav>");
}

fn comment_link_label(comment_count: i64) -> String {
    match comment_count {
        0 => "discuss".to_string(),
        1 => "1 comment".to_string(),
        count => format!("{count} comments"),
    }
}

/// Returns the rank cell width in `ch` units for the currently visible page.
///
/// We size the column from the widest rank shown on the page (e.g. `10.` vs `9.`)
/// so the numbers stay flush-right while the story titles line up consistently.
fn rank_slot_char_count(page_number: usize, story_count: usize) -> usize {
    let start_rank = (page_number.saturating_sub(1) * PAGE_SIZE) + 1;
    let max_rank = if story_count == 0 {
        start_rank
    } else {
        start_rank + story_count - 1
    };
    max_rank.to_string().len() + 1
}

fn relative_age_label(unix_seconds: Option<i64>, now_seconds: i64) -> String {
    let Some(value) = unix_seconds else {
        return "unknown age".to_string();
    };

    let delta = (now_seconds - value).max(0);
    match delta {
        0..=59 => "just now".to_string(),
        60..=3_599 => unit_age(delta / 60, "minute"),
        3_600..=86_399 => unit_age(delta / 3_600, "hour"),
        86_400..=2_592_000 => unit_age(delta / 86_400, "day"),
        2_592_001..=31_536_000 => unit_age(delta / 2_592_000, "month"),
        _ => unit_age(delta / 31_536_000, "year"),
    }
}

fn unit_age(value: i64, unit: &str) -> String {
    if value == 1 {
        format!("1 {unit} ago")
    } else {
        format!("{value} {unit}s ago")
    }
}

fn unix_now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_secs() as i64
}

fn escape_html(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    fn story() -> HomePageStory {
        HomePageStory {
            id: 42,
            title: "Hello <world>".to_string(),
            url: Some("https://example.com/a?b=1&c=2".to_string()),
            domain: Some("example.com".to_string()),
            by: Some("alice".to_string()),
            time: Some(1_700_000_000),
            score: Some(12),
            descendants: Some(3),
        }
    }

    #[test]
    fn renders_article_link_for_title_and_item_link_for_comments() {
        let html = render_home_page(&HomePageView {
            page_number: 1,
            has_more: true,
            stories: vec![story()],
        });

        assert!(html.contains("class=\"story-title\" href=\"https://example.com/a?b=1&amp;c=2\""));
        assert!(html.contains("href=\"/item?id=42\">3 comments</a>"));
        assert!(html.contains("Hello &lt;world&gt;"));
        assert!(html.contains("href=\"/?p=2\">More</a>"));
    }

    #[test]
    fn pager_links_back_to_root_for_first_page_prev() {
        let mut out = String::new();
        render_pager(2, false, &mut out);
        assert!(out.contains("href=\"/\">Prev</a>"));
    }
}
