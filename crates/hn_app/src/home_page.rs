use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

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
  :root {
    --hn-orange: #ff6600;
    --bg: #f6f6ef;
    --panel: #f6f6ef;
    --text: #000;
    --muted: #828282;
    --rule: #e3e3dc;
    --link: #000;
    --error-bg: #fff2f1;
    --error-border: #e6c3bf;
    --error-text: #7a1712;
  }

  * { box-sizing: border-box; }

  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: Verdana, Geneva, sans-serif;
  }

  a {
    color: var(--link);
    text-decoration: none;
  }

  a:hover {
    text-decoration: underline;
  }

  .top-bar {
    background: var(--hn-orange);
    color: #000;
    font-size: 12px;
    line-height: 24px;
  }

  .top-bar-inner {
    max-width: 1100px;
    margin: 0 auto;
    padding: 0 10px;
    display: flex;
    align-items: center;
    gap: 8px;
    min-height: 24px;
  }

  .top-bar a {
    color: #000;
    font-weight: 700;
  }

  .page {
    max-width: 1100px;
    margin: 0 auto;
    padding: 10px 10px 30px;
    background: var(--panel);
  }

  .story-list {
    display: flex;
    flex-direction: column;
    gap: 7px;
  }

  .story {
    border-bottom: 1px solid var(--rule);
    padding-bottom: 7px;
  }

  .story-line {
    font-size: 13px;
    line-height: 1.35;
    display: flex;
    flex-wrap: wrap;
    align-items: baseline;
    gap: 4px;
  }

  .rank {
    color: var(--muted);
    min-width: 2.2em;
    text-align: right;
    display: inline-block;
  }

  .story-title {
    font-size: 14px;
    font-weight: 700;
  }

  .story-domain {
    color: var(--muted);
    font-size: 12px;
  }

  .story-meta {
    margin: 2px 0 0;
    padding-left: 2.55em;
    font-size: 10px;
    line-height: 1.35;
    color: var(--muted);
  }

  .story-meta a {
    color: var(--muted);
  }

  .pager {
    margin-top: 14px;
    padding-left: 2.55em;
    font-size: 12px;
    display: flex;
    gap: 10px;
    align-items: center;
  }

  .page-note {
    color: var(--muted);
  }

  .empty-state {
    margin: 0;
    padding-left: 2.55em;
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

  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0b0c0f;
      --panel: #0f1115;
      --text: #e7e7e7;
      --muted: #a3a3a3;
      --rule: #242833;
      --link: #f0f0f0;
      --error-bg: #2d1716;
      --error-border: #6b2b27;
      --error-text: #ffb7b0;
    }

    .top-bar {
      color: #1d1205;
    }

    .top-bar a {
      color: #1d1205;
    }
  }

  @media (max-width: 640px) {
    .page {
      padding: 8px 8px 24px;
    }

    .story-title {
      font-size: 13px;
    }

    .story-line {
      gap: 3px;
    }
  }
</style>
"#;

/// Renders the paginated `/` homepage with HN-style compact rows.
///
/// The title links point at the local `/item` page as requested, while the original
/// source domain remains visible as lightweight context.
pub fn render_home_page(view: &HomePageView) -> String {
    let mut html = String::new();
    html.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    html.push_str("<title>top</title>");
    html.push_str(HOME_STYLES);
    html.push_str("</head><body>");
    html.push_str("<header class=\"top-bar\"><div class=\"top-bar-inner\">");
    html.push_str("<a href=\"/\">top</a>");
    if view.page_number > 1 {
        write!(
            html,
            "<span class=\"page-note\">page {}</span>",
            view.page_number
        )
        .expect("writing to String should not fail");
    }
    html.push_str("</div></header>");
    html.push_str("<main class=\"page\">");

    if view.stories.is_empty() {
        html.push_str("<p class=\"empty-state\">No stories found for this page.</p>");
    } else {
        html.push_str("<section class=\"story-list\">");
        let now_seconds = unix_now_seconds();
        let start_rank = (view.page_number.saturating_sub(1) * PAGE_SIZE) + 1;
        for (offset, story) in view.stories.iter().enumerate() {
            render_story_row(story, start_rank + offset, now_seconds, &mut html);
        }
        html.push_str("</section>");
    }

    render_pager(view.page_number, view.has_more, &mut html);
    html.push_str("</main></body></html>");
    html
}

/// Renders a top-level homepage error page while preserving the same shell and typography.
pub fn render_home_page_error(page_number: usize, message: &str) -> String {
    let view = HomePageView {
        page_number,
        has_more: false,
        stories: Vec::new(),
    };
    let mut html = render_home_page(&view);
    let marker = "<main class=\"page\">";
    if let Some(idx) = html.find(marker) {
        let insert_at = idx + marker.len();
        html.insert_str(
            insert_at,
            &format!(
                "<div class=\"page-error\">Could not load homepage: {}</div>",
                escape_html(message)
            ),
        );
    }
    html
}

fn render_story_row(story: &HomePageStory, rank: usize, now_seconds: i64, out: &mut String) {
    let author = story.by.as_deref().unwrap_or("unknown");
    let age = relative_age_label(story.time, now_seconds);
    let points = story.score.unwrap_or(0).max(0);
    let comments = story.descendants.unwrap_or(0).max(0);
    let item_href = format!("/item?id={}", story.id);

    out.push_str("<article class=\"story\">");
    out.push_str("<div class=\"story-line\">");
    write!(out, "<span class=\"rank\">{}.</span>", rank)
        .expect("writing to String should not fail");
    write!(
        out,
        "<a class=\"story-title\" href=\"{}\">{}</a>",
        item_href,
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
    fn renders_item_links_for_titles_and_comments() {
        let html = render_home_page(&HomePageView {
            page_number: 1,
            has_more: true,
            stories: vec![story()],
        });

        assert!(html.contains("href=\"/item?id=42\""));
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
