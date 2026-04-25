use std::fmt::Write;

/// Shared HN-like page frame used by local HTML routes.
///
/// This centralizes the constrained shell (orange header + cream panel), shared
/// typography, and light/dark theme tokens so homepage and item pages stay visually
/// in sync. Callers provide page-specific CSS and the main content HTML.
pub fn render_hn_shell(
    title: &str,
    page_styles: &str,
    main_attrs: Option<&str>,
    main_content: &str,
    footer_html: Option<&str>,
) -> String {
    let mut html = String::new();
    html.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    write!(html, "<title>{}</title>", title).expect("writing to String should not fail");
    html.push_str(SHARED_SHELL_STYLES);
    html.push_str(page_styles);
    html.push_str("</head><body>");
    html.push_str("<div class=\"shell\">");
    html.push_str("<header class=\"top-bar\"><div class=\"top-bar-inner\">");
    html.push_str("<a href=\"/\">Home</a>");
    html.push_str("</div></header>");
    if let Some(attrs) = main_attrs.filter(|value| !value.is_empty()) {
        write!(html, "<main class=\"page\" {}>", attrs).expect("writing to String should not fail");
    } else {
        html.push_str("<main class=\"page\">");
    }
    html.push_str(main_content);
    html.push_str("</main></div>");
    if let Some(footer) = footer_html {
        html.push_str(footer);
    }
    html.push_str("</body></html>");
    html
}

/// Shared shell styling for local HN-like pages.
///
/// The outer background is a subtle tint of HN cream (lighter by mixing toward
/// white), while the inner panel keeps the canonical HN cream tone.
const SHARED_SHELL_STYLES: &str = r#"
<style>
  :root {
    --hn-orange: #ff6600;
    --panel: #f6f6ef;
    --bg: #f8f8f3;
    --text: #000;
    --muted: #828282;
    --link: #000;
    --error-bg: #fff2f1;
    --error-border: #e6c3bf;
    --error-text: #7a1712;

    /* Aliases kept for existing page-local CSS during incremental refactors. */
    --hn-bg: var(--bg);
    --hn-panel: var(--panel);
    --hn-text: var(--text);
    --hn-muted: var(--muted);
    --hn-link: var(--link);
    --hn-error-bg: var(--error-bg);
    --hn-error-border: var(--error-border);
    --hn-error-text: var(--error-text);
  }

  * { box-sizing: border-box; }

  html,
  body {
    max-width: 100%;
    overflow-x: hidden;
  }

  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: Verdana, Geneva, sans-serif;
  }

  a {
    color: var(--link);
    text-decoration: none;
    overflow-wrap: anywhere;
    word-break: break-word;
  }

  a:hover {
    text-decoration: underline;
  }

  .shell {
    max-width: 1100px;
    margin: 0 auto;
    background: var(--panel);
    overflow-x: hidden;
  }

  .top-bar {
    background: var(--hn-orange);
    color: #000;
    font-size: 12px;
    line-height: 24px;
  }

  .top-bar-inner {
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
    padding: 10px 10px 30px;
    background: var(--panel);
    min-width: 0;
    overflow-x: hidden;
  }

  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0b0c0f;
      --panel: #0f1115;
      --text: #e7e7e7;
      --muted: #a3a3a3;
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
  }
</style>
"#;
