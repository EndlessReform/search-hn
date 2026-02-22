use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::assets::HTMX_ASSET_ROUTE;
use crate::page_shell::render_hn_shell;
use hn_core::db::story_tree::{CommentTreeNode, StoryCommentTree};

const PAGE_STYLES: &str = r#"
<style>
  .page {
    /*
      Match the homepage title column offset using the same visual model:
      a double-digit rank slot (`3ch`, including the dot) plus the title gap.
    */
    --rank-slot-width: 3ch;
    --story-line-gap: 4px;
    --comment-meta-font-size: 10px;
    --comment-meta-line-height: 14px;
    --comment-connector-y: calc(var(--comment-meta-line-height) / 2);
  }

  :root {
    --hn-thread-line: #d9d9d9;
    --hn-border: #e3e3dc;
  }

  #story-thread {
    padding-left: calc(var(--rank-slot-width) + var(--story-line-gap));
    padding-right: calc(var(--rank-slot-width) + var(--story-line-gap));
  }

  .story {
    margin-bottom: 12px;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--hn-border);
  }

  .story-title {
    margin: 0 0 5px;
    font-size: 16px;
    line-height: 1.35;
    font-weight: 400;
  }

  .story-meta,
  .comment-meta {
    margin: 0;
    font-size: var(--comment-meta-font-size);
    line-height: var(--comment-meta-line-height);
    color: var(--hn-muted);
  }

  .comment-stream,
  .comment-children {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }

  .comment {
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .comment-children {
    margin-left: 14px;
    border-left: 1px solid var(--hn-thread-line);
    padding-left: 12px;
    margin-top: 4px;
  }

  .comment::before {
    content: "";
    position: absolute;
    left: -12px;
    top: var(--comment-connector-y);
    width: 8px;
    border-top: 1px solid var(--hn-thread-line);
  }

  .comment-stream > .comment::before {
    display: none;
  }

  /*
    Stop the parent gutter line at the final child connector. The vertical line is
    drawn on `.comment-children` as a border. The last child starts after the
    container's 1px border + 12px left padding, so we back up by both amounts to
    cover the trailing segment while preserving the horizontal connector stem.
  */
  .comment-children > .comment:last-child::after {
    content: "";
    position: absolute;
    left: calc(-12px - 1px);
    top: calc(var(--comment-connector-y) + 1px);
    bottom: 0;
    width: 2px;
    background: var(--hn-panel);
    z-index: 1;
  }

  .comment-text {
    font-size: 13px;
    line-height: 1.38;
  }

  .comment-text p {
    margin: 0 0 7px;
  }

  .comment-text p:last-child {
    margin-bottom: 0;
  }

  .comment-text pre {
    overflow-x: auto;
  }

  .comment-author {
    color: var(--hn-link);
  }

  .comment-muted {
    color: var(--hn-muted);
  }

  .comment-toggle {
    border: 0;
    background: none;
    padding: 0;
    margin: 0 0 0 5px;
    color: var(--hn-muted);
    font: inherit;
    cursor: pointer;
  }

  .comment-toggle:hover {
    text-decoration: underline;
  }

  .comment-collapsed > .comment-children {
    display: none;
  }

  .loading,
  .empty-thread {
    margin: 0;
    font-size: 12px;
    color: var(--hn-muted);
  }

  .thread-error {
    font-size: 12px;
    color: var(--hn-error-text);
    background: var(--hn-error-bg);
    border: 1px solid var(--hn-error-border);
    padding: 10px 12px;
  }

  @media (prefers-color-scheme: dark) {
    :root {
      --hn-thread-line: #2a2f39;
      --hn-border: #242833;
    }
  }

  @media (max-width: 640px) {
    .page {
      --story-line-gap: 3px;
    }

    .story-title {
      font-size: 15px;
    }

    .comment-text {
      font-size: 12px;
    }
  }
</style>
"#;

/// Small client-side behavior used by the HTMX fragment.
///
/// Comments are loaded asynchronously, so we use event delegation on `document`
/// instead of binding listeners to individual nodes at render time.
const PAGE_SCRIPTS: &str = r#"
<script>
  function syncBrowserTitleFromStoryThread() {
    const thread = document.getElementById("story-thread");
    if (!thread) {
      return;
    }

    const story = thread.querySelector(".story[data-browser-title]");
    if (!story) {
      return;
    }

    const browserTitle = story.getAttribute("data-browser-title");
    if (browserTitle) {
      document.title = browserTitle;
    }
  }

  document.addEventListener("click", function (event) {
    const toggle = event.target.closest(".comment-toggle");
    if (!toggle) {
      return;
    }
    const comment = toggle.closest(".comment");
    if (!comment) {
      return;
    }

    const collapsed = comment.classList.toggle("comment-collapsed");
    toggle.textContent = collapsed ? "[+]" : "[-]";
    toggle.setAttribute("aria-expanded", collapsed ? "false" : "true");
  });

  document.addEventListener("htmx:afterSwap", function (event) {
    if (event.target && event.target.id === "story-thread") {
      syncBrowserTitleFromStoryThread();
    }
  });
</script>
"#;

/// Renders the HTMX-driven `/item` shell.
///
/// The shell intentionally does not include comment markup up front. Instead, it delegates
/// story-thread loading to `/item/thread` so the page is driven by the same retrieval logic
/// as the JSON API and keeps first paint lightweight.
pub fn render_item_page_shell(story_id: i64) -> String {
    let mut main_html = String::new();
    write!(
        main_html,
        "<section id=\"story-thread\" hx-get=\"/item/thread?id={story_id}\" hx-trigger=\"load\" hx-swap=\"innerHTML\" hx-on::response-error=\"this.innerHTML = event.detail.xhr.responseText\">"
    )
    .expect("writing to String should not fail");
    main_html.push_str("<p class=\"loading\">Loading thread...</p>");
    main_html.push_str("</section>");

    let mut footer_html = String::new();
    write!(footer_html, "<script src=\"{HTMX_ASSET_ROUTE}\"></script>")
        .expect("writing to String should not fail");
    footer_html.push_str(PAGE_SCRIPTS);

    render_hn_shell(
        "Local HN",
        PAGE_STYLES,
        None,
        &main_html,
        Some(footer_html.as_str()),
    )
}

/// Renders the HN-style story + nested comment fragment consumed by HTMX.
///
/// The tree node order comes directly from `hn_core::db::story_tree`, so this function is
/// strictly presentational and never reorders comments.
pub fn render_story_thread_fragment(tree: &StoryCommentTree) -> String {
    let mut html = String::new();
    let now_seconds = unix_now_seconds();

    let story_title = tree.story.title.as_deref().unwrap_or("Untitled story");
    let story_author = tree.story.by.as_deref().unwrap_or("unknown");
    let story_age = relative_age_label(tree.story.time, now_seconds);
    let comment_count = count_comments(&tree.roots);
    let breaks_count = tree.graph_breaks.len();

    write!(
        html,
        "<section class=\"story\" data-browser-title=\"{}\"><h1 class=\"story-title\">{}</h1>",
        escape_html(&format!("{story_title} | Local HN")),
        escape_html(story_title)
    )
    .expect("writing to String should not fail");
    write!(
        html,
        "<p class=\"story-meta\">by {} | {} | {} comment{} | item {}</p>",
        escape_html(story_author),
        escape_html(&story_age),
        comment_count,
        if comment_count == 1 { "" } else { "s" },
        tree.story.id
    )
    .expect("writing to String should not fail");
    if breaks_count > 0 {
        write!(
            html,
            "<p class=\"story-meta\">{} graph break{} detected while reconstructing thread</p>",
            breaks_count,
            if breaks_count == 1 { "" } else { "s" }
        )
        .expect("writing to String should not fail");
    }
    html.push_str("</section>");

    if tree.roots.is_empty() {
        html.push_str("<p class=\"empty-thread\">No comments yet.</p>");
        return html;
    }

    html.push_str("<section class=\"comment-stream\">");
    for node in &tree.roots {
        render_comment_node(node, now_seconds, &mut html);
    }
    html.push_str("</section>");
    html
}

/// Renders an in-fragment error block returned by `/item/thread`.
pub fn render_thread_error_fragment(story_id: i64, message: &str) -> String {
    format!(
        "<div class=\"thread-error\">Could not load story {}: {}</div>",
        story_id,
        escape_html(message)
    )
}

fn render_comment_node(node: &CommentTreeNode, now_seconds: i64, out: &mut String) {
    write!(
        out,
        "<article class=\"comment\" id=\"comment-{}\">",
        node.item.id
    )
    .expect("writing to String should not fail");

    let author = node.item.by.as_deref().unwrap_or("unknown");
    let age = relative_age_label(node.item.time, now_seconds);
    out.push_str("<p class=\"comment-meta\">");
    write!(
        out,
        "<span class=\"comment-author\">{}</span> <span class=\"comment-muted\">{}</span>",
        escape_html(author),
        escape_html(&age)
    )
    .expect("writing to String should not fail");
    if !node.children.is_empty() {
        let children_id = format!("comment-{}-children", node.item.id);
        write!(
            out,
            "<button type=\"button\" class=\"comment-toggle\" aria-expanded=\"true\" aria-controls=\"{}\">[-]</button>",
            children_id
        )
        .expect("writing to String should not fail");
    }
    out.push_str("</p>");

    let deleted = node.item.deleted.unwrap_or(false) || node.item.dead.unwrap_or(false);
    out.push_str("<div class=\"comment-text\">");
    if deleted {
        out.push_str("<span class=\"comment-muted\">[deleted]</span>");
    } else if let Some(text) = &node.item.text {
        out.push_str(text);
    } else {
        out.push_str("<span class=\"comment-muted\">[no text]</span>");
    }
    out.push_str("</div>");

    if !node.children.is_empty() {
        write!(
            out,
            "<section class=\"comment-children\" id=\"comment-{}-children\">",
            node.item.id
        )
        .expect("writing to String should not fail");
        for child in &node.children {
            render_comment_node(child, now_seconds, out);
        }
        out.push_str("</section>");
    }
    out.push_str("</article>");
}

fn count_comments(nodes: &[CommentTreeNode]) -> usize {
    fn count(node: &CommentTreeNode) -> usize {
        1 + node.children.iter().map(count).sum::<usize>()
    }

    nodes.iter().map(count).sum()
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
    use hn_core::db::story_tree::{CommentTreeNode, StoryCommentTree, StoryTreeItem};

    fn story_item(id: i64) -> StoryTreeItem {
        StoryTreeItem {
            id,
            parent: None,
            type_: Some("story".to_string()),
            by: Some("author".to_string()),
            time: Some(1_700_000_000),
            text: None,
            title: Some("A sample story".to_string()),
            deleted: Some(false),
            dead: Some(false),
            story_id: Some(id),
        }
    }

    fn comment_item(id: i64, parent: i64, text: &str) -> StoryTreeItem {
        StoryTreeItem {
            id,
            parent: Some(parent),
            type_: Some("comment".to_string()),
            by: Some("commenter".to_string()),
            time: Some(1_700_000_060),
            text: Some(text.to_string()),
            title: None,
            deleted: Some(false),
            dead: Some(false),
            story_id: Some(1),
        }
    }

    #[test]
    fn item_shell_uses_htmx_thread_loader() {
        let shell = render_item_page_shell(31741589);
        assert!(shell.contains("hx-get=\"/item/thread?id=31741589\""));
        assert!(shell.contains(HTMX_ASSET_ROUTE));
        assert!(shell.contains("comment-toggle"));
        assert!(shell.contains("<title>Local HN</title>"));
        assert!(shell.contains("htmx:afterSwap"));
        assert!(!shell.contains("unpkg.com"));
    }

    #[test]
    fn story_fragment_renders_nested_comments() {
        let tree = StoryCommentTree {
            story: story_item(1),
            roots: vec![CommentTreeNode {
                item: comment_item(10, 1, "<p>root</p>"),
                children: vec![CommentTreeNode {
                    item: comment_item(11, 10, "<p>child</p>"),
                    children: Vec::new(),
                }],
            }],
            graph_breaks: Vec::new(),
        };

        let rendered = render_story_thread_fragment(&tree);
        assert!(rendered.contains("A sample story"));
        assert!(rendered.contains("data-browser-title=\"A sample story | Local HN\""));
        assert!(rendered.contains("<article class=\"comment\" id=\"comment-10\">"));
        assert!(rendered.contains("<article class=\"comment\" id=\"comment-11\">"));
        assert!(rendered.contains("aria-controls=\"comment-10-children\""));
        assert!(rendered.contains("id=\"comment-10-children\""));
        assert!(!rendered.contains("aria-controls=\"comment-11-children\""));
        assert!(rendered.contains("<p>root</p>"));
        assert!(rendered.contains("<p>child</p>"));
    }

    #[test]
    fn thread_error_fragment_escapes_html() {
        let rendered = render_thread_error_fragment(123, "<broken>");
        assert!(rendered.contains("story 123"));
        assert!(rendered.contains("&lt;broken&gt;"));
    }
}
