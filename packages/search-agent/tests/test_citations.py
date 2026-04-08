"""Unit tests for the shared citation registry and marker resolution layer."""

from __future__ import annotations

import unittest

from search_agent.citations import CitationRegistry


class CitationRegistryTests(unittest.TestCase):
    """Exercise the cursor contract between tools, model text, and renderers."""

    def test_ingests_story_and_comment_payloads_and_renders_markers(self) -> None:
        registry = CitationRegistry()

        registry.ingest_tool_result(
            "fetch_stories",
            """
            {
              "query": "pricing",
              "results": [
                {
                  "id": 123,
                  "cursor": "story:123",
                  "title": "Pricing launch",
                  "url": "https://example.com/pricing"
                }
              ],
              "queries": [
                {
                  "query": "pricing",
                  "results": [
                    {
                      "id": 123,
                      "cursor": "story:123",
                      "title": "Pricing launch",
                      "url": "https://example.com/pricing"
                    }
                  ]
                }
              ]
            }
            """,
        )
        registry.ingest_tool_result(
            "fetch_top_comments",
            """
            {
              "story_id": 123,
              "story_cursor": "story:123",
              "comments": [
                {
                  "id": 456,
                  "cursor": "comment:456",
                  "author": "dang",
                  "comment": "This is about pricing."
                }
              ],
              "stories": [
                {
                  "story_id": 123,
                  "story_cursor": "story:123",
                  "comments": [
                    {
                      "id": 456,
                      "cursor": "comment:456",
                      "author": "dang",
                      "comment": "This is about pricing."
                    }
                  ]
                }
              ]
            }
            """,
        )

        rendered = registry.render_text(
            "The launch thread focused on pricing【story:123】【comment:456】."
        )

        self.assertEqual(
            rendered.text,
            "The launch thread focused on pricing[1][2].",
        )
        self.assertEqual([reference.number for reference in rendered.references], [1, 2])
        self.assertEqual(
            [reference.entry.cursor for reference in rendered.references],
            ["story:123", "comment:456"],
        )
        self.assertEqual(rendered.references[0].entry.title, "Pricing launch")
        self.assertEqual(rendered.references[1].entry.author, "dang")

    def test_repeated_markers_reuse_one_reference_number(self) -> None:
        registry = CitationRegistry()
        registry.ingest_tool_result(
            "fetch_stories",
            """
            {
              "query": "rust",
              "results": [
                {"id": 123, "cursor": "story:123", "title": "Rust story", "url": null}
              ],
              "queries": [
                {
                  "query": "rust",
                  "results": [
                    {"id": 123, "cursor": "story:123", "title": "Rust story", "url": null}
                  ]
                }
              ]
            }
            """,
        )

        rendered = registry.render_text("Rust won twice【story:123】 and again【story:123】.")

        self.assertEqual(rendered.text, "Rust won twice[1] and again[1].")
        self.assertEqual(len(rendered.references), 1)
        self.assertEqual(rendered.references[0].entry.cursor, "story:123")

    def test_bare_numeric_marker_resolves_as_fallback(self) -> None:
        registry = CitationRegistry()
        registry.ingest_tool_result(
            "fetch_top_comments",
            """
            {
              "story_id": 123,
              "story_cursor": "story:123",
              "comments": [
                {
                  "id": 456,
                  "cursor": "comment:456",
                  "author": "dang",
                  "comment": "fallback test"
                }
              ],
              "stories": [
                {
                  "story_id": 123,
                  "story_cursor": "story:123",
                  "comments": [
                    {
                      "id": 456,
                      "cursor": "comment:456",
                      "author": "dang",
                      "comment": "fallback test"
                    }
                  ]
                }
              ]
            }
            """,
        )

        rendered = registry.render_text("Fallback shorthand still works【456】.")

        self.assertEqual(rendered.text, "Fallback shorthand still works[1].")
        self.assertEqual(len(rendered.references), 1)
        self.assertEqual(rendered.references[0].entry.cursor, "comment:456")

    def test_later_story_search_enriches_stub_story_entry(self) -> None:
        registry = CitationRegistry()
        registry.ingest_tool_result(
            "fetch_top_comments",
            """
            {
              "story_id": 123,
              "story_cursor": "story:123",
              "comments": [],
              "stories": [{"story_id": 123, "story_cursor": "story:123", "comments": []}]
            }
            """,
        )
        registry.ingest_tool_result(
            "fetch_stories",
            """
            {
              "query": "launch",
              "results": [
                {
                  "id": 123,
                  "cursor": "story:123",
                  "title": "Launch thread",
                  "url": "https://example.com/launch"
                }
              ],
              "queries": [
                {
                  "query": "launch",
                  "results": [
                    {
                      "id": 123,
                      "cursor": "story:123",
                      "title": "Launch thread",
                      "url": "https://example.com/launch"
                    }
                  ]
                }
              ]
            }
            """,
        )

        rendered = registry.render_text("Launch thread【story:123】.")

        self.assertEqual(rendered.references[0].entry.title, "Launch thread")
        self.assertEqual(
            rendered.references[0].entry.source_url,
            "https://example.com/launch",
        )


if __name__ == "__main__":
    unittest.main()
