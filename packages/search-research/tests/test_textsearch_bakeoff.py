"""Verify fixed field definitions and explicit index selection."""

from search_research.textsearch_bakeoff import retrieve


class Cursor:
    def execute(self, sql, args):
        self.sql, self.args = sql, args

    def fetchall(self):
        return [(123, -2.5)]


def test_combined_and_title_use_their_own_indexes():
    cur = Cursor()
    assert retrieve(cur, "ISS leak", "combined") == [(123, -2.5)]
    assert "textsearch_input_idx" in cur.args
    retrieve(cur, "ISS leak", "title-only")
    assert "textsearch_title_idx" in cur.args


def test_title_boost_is_an_exact_two_field_score_not_repeated_text():
    cur = Cursor()
    retrieve(cur, "ISS leak", "title2-url1")
    assert "2*(title" in cur.sql
    assert "textsearch_url_idx" in cur.sql
    assert "MATERIALIZED" in cur.sql
    assert cur.args == ("ISS leak", "ISS leak")
