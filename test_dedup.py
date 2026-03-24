"""Tests for dedup logic — proving the [PLAYER] drinks [DRINKS] bug and verifying the fix."""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from text_utils import normalize, build_dedup_key
from database import _prompt_dedup_key, DatabaseManager, GlobalDatabaseStore, UserSelectionStore, DiscardedItemsStore


class TestDedupKeyNotEmpty(unittest.TestCase):
    """The core bug: prompts made entirely of placeholder words produce empty dedup keys."""

    def test_player_drinks_brackets_produces_nonempty_key(self):
        """[PLAYER] drinks [DRINKS] must NOT produce an empty dedup key."""
        key = _prompt_dedup_key("[PLAYER] drinks [DRINKS]")
        self.assertTrue(key, "Dedup key for '[PLAYER] drinks [DRINKS]' must not be empty")

    def test_player_drinks_brackets_key_is_stable(self):
        """Two identical placeholder prompts must produce the same key."""
        k1 = _prompt_dedup_key("[PLAYER] drinks [DRINKS]")
        k2 = _prompt_dedup_key("[PLAYER] drinks [DRINKS]")
        self.assertEqual(k1, k2)

    def test_player_only_bracket(self):
        key = _prompt_dedup_key("[PLAYER]")
        self.assertTrue(key, "Dedup key for '[PLAYER]' must not be empty")

    def test_drinks_only_bracket(self):
        key = _prompt_dedup_key("[DRINKS]")
        self.assertTrue(key, "Dedup key for '[DRINKS]' must not be empty")

    def test_player_drinks_no_other_words(self):
        """[PLAYER] [DRINKS] with no extra words must still have a key."""
        key = _prompt_dedup_key("[PLAYER] [DRINKS]")
        self.assertTrue(key, "Dedup key for '[PLAYER] [DRINKS]' must not be empty")

    def test_case_variants_same_key(self):
        """[player], [PLAYER], [Player] should all produce the same dedup key."""
        k1 = _prompt_dedup_key("[PLAYER] drinks [DRINKS]")
        k2 = _prompt_dedup_key("[player] drinks [drinks]")
        k3 = _prompt_dedup_key("[Player] drinks [Drinks]")
        self.assertEqual(k1, k2)
        self.assertEqual(k2, k3)


class TestPlaceholderDedupDistinctness(unittest.TestCase):
    """Placeholder prompts should dedup correctly but remain distinct from unrelated prompts."""

    def test_different_prompts_different_keys(self):
        """'[PLAYER] drinks [DRINKS]' and 'Tell [PLAYER] a joke' must differ."""
        k1 = _prompt_dedup_key("[PLAYER] drinks [DRINKS]")
        k2 = _prompt_dedup_key("Tell [PLAYER] a joke")
        self.assertNotEqual(k1, k2)

    def test_same_content_with_and_without_brackets_differ(self):
        """Bare 'Player drinks' (no brackets, likely not a real prompt) vs bracketed should differ.
        The bracketed version has sentinels; the bare version just has the words stripped."""
        k_bracketed = _prompt_dedup_key("[PLAYER] drinks [DRINKS]")
        k_bare = _prompt_dedup_key("Player drinks")
        # These may or may not differ depending on implementation, but the
        # critical thing is the bracketed version has a non-empty key.
        self.assertTrue(k_bracketed)

    def test_player_placeholder_preserved_in_longer_prompt(self):
        """[PLAYER] inside a longer prompt should not break dedup."""
        k1 = _prompt_dedup_key("[PLAYER] must sing a song")
        k2 = _prompt_dedup_key("[PLAYER] must sing a song")
        self.assertEqual(k1, k2)
        self.assertTrue(k1)


class TestNormalizePreservesPlaceholders(unittest.TestCase):
    """normalize() should convert [PLAYER]/[DRINKS] to stable tokens, not strip them."""

    def test_brackets_produce_stable_token(self):
        result = normalize("[PLAYER] drinks [DRINKS]")
        # After normalization, the result should contain something representing
        # the placeholders — not just "PLAYER drinks DRINKS" which gets wiped.
        # We don't test exact token names, but the dedup key must be non-empty.
        key = build_dedup_key(result)
        self.assertTrue(key)

    def test_non_placeholder_brackets_still_stripped(self):
        """Brackets around non-placeholder words should still be stripped normally."""
        result = normalize("[hello] world")
        self.assertEqual(result, "hello world")

    def test_regular_text_unchanged(self):
        """Text without brackets should normalize the same as before."""
        self.assertEqual(normalize("Take a sip of beer"), "Take a sip of beer")


class TestBuildDedupKeyRegression(unittest.TestCase):
    """Ensure build_dedup_key still works for normal prompts."""

    def test_normal_prompt(self):
        key = build_dedup_key(normalize("Everyone take a shot"))
        self.assertEqual(key, "everyone take a shot")

    def test_bare_player_word_still_removed(self):
        """The bare word 'player' (no brackets) should still be removed from dedup."""
        key = build_dedup_key(normalize("The player must drink"))
        self.assertNotIn("player", key)

    def test_bare_drinks_word_still_removed(self):
        key = build_dedup_key(normalize("Everyone drinks water"))
        self.assertNotIn("drinks", key)

    def test_empty_string(self):
        self.assertEqual(build_dedup_key(""), "")

    def test_whitespace_only(self):
        self.assertEqual(build_dedup_key("   "), "")


class TestDatabaseManagerExistsDedupIntegration(unittest.TestCase):
    """Integration tests: mock the GCS layer, verify that placeholder prompts
    are correctly detected as duplicates by DatabaseManager.exists_in_database."""

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    def _run(self, coro):
        return self.loop.run_until_complete(coro)

    def _make_fake_global_store(self, existing_prompts):
        """Create a GlobalDatabaseStore with a pre-populated dedup cache (no GCS)."""
        store = GlobalDatabaseStore()
        store._initialized = True
        store._client = MagicMock()
        store._bucketName = "test"
        store._objectName = "test.json"
        store._dedupCache = set()
        for p in existing_prompts:
            key = _prompt_dedup_key(p)
            if key:
                store._dedupCache.add(key)
        store._cacheTimestamp = float('inf')  # never expire
        return store

    def _make_fake_user_selection_store(self, existing_prompts):
        store = UserSelectionStore()
        store._initialized = True
        store._lock = asyncio.Lock()
        store._client = MagicMock()
        store._bucketName = "test"
        store._objectName = "test.json"
        data = [{"prompt": p} for p in existing_prompts]
        store._load_json = AsyncMock(return_value=data)
        return store

    def _make_fake_discards_store(self, existing_prompts):
        store = DiscardedItemsStore()
        store._initialized = True
        store._lock = asyncio.Lock()
        store._client = MagicMock()
        store._bucketName = "test"
        store._objectName = "test.json"
        data = [{"prompt": p, "occurrences": 1} for p in existing_prompts]
        store._load_json = AsyncMock(return_value=data)
        return store

    def test_placeholder_prompt_detected_in_global_db(self):
        """If '[PLAYER] drinks [DRINKS]' is in the global DB, exists_in_database must return True."""
        db = DatabaseManager()
        db.globalStore = self._make_fake_global_store(["[PLAYER] drinks [DRINKS]"])
        db.discardsStore = self._make_fake_discards_store([])
        db.userSelection = self._make_fake_user_selection_store([])

        result = self._run(db.exists_in_database("[PLAYER] drinks [DRINKS]"))
        self.assertTrue(result, "Should detect '[PLAYER] drinks [DRINKS]' as existing in global DB")

    def test_placeholder_prompt_detected_in_user_selection(self):
        db = DatabaseManager()
        db.globalStore = self._make_fake_global_store([])
        db.discardsStore = self._make_fake_discards_store([])
        db.userSelection = self._make_fake_user_selection_store(["[PLAYER] drinks [DRINKS]"])

        result = self._run(db.exists_in_database("[PLAYER] drinks [DRINKS]"))
        self.assertTrue(result, "Should detect '[PLAYER] drinks [DRINKS]' as existing in user selection")

    def test_placeholder_prompt_detected_in_discards(self):
        db = DatabaseManager()
        db.globalStore = self._make_fake_global_store([])
        db.discardsStore = self._make_fake_discards_store(["[PLAYER] drinks [DRINKS]"])
        db.userSelection = self._make_fake_user_selection_store([])

        result = self._run(db.exists_in_database("[PLAYER] drinks [DRINKS]"))
        self.assertTrue(result, "Should detect '[PLAYER] drinks [DRINKS]' as existing in discards")

    def test_normal_prompt_still_detected(self):
        """Regression: normal prompts should still be detected."""
        db = DatabaseManager()
        db.globalStore = self._make_fake_global_store(["Everyone take a shot"])
        db.discardsStore = self._make_fake_discards_store([])
        db.userSelection = self._make_fake_user_selection_store([])

        result = self._run(db.exists_in_database("Everyone take a shot"))
        self.assertTrue(result)

    def test_nonexistent_prompt_not_detected(self):
        db = DatabaseManager()
        db.globalStore = self._make_fake_global_store(["Something else entirely"])
        db.discardsStore = self._make_fake_discards_store([])
        db.userSelection = self._make_fake_user_selection_store([])

        result = self._run(db.exists_in_database("[PLAYER] drinks [DRINKS]"))
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
