from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


SCRIPT = Path.home() / ".omon/workspace/runtime/scripts/katok_group_digest.py"
SPEC = importlib.util.spec_from_file_location("katok_group_digest", SCRIPT)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_global_page_budget_is_fair_and_lossless() -> None:
    allocations = MODULE.allocate_page_limits([1000, 37, 18], 300, 1000)

    assert allocations == [245, 37, 18]
    assert sum(allocations) == 300
    remaining = [
        count - page for count, page in zip([1000, 37, 18], allocations, strict=True)
    ]
    assert remaining == [755, 0, 0]


def test_global_page_budget_respects_per_chat_cap() -> None:
    allocations = MODULE.allocate_page_limits([2000, 2000], 3000, 1000)

    assert allocations == [1000, 1000]
    assert sum(allocations) == 2000


def test_global_page_budget_keeps_all_unread_messages_queued() -> None:
    counts = [1000, 37, 18]
    allocations = MODULE.allocate_page_limits(counts, 300, 1000)

    pending_counts = [
        count - page for count, page in zip(counts, allocations, strict=True)
    ]
    pending_flags = [remaining > 0 for remaining in pending_counts]

    assert pending_counts == [755, 0, 0]
    assert pending_flags == [True, False, False]


def test_checkpoint_advances_only_to_last_included_message() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        "SELECT 'message-245' AS message_id, '2026-08-30T12:34:56+00:00' AS timestamp"
    ).fetchone()
    assert row is not None

    checkpoint = MODULE.checkpoint_for_page(
        "busy chat", row, 755, "2026-08-30T12:35:00+00:00"
    )

    assert checkpoint == {
        "chat_name": "busy chat",
        "last_summarized_message_id": "message-245",
        "last_summarized_timestamp": "2026-08-30T12:34:56+00:00",
        "last_summarized_at": "2026-08-30T12:35:00+00:00",
        "pending": True,
        "pending_count": 755,
    }
