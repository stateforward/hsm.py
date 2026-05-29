from __future__ import annotations

from typing import Any, assert_type, cast

import hsm


typed_event = hsm.Event("typed", {"count": 1})
assert_type(typed_event, hsm.Event[dict[str, int]])
assert_type(typed_event.Data, dict[str, int])
assert_type(typed_event.data, dict[str, int])

keyword_event = hsm.Event(name="typed", data=1)
assert_type(keyword_event, hsm.Event[int])
assert_type(keyword_event.Data, int)

empty_event = hsm.Event("empty")
assert_type(empty_event, hsm.Event[Any])
assert_type(empty_event.Data, Any)

with_data_event = empty_event.WithData("payload")
assert_type(with_data_event, hsm.Event[str])
assert_type(with_data_event.Data, str)

with_data_and_id_event = empty_event.WithDataAndID(1.5, "event-1")
assert_type(with_data_and_id_event, hsm.Event[float])
assert_type(with_data_and_id_event.Data, float)

snake_payload = cast(tuple[str], ("payload",))
snake_data_event = empty_event.with_data(snake_payload)
assert_type(snake_data_event, hsm.Event[tuple[str]])
assert_type(snake_data_event.Data, tuple[str])
