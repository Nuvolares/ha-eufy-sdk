# ruff: noqa: ANN201, D101, D102, INP001, PT009
"""[fork] Fresh stills wake only reachable, idle battery cameras, one lens at a time."""

import asyncio
import unittest
from typing import Any, Self
from unittest.mock import AsyncMock, patch

from custom_components.eufy_sdk.still_refresh import (
    LENS_GAP_S,
    refresh_stills,
    refresh_targets,
)

SLEEP = "custom_components.eufy_sdk.still_refresh.asyncio.sleep"

SOLAR = "T8172P0000000001"
WIRED = "T8E00P0000000001"
WATCHED = "T8162P0000000001"
OFFLINE = "T8162P0000000002"
SENSOR = "T8900P0000000001"

DEVICES: dict[str, dict[str, Any]] = {
    SOLAR: {"codec": "camera", "power": "battery", "stream": "/stream/s"},
    WIRED: {"codec": "camera", "power": "wired", "stream": "/stream/w"},
    WATCHED: {
        "codec": "camera",
        "power": "battery",
        "stream": "/stream/x",
        "streaming": True,
    },
    OFFLINE: {
        "codec": "camera",
        "power": "battery",
        "stream": "/stream/o",
        "availability": "unavailable",
    },
    SENSOR: {"codec": "sensor", "state": {"deviceStatus": True}},
}


class _Response:
    def __init__(self, session: "_Session", status: int) -> None:
        self._session = session
        self.status = status

    async def __aenter__(self) -> Self:
        self._session.in_flight += 1
        self._session.peak = max(self._session.peak, self._session.in_flight)
        await asyncio.sleep(0)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self._session.in_flight -= 1

    async def read(self) -> bytes:
        return b"jpeg"


class _Session:
    def __init__(self, status: int = 200) -> None:
        self.urls: list[str] = []
        self.status = status
        self.in_flight = 0
        self.peak = 0

    def get(self, url: str, **_kwargs: object) -> _Response:
        self.urls.append(url)
        return _Response(self, self.status)


class StillRefreshTests(unittest.IsolatedAsyncioTestCase):
    def test_only_reachable_idle_battery_cameras_are_targets(self):
        self.assertEqual(refresh_targets(DEVICES), [SOLAR])
        self.assertEqual(refresh_targets(None), [])

    async def test_each_target_gets_one_live_still_one_at_a_time(self):
        devices = {**DEVICES, "T8162P0000000003": {**DEVICES[SOLAR]}}
        session = _Session()
        results = await refresh_stills(session, "10.0.0.5", 3000, devices)
        self.assertEqual(
            session.urls,
            [
                f"http://10.0.0.5:3000/snapshot/{SOLAR}?mode=live",
                "http://10.0.0.5:3000/snapshot/T8162P0000000003?mode=live",
            ],
        )
        self.assertEqual(session.peak, 1)
        self.assertEqual(results, {SOLAR: True, "T8162P0000000003": True})

    async def test_a_second_lens_gets_its_own_live_still(self):
        views = [
            {"view": 0, "id": SOLAR, "snapshot": f"/snapshot/{SOLAR}"},
            {
                "view": 1,
                "id": f"{SOLAR}_view_1",
                "snapshot": f"/snapshot/{SOLAR}?view=1",
            },
        ]
        devices = {SOLAR: {**DEVICES[SOLAR], "views": views}}
        session = _Session()
        with patch(SLEEP, new=AsyncMock()) as sleep:
            results = await refresh_stills(session, "h", 1, devices)
        self.assertEqual(
            session.urls,
            [
                f"http://h:1/snapshot/{SOLAR}?mode=live",
                f"http://h:1/snapshot/{SOLAR}?view=1&mode=live",
            ],
        )
        self.assertEqual(results, {SOLAR: True, f"{SOLAR}_view_1": True})
        # The second lens waits for the first lens's call to close: no combined image.
        gaps = [c for c in sleep.await_args_list if c.args == (LENS_GAP_S,)]
        self.assertEqual(len(gaps), 1)

    async def test_a_failed_still_is_reported_not_raised(self):
        results = await refresh_stills(_Session(502), "h", 1, DEVICES)
        self.assertEqual(results, {SOLAR: False})


if __name__ == "__main__":
    unittest.main()
