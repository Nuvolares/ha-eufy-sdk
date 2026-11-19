"""
[fork] Fresh stills for battery cameras: on a timer, and when they detect motion.

A battery camera's tile shows the newest picture the bridge keeps on disk: the last
event, the last watched frame, or the last live still. Behind an NVR the motion
pushes often carry no thumbnail, so without help the tile can be hours old.

- Every `still_refresh_minutes`, each idle battery camera is woken briefly for one
  live still (`/snapshot?mode=live`); the bridge keeps it, so the tile shows it.
- With `motion_still` on, a motion/person/vehicle/... detection takes one live still
  from that camera (at most once a minute). It is awake for the event anyway.

Every still request goes through one lock, one lens at a time with a pause between
the lenses of a camera: an NVR serves only a few calls at once, and two lenses of a
dual-lens camera requested together come back as one combined picture.
"""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import aiohttp
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    CONF_HOST,
    CONF_MOTION_STILL,
    CONF_PORT,
    CONF_STILL_REFRESH,
    DEFAULT_MOTION_STILL,
    DEFAULT_STILL_REFRESH_MIN,
    EVENT_TYPE,
    LOGGER,
)
from .entity import device_offline
from .preload import on_battery_power
from .pushmap import MOTION_EVENTS
from .snapshot_policy import SNAPSHOT_POLICY_LIVE, snapshot_url

if TYPE_CHECKING:
    from datetime import datetime

    from homeassistant.core import Event, HomeAssistant

    from .data import EufySdkConfigEntry

# A battery camera behind an NVR takes 10-20 s to wake, and the bridge tries twice.
REFRESH_TIMEOUT_S = 90
# Let one lens's call close before the next lens of the same camera is requested.
LENS_GAP_S = 10
# At most one motion still per camera in this window.
MOTION_COOLDOWN_S = 60

Request = tuple[str, str, str | None]  # (key, camera serial, snapshot path)


def refresh_targets(devices: dict[str, dict[str, Any]] | None) -> list[str]:
    """Battery cameras worth a fresh still: reachable and not already streaming."""
    return [
        sn
        for sn, record in (devices or {}).items()
        if record.get("stream")
        and on_battery_power(record)
        and not record.get("streaming")
        and not device_offline(record)
    ]


def camera_requests(sn: str, record: dict[str, Any]) -> list[Request]:
    """
    One request per lens of a camera.

    A camera with a second lens (the PTZ lens of an S4) lists it in `views`; each
    lens has its own still. The main lens uses the default path (None).
    """
    return [
        (sn, sn, None),
        *(
            (view["id"], sn, view["snapshot"])
            for view in record.get("views") or []
            if view.get("view") and view.get("id") and view.get("snapshot")
        ),
    ]


def refresh_requests(devices: dict[str, dict[str, Any]] | None) -> list[Request]:
    """One request per lens of every target camera."""
    return [
        request
        for sn in refresh_targets(devices)
        for request in camera_requests(sn, (devices or {})[sn])
    ]


def motion_requests(
    data: dict[str, Any], devices: dict[str, dict[str, Any]] | None
) -> list[Request]:
    """The still requests a bridge event asks for: none unless it is motion."""
    sn = data.get("deviceSn")
    if data.get("event") not in MOTION_EVENTS or not isinstance(sn, str):
        return []
    record = (devices or {}).get(sn)
    if not record or not refresh_targets({sn: record}):
        return []
    return camera_requests(sn, record)


class Cooldown:
    """Lets a key through at most once per `seconds`."""

    def __init__(self, seconds: float) -> None:
        """Start with every key free."""
        self._seconds = seconds
        self._last: dict[str, float] = {}

    def allow(self, key: str, now: float) -> bool:
        """Whether `key` may go now; records the pass."""
        last = self._last.get(key)
        if last is not None and now - last < self._seconds:
            return False
        self._last[key] = now
        return True


async def request_stills(
    session: aiohttp.ClientSession,
    host: str,
    port: int,
    requests: list[Request],
    *,
    lens_gap_s: float = LENS_GAP_S,
) -> dict[str, bool]:
    """Ask the bridge for one live still per request, one after another."""
    results: dict[str, bool] = {}
    timeout = aiohttp.ClientTimeout(total=REFRESH_TIMEOUT_S)
    previous_sn: str | None = None
    for key, sn, path in requests:
        if sn == previous_sn and lens_gap_s > 0:
            await asyncio.sleep(lens_gap_s)
        previous_sn = sn
        url = snapshot_url(host, port, sn, SNAPSHOT_POLICY_LIVE, snapshot_path=path)
        try:
            async with session.get(url, timeout=timeout) as resp:
                await resp.read()
                results[key] = resp.status == HTTPStatus.OK
        except (TimeoutError, aiohttp.ClientError, OSError) as err:
            LOGGER.debug("live still for %s failed: %s", key, err)
            results[key] = False
    return results


async def refresh_stills(
    session: aiohttp.ClientSession,
    host: str,
    port: int,
    devices: dict[str, dict[str, Any]] | None,
    *,
    lens_gap_s: float = LENS_GAP_S,
) -> dict[str, bool]:
    """One live still per lens of every target camera."""
    return await request_stills(
        session, host, port, refresh_requests(devices), lens_gap_s=lens_gap_s
    )


def async_setup_still_refresh(hass: HomeAssistant, entry: EufySdkConfigEntry) -> None:
    """Start the timed refresh and the motion still, as the options ask."""
    minutes = int(entry.options.get(CONF_STILL_REFRESH, DEFAULT_STILL_REFRESH_MIN) or 0)
    on_motion = bool(entry.options.get(CONF_MOTION_STILL, DEFAULT_MOTION_STILL))
    if minutes <= 0 and not on_motion:
        return
    lock = asyncio.Lock()
    refreshing = False

    async def _take(requests: list[Request], label: str) -> None:
        if not requests:
            return
        async with lock:
            results = await request_stills(
                async_get_clientsession(hass),
                entry.data[CONF_HOST],
                entry.data[CONF_PORT],
                requests,
            )
        LOGGER.debug("%s: %s", label, results)

    async def _refresh() -> None:
        nonlocal refreshing
        refreshing = True
        try:
            devices = entry.runtime_data.coordinator.data
            await _take(refresh_requests(devices), "battery still refresh")
        finally:
            refreshing = False

    @callback
    def _tick(_now: datetime) -> None:
        if refreshing:  # the previous round is still waking cameras
            return
        entry.async_create_background_task(hass, _refresh(), "eufy_sdk still refresh")

    if minutes > 0:
        entry.async_on_unload(
            async_track_time_interval(hass, _tick, timedelta(minutes=minutes))
        )

    if not on_motion:
        return
    cooldown = Cooldown(MOTION_COOLDOWN_S)

    @callback
    def _on_event(event: Event) -> None:
        requests = motion_requests(event.data, entry.runtime_data.coordinator.data)
        if not requests or not cooldown.allow(requests[0][1], time.monotonic()):
            return
        entry.async_create_background_task(
            hass, _take(requests, "motion still"), "eufy_sdk motion still"
        )

    entry.async_on_unload(hass.bus.async_listen(EVENT_TYPE, _on_event))
