"""
[fork] Fresh stills for battery cameras on a timer.

A battery camera's tile shows the newest picture the bridge keeps on disk: the last
event, the last watched frame, or the last live still. Behind an NVR the motion
pushes often carry no thumbnail, so without help the tile can be hours old.

Every `still_refresh_minutes`, each idle battery camera is woken briefly for one
live still (`/snapshot?mode=live`); the bridge keeps it, so the tile shows it.

The requests go one lens at a time with a pause between the lenses of a camera: an
NVR serves only a few calls at once, and two lenses of a dual-lens camera requested
together come back as one combined picture.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import aiohttp
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    CONF_HOST,
    CONF_PORT,
    CONF_STILL_REFRESH,
    DEFAULT_STILL_REFRESH_MIN,
    LOGGER,
)
from .entity import device_offline
from .preload import on_battery_power
from .snapshot_policy import SNAPSHOT_POLICY_LIVE, snapshot_url

if TYPE_CHECKING:
    from datetime import datetime

    from homeassistant.core import HomeAssistant

    from .data import EufySdkConfigEntry

# A battery camera behind an NVR takes 10-20 s to wake, and the bridge tries twice.
REFRESH_TIMEOUT_S = 90
# Let one lens's call close before the next lens of the same camera is requested.
LENS_GAP_S = 10

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
    """Start the timed refresh when the option asks for one."""
    minutes = int(entry.options.get(CONF_STILL_REFRESH, DEFAULT_STILL_REFRESH_MIN) or 0)
    if minutes <= 0:
        return
    refreshing = False

    async def _refresh() -> None:
        nonlocal refreshing
        refreshing = True
        try:
            results = await refresh_stills(
                async_get_clientsession(hass),
                entry.data[CONF_HOST],
                entry.data[CONF_PORT],
                entry.runtime_data.coordinator.data,
            )
            LOGGER.debug("battery still refresh: %s", results)
        finally:
            refreshing = False

    @callback
    def _tick(_now: datetime) -> None:
        if refreshing:  # the previous round is still waking cameras
            return
        entry.async_create_background_task(hass, _refresh(), "eufy_sdk still refresh")

    entry.async_on_unload(
        async_track_time_interval(hass, _tick, timedelta(minutes=minutes))
    )
