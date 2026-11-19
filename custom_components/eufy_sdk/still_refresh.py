"""
[fork] Refresh battery cameras' stills on a timer.

A battery camera's tile shows the newest picture the bridge keeps on disk: the last
event or the last watched frame. Without motion that can be hours old. Every
`still_refresh_minutes` this asks the bridge for one live still per battery camera
(`/snapshot?mode=live`), which wakes the camera briefly; the bridge keeps that still,
so the tile shows it from then on. Cameras go one at a time, because an NVR serves
only a few calls at once.
"""

from __future__ import annotations

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

# A battery camera behind an NVR takes 10-20 s to wake; leave room for the burst.
REFRESH_TIMEOUT_S = 90


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


async def refresh_stills(
    session: aiohttp.ClientSession,
    host: str,
    port: int,
    devices: dict[str, dict[str, Any]] | None,
) -> dict[str, bool]:
    """Ask the bridge for one live still per target camera, one after another."""
    results: dict[str, bool] = {}
    timeout = aiohttp.ClientTimeout(total=REFRESH_TIMEOUT_S)
    for sn in refresh_targets(devices):
        url = snapshot_url(host, port, sn, SNAPSHOT_POLICY_LIVE)
        try:
            async with session.get(url, timeout=timeout) as resp:
                await resp.read()
                results[sn] = resp.status == HTTPStatus.OK
        except (TimeoutError, aiohttp.ClientError, OSError) as err:
            LOGGER.debug("still refresh for %s failed: %s", sn, err)
            results[sn] = False
    return results


def async_setup_still_refresh(hass: HomeAssistant, entry: EufySdkConfigEntry) -> None:
    """Start the timed still refresh when the option is above zero."""
    minutes = int(entry.options.get(CONF_STILL_REFRESH, DEFAULT_STILL_REFRESH_MIN) or 0)
    if minutes <= 0:
        return
    running = False

    async def _run() -> None:
        nonlocal running
        running = True
        try:
            results = await refresh_stills(
                async_get_clientsession(hass),
                entry.data[CONF_HOST],
                entry.data[CONF_PORT],
                entry.runtime_data.coordinator.data,
            )
            LOGGER.debug("battery still refresh: %s", results)
        finally:
            running = False

    @callback
    def _tick(_now: datetime) -> None:
        if running:  # the previous round is still waking cameras
            return
        entry.async_create_background_task(hass, _run(), "eufy_sdk still refresh")

    entry.async_on_unload(
        async_track_time_interval(hass, _tick, timedelta(minutes=minutes))
    )
