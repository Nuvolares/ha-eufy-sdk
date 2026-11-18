# ruff: noqa: ANN201, D100, D101, D102, INP001, PT009
"""[fork] The live-codec option picks the bridge's H.264 twin where it applies."""

import unittest
from unittest.mock import Mock

from custom_components.eufy_sdk.camera import EufySdkCamera
from custom_components.eufy_sdk.const import (
    LIVE_CODEC_H264_ALL,
    LIVE_CODEC_H264_BATTERY,
    LIVE_CODEC_NATIVE,
)

BATTERY = "T8162P0000000001"
MAINS = "T8E00P0000000001"
OLD_BRIDGE = "T8172P0000000001"


def _camera(sn: str, live_codec: str, view: dict | None = None) -> EufySdkCamera:
    coordinator = Mock()
    coordinator.data = {
        BATTERY: {"power": "battery", "stream": "/s", "h264Suffix": "_h264"},
        MAINS: {"power": "wired", "stream": "/s", "h264Suffix": "_h264"},
        # An upstream bridge advertises no twin.
        OLD_BRIDGE: {"power": "battery", "stream": "/s"},
    }
    return EufySdkCamera(
        coordinator, sn, "10.0.0.5", 3000, 8554, view=view, live_codec=live_codec
    )


class LiveCodecTests(unittest.IsolatedAsyncioTestCase):
    async def _source(self, sn: str, codec: str, view: dict | None = None) -> str:
        return await _camera(sn, codec, view).stream_source()

    async def test_battery_mode_twins_only_battery_cameras(self):
        mode = LIVE_CODEC_H264_BATTERY
        self.assertEqual(
            await self._source(BATTERY, mode), f"rtsp://10.0.0.5:8554/{BATTERY}_h264"
        )
        self.assertEqual(
            await self._source(MAINS, mode), f"rtsp://10.0.0.5:8554/{MAINS}"
        )

    async def test_all_mode_twins_every_camera(self):
        self.assertEqual(
            await self._source(MAINS, LIVE_CODEC_H264_ALL),
            f"rtsp://10.0.0.5:8554/{MAINS}_h264",
        )

    async def test_native_mode_keeps_the_native_stream(self):
        self.assertEqual(
            await self._source(BATTERY, LIVE_CODEC_NATIVE),
            f"rtsp://10.0.0.5:8554/{BATTERY}",
        )

    async def test_a_bridge_without_twins_keeps_the_native_stream(self):
        self.assertEqual(
            await self._source(OLD_BRIDGE, LIVE_CODEC_H264_ALL),
            f"rtsp://10.0.0.5:8554/{OLD_BRIDGE}",
        )

    async def test_a_secondary_view_gets_its_own_twin(self):
        view = {"view": 1, "id": f"{MAINS}_view_1", "snapshot": "/v1"}
        self.assertEqual(
            await self._source(MAINS, LIVE_CODEC_H264_ALL, view),
            f"rtsp://10.0.0.5:8554/{MAINS}_view_1_h264",
        )


if __name__ == "__main__":
    unittest.main()
