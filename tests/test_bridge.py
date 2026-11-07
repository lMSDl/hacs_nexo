"""Exercise the actual bridge and entry lifecycle with fake network and HA APIs."""

import asyncio
from enum import StrEnum
import importlib
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch


def _load_modules():
    root = Path(__file__).resolve().parents[1] / "custom_components" / "nexo"
    name = "_nexo_bridge_tests"
    package = ModuleType(name)
    package.__path__ = [str(root)]
    stubs = {name: package}
    for module in ("homeassistant", "homeassistant.config_entries", "homeassistant.const",
                   "homeassistant.core", "homeassistant.exceptions", "homeassistant.helpers", "websockets"):
        stubs[module] = ModuleType(module)
    stubs["homeassistant.helpers"].config_validation = SimpleNamespace(config_entry_only_config_schema=lambda domain: {})
    stubs["homeassistant.config_entries"].ConfigEntry = SimpleNamespace
    stubs["homeassistant.core"].HomeAssistant = SimpleNamespace
    stubs["homeassistant.const"].CONF_HOST = "host"
    stubs["homeassistant.const"].CONF_PORT = "port"
    stubs["homeassistant.const"].Platform = StrEnum("Platform", {
        x: x.lower() for x in ("ALARM_CONTROL_PANEL", "BINARY_SENSOR", "CLIMATE",
                              "COVER", "LIGHT", "LOCK", "SENSOR", "SWITCH", "WEATHER", "BUTTON")
    })
    stubs["homeassistant.exceptions"].HomeAssistantError = type("HomeAssistantError", (Exception,), {})
    stubs["homeassistant.exceptions"].ConfigEntryNotReady = type("ConfigEntryNotReady", (Exception,), {})
    stubs["websockets"].exceptions = SimpleNamespace(
        WebSocketException=type("WebSocketException", (Exception,), {}),
        ConnectionClosed=type("ConnectionClosed", (Exception,), {}),
    )
    stubs["websockets"].connect = Mock()
    with patch.dict(sys.modules, stubs):
        bridge = importlib.import_module(f"{name}.nexoBridge")
        spec = importlib.util.spec_from_file_location(f"{name}.entry", root / "__init__.py")
        entry = importlib.util.module_from_spec(spec)
        entry.__package__ = name
        spec.loader.exec_module(entry)
    return bridge, entry


BRIDGE, ENTRY = _load_modules()


class FakeSocket:
    """Async context manager and iterator standing in for a websockets connection."""

    def __init__(self, messages=()):
        self.send = AsyncMock()
        self._messages = list(messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def __aiter__(self):
        return self._messages_then_idle()

    async def _messages_then_idle(self):
        for message in self._messages:
            yield message
        await asyncio.Event().wait()


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bridge = BRIDGE.NexoBridge("192.0.2.1")

    async def asyncTearDown(self):
        await self.bridge.disconnect()

    async def test_factory_keeps_led_classification_and_adds_groups(self):
        types = ("light", "led", "dimmer", "group_dimmer", "group_led", "group_output")
        for rid, kind in enumerate(types, 1):
            self.bridge.add_resource({"id": rid, "name": kind, "type": kind,
                                      "state": {"is_on": 1, "brightness": 44}, "ios": []})
        self.assertIs(type(self.bridge.resources[1]), BRIDGE.NexoDimmableLight)
        self.assertIs(type(self.bridge.resources[2]), BRIDGE.NexoDimmableLight)
        self.assertIs(type(self.bridge.resources[3]), BRIDGE.NexoDimmer)
        self.assertIs(type(self.bridge.resources[4]), BRIDGE.NexoGroupDimmer)
        self.assertIs(type(self.bridge.resources[5]), BRIDGE.NexoGroupLed)
        self.assertIs(type(self.bridge.resources[6]), BRIDGE.NexoGroupOutput)
        self.assertEqual(len(self.bridge.get_resources_by_type(BRIDGE.NexoDimmer)), 1)

    async def test_reconnect_replaces_resource_sockets(self):
        self.bridge.add_resource({"id": 1, "name": "light", "type": "light", "state": {"is_on": 1}})
        resource = self.bridge.resources[1]
        self.bridge.initialized = True
        sock = FakeSocket()
        with patch.object(BRIDGE.websockets, "connect", return_value=sock):
            self.bridge._task = asyncio.create_task(self.bridge._run())
            for _ in range(5):
                await asyncio.sleep(0)
            self.assertTrue(self.bridge.connected)
            self.assertIs(resource.web_socket, sock)
            await self.bridge.disconnect()
        self.assertFalse(self.bridge.connected)

    async def test_incoming_messages_are_dispatched(self):
        update = {"op": "data_update", "resources": {"1": {"id": 1, "state": {"is_on": 0}}}}
        self.bridge.add_resource({"id": 1, "name": "light", "type": "light", "state": {"is_on": 1}})
        with patch.object(BRIDGE.websockets, "connect", return_value=FakeSocket([json.dumps(update)])):
            self.bridge._task = asyncio.create_task(self.bridge._run())
            for _ in range(5):
                await asyncio.sleep(0)
            await self.bridge.disconnect()
        self.assertEqual(self.bridge.resources[1].state, {"is_on": 0})

    async def test_malformed_message_does_not_stop_the_receive_loop(self):
        broken = {"op": "data_update", "resources": {"1": {"state": {}}}}
        update = {"op": "data_update", "resources": {"1": {"id": 1, "state": {"is_on": 0}}}}
        self.bridge.add_resource({"id": 1, "name": "light", "type": "light", "state": {"is_on": 1}})
        sock = FakeSocket([json.dumps(broken), json.dumps(update)])
        with patch.object(BRIDGE.websockets, "connect", return_value=sock):
            self.bridge._task = asyncio.create_task(self.bridge._run())
            for _ in range(5):
                await asyncio.sleep(0)
            self.assertFalse(self.bridge._task.done())
            await self.bridge.disconnect()
        self.assertEqual(self.bridge.resources[1].state, {"is_on": 0})

    async def test_command_press_resolves_active_socket_and_escapes_json(self):
        first, second = FakeSocket(), FakeSocket()
        self.bridge.ws = first
        await self.bridge.async_send_ext_command('gate "open"')
        self.bridge.ws = second
        await self.bridge.async_send_ext_command("close")
        self.assertEqual(json.loads(first.send.call_args.args[0]), {"type": "ext_command", "cmd": 'gate "open"'})
        self.assertEqual(json.loads(second.send.call_args.args[0])["cmd"], "close")

    async def test_command_failure_propagates(self):
        self.bridge.ws = FakeSocket()
        self.bridge.ws.send.side_effect = BrokenPipeError()
        with self.assertRaises(BRIDGE.HomeAssistantError):
            await self.bridge.async_send_ext_command("open")

    async def test_resource_send_failure_is_logged_not_raised(self):
        sock = FakeSocket()
        sock.send.side_effect = BrokenPipeError()
        resource = BRIDGE.NexoResource(sock, id=1, name="out", state={})
        with patch.object(resource._LOGGER, "warning") as warning:
            await resource._async_send("{}")
        warning.assert_called_once()

    async def test_late_commands_dispatch_once_and_weather_stays_local(self):
        commands = Mock()
        weather = Mock()
        self.bridge.register_command_listener(commands)
        self.bridge.register_weather_listener(weather)
        message = {"op": "modifications", "modifications": {"ext_commands": {"to_system": {"open": {}}, "to_user": {"close": {}}}}}
        self.bridge.on_message(json.dumps(message))
        self.bridge.on_message(json.dumps(message))
        self.bridge.on_message(json.dumps({"op": "set_weather", "data": {"devices/0": {"air": {"temperature": 21.5}}}}))
        await asyncio.sleep(0)
        self.assertEqual(self.bridge.get_ext_commands(), ["close", "open"])
        commands.assert_called_once()
        weather.assert_called_once()
        self.assertEqual(self.bridge.get_weather_resources()[0].temperature_c, 21.5)
        self.bridge.unregister_command_listener(commands)
        self.bridge.unregister_weather_listener(weather)
        self.assertFalse(self.bridge._command_listeners or self.bridge._weather_listeners)

    async def test_sync_and_async_resource_callbacks_complete_without_errors(self):
        resource = BRIDGE.NexoResource(None, id=1, name="sensor", state={})
        calls = []
        def sync():
            calls.append("sync")
            resource.remove_callback(sync)
        async def asynchronous():
            calls.append("async")
        resource.register_callback(sync)
        resource.register_callback(asynchronous)
        with patch.object(resource._LOGGER, "exception") as errors:
            resource.publish_update(asyncio.get_running_loop())
            for _ in range(5):
                await asyncio.sleep(0)
            self.assertCountEqual(calls, ["sync", "async"])
            errors.assert_not_called()

    async def test_initialization_timeout_is_not_success(self):
        with self.assertRaises(TimeoutError):
            await self.bridge.wait_for_initial_resources_load(0)

    async def test_disconnect_interrupts_reconnect_wait(self):
        with patch.object(BRIDGE.websockets, "connect", side_effect=OSError("down")):
            self.bridge._task = asyncio.create_task(self.bridge._run())
            for _ in range(3):
                await asyncio.sleep(0)
            await asyncio.wait_for(self.bridge.disconnect(), timeout=1)
        self.assertIsNone(self.bridge._task)
        self.assertFalse(self.bridge.connected)


class EntryTests(unittest.IsolatedAsyncioTestCase):
    def make_hass(self, unload=True):
        client = SimpleNamespace(disconnect=AsyncMock(), connect=AsyncMock())
        hass = SimpleNamespace(data={"nexo": {"test": client}}, config_entries=SimpleNamespace(
            async_unload_platforms=AsyncMock(return_value=unload),
            async_forward_entry_setups=AsyncMock(), async_reload=AsyncMock()))
        entry = SimpleNamespace(entry_id="test", title="Karta MM", data={"host": "192.0.2.1"},
                                async_on_unload=Mock(), add_update_listener=Mock())
        return hass, entry, client

    async def test_failed_unload_retains_bridge_and_running_connection(self):
        hass, entry, client = self.make_hass(unload=False)
        self.assertFalse(await ENTRY.async_unload_entry(hass, entry))
        self.assertIs(hass.data["nexo"]["test"], client)
        client.disconnect.assert_not_awaited()

    async def test_successful_unload_stops_bridge(self):
        hass, entry, client = self.make_hass()
        self.assertTrue(await ENTRY.async_unload_entry(hass, entry))
        self.assertNotIn("test", hass.data["nexo"])
        client.disconnect.assert_awaited_once()

    async def test_timed_out_setup_stops_bridge_and_requests_retry(self):
        hass, entry, client = self.make_hass()
        client.connect.side_effect = TimeoutError()
        with patch.object(ENTRY, "NexoBridge", return_value=client):
            with self.assertRaises(ENTRY.ConfigEntryNotReady):
                await ENTRY.async_setup_entry(hass, entry)
        client.disconnect.assert_awaited_once()
        hass.config_entries.async_forward_entry_setups.assert_not_awaited()
        self.assertNotIn("test", hass.data["nexo"])

    async def test_setup_renames_old_placeholder_title(self):
        hass, entry, client = self.make_hass()
        entry.title = "Name of the device"
        hass.config_entries.async_update_entry = Mock()
        with patch.object(ENTRY, "NexoBridge", return_value=client):
            self.assertTrue(await ENTRY.async_setup_entry(hass, entry))
        hass.config_entries.async_update_entry.assert_called_once_with(entry, title="Karta MM")

    async def test_setup_passes_custom_port_to_bridge(self):
        hass, entry, client = self.make_hass()
        entry.data["port"] = 9000
        with patch.object(ENTRY, "NexoBridge", return_value=client) as factory:
            self.assertTrue(await ENTRY.async_setup_entry(hass, entry))
        factory.assert_called_once_with("192.0.2.1", 9000)

    async def test_options_listener_registered_and_reloads_entry(self):
        hass, entry, client = self.make_hass()
        with patch.object(ENTRY, "NexoBridge", return_value=client):
            self.assertTrue(await ENTRY.async_setup_entry(hass, entry))
        entry.add_update_listener.assert_called_once_with(ENTRY.async_reload_entry)
        await ENTRY.async_reload_entry(hass, entry)
        hass.config_entries.async_reload.assert_awaited_once_with("test")


if __name__ == "__main__":
    unittest.main()
