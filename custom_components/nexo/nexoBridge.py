"""Nexo websocket connection, resource discovery and reconnect lifecycle."""

# Preserve the existing import path used by the integration.
# pylint: disable=invalid-name
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import json
import logging
from typing import Final

import websockets
from websockets.exceptions import WebSocketException
from homeassistant.exceptions import HomeAssistantError

from .const import DEFAULT_PORT
from .nexo_analog_sensor import NexoAnalogSensor
from .nexo_binary_sensor import NexoBinarySensor
from .nexo_blind import NexoBlind
from .nexo_blind_group import NexoBlindGroup
from .nexo_gate import NexoGate
from .nexo_group_dimmer import NexoGroupDimmer
from .nexo_dimmer import NexoDimmer
from .nexo_group_led import NexoGroupLed
from .nexo_group_output import NexoGroupOutput
from .nexo_light import NexoLight
from .nexo_light_dimmable import NexoDimmableLight
from .nexo_output import NexoOutput
from .nexo_partition import NexoPartition
from .nexo_resource import NexoResource
from .nexo_temperature import NexoTemperature
from .nexo_thermostat import NexoThermostat
from .nexo_weather import NexoWeather


NEXO_RESOURCE_TYPE_TEMPERATURE = "temperature"
NEXO_RESOURCE_TYPE_OUTPUT = "output"
NEXO_RESOURCE_TYPE_SENSOR = "sensor"
NEXO_RESOURCE_TYPE_ANALOG_SENSOR = "analogsensor"
NEXO_RESOURCE_TYPE_LIGHT = "light"
NEXO_INIT_TIMEOUT = 10
NEXO_RECONNECT_TIMEOUT = 5

_LOGGER: Final = logging.getLogger(__name__)


class NexoBridge:
    """Maintain the socket and dispatch bridge messages to local resource models."""
    def __init__(self, local_ip, port=DEFAULT_PORT) -> None:
        self.ws = None
        self.resources = {}
        self.local_ip = local_ip
        self.port = port
        self.raw_data_model = {}
        self.latest_initial_data = (None, None)
        self.connected = False
        self.initialized = False

        self._loop = asyncio.get_running_loop()
        self._task = None
        self._stop_event = asyncio.Event()
        self._weather = NexoWeather()
        self._weather_listeners = set()
        self._ext_commands = set()
        self._command_listeners = set()

    async def _run(self) -> None:
        """Keep one connection alive, reconnecting until stopped."""
        url = f"ws://{self.local_ip}:{self.port}/"
        while not self._stop_event.is_set():
            try:
                _LOGGER.info("Connecting to Nexo... %s:%s", self.local_ip, self.port)
                async with websockets.connect(url) as web_socket:
                    self.ws = web_socket
                    self.connected = True
                    _LOGGER.info("Nexo integration started / reconnected")
                    if self.initialized:
                        self.refresh_resources()
                    async for message in web_socket:
                        self._safe_on_message(message)
            except (OSError, WebSocketException) as err:
                _LOGGER.warning("Nexo WS error: %s", err)
            finally:
                self.connected = False

            _LOGGER.warning(
                "Nexo WS disconnected. Reconnecting in %s s...", NEXO_RECONNECT_TIMEOUT
            )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=NEXO_RECONNECT_TIMEOUT
                )
            except TimeoutError:
                pass

    async def _stop(self) -> None:
        """Stop the connection task and wait for it to finish."""
        self._stop_event.set()
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.ws = None
        self.connected = False

    async def connect(self) -> None:
        """Start the websocket task and wait for initial resources."""
        _LOGGER.info("Initializing Nexo connection...")
        await self._stop()
        self._stop_event.clear()
        self._task = self._loop.create_task(self._run())
        await self.wait_for_initial_resources_load(NEXO_INIT_TIMEOUT)

    async def disconnect(self) -> None:
        """Stop reconnects, callbacks and background workers when an entry unloads."""
        await self._stop()
        self._weather_listeners.clear()
        self._command_listeners.clear()

    async def wait_for_initial_resources_load(self, timeout: int) -> None:
        """Wait for initial resources or fail setup after the timeout."""
        t = timeout
        while not self.initialized and t > 0:
            await asyncio.sleep(1)
            t -= 1
        if not self.initialized:
            raise TimeoutError("Timed out waiting for Nexo initial_data")

    def _safe_on_message(self, message: str) -> None:
        """Keep the receive loop alive when a single message cannot be handled."""
        try:
            self.on_message(message)
        except Exception:  # pylint: disable=broad-exception-caught
            _LOGGER.exception("Failed to handle Nexo message")

    def on_message(self, message: str) -> None:
        """Dispatch a decoded bridge message by operation."""
        _LOGGER.debug("Message: %s", message)
        try:
            json_message = json.loads(message)
        except (ValueError, TypeError):
            _LOGGER.warning("Ignoring invalid Nexo JSON message")
            return
        if not isinstance(json_message, dict):
            return

        if json_message.get("op") == "initial_data":
            self.on_message_initial_data(json_message)

        if json_message.get("op") == "data_update":
            self.on_message_data_update(json_message)

        if json_message.get("op") == "set_weather":
            self.on_message_set_weather(json_message)

        if json_message.get("op") == "modifications":
            self.on_message_modifications(json_message)

    def on_message_initial_data(self, json_message: dict) -> None:
        """Retain the latest init response and initialize or refresh resources."""
        self.latest_initial_data = (
            datetime.now(timezone.utc).isoformat(), deepcopy(json_message)
        )
        if not self.raw_data_model and json_message:
            self.raw_data_model = json_message
            self.update_resources()
        else:
            self.refresh_resources()

    def update_resources(self) -> None:
        """Create resource objects from the first initialization response."""
        self.initialized = False
        self.resources.clear()

        for resource_id in dict(self.raw_data_model["resources"]):
            self.add_resource(self.raw_data_model["resources"][resource_id])

        self.initialized = True

    def add_weather_station(self, weather_station: dict) -> None:
        """Add resources announced by a weather station."""
        if weather_station and len(weather_station.keys()) > 0:
            for resource_id in dict(weather_station):
                self.add_resource(weather_station[resource_id])

    def refresh_resources(self) -> None:
        """Attach existing resources to the current websocket."""
        for resource in self.resources.values():
            resource.web_socket = self.ws

    def on_message_data_update(self, json_message: dict) -> None:
        """Apply resource state changes and notify entity callbacks."""
        if "resources" in json_message:
            for res in json_message["resources"]:
                res_obj = json_message["resources"][res]
                resource = self.get_resource_by_id(res_obj["id"])
                if resource is not None and "state" in res_obj:
                    resource.state = res_obj["state"]
                    resource.publish_update(self._loop)

    def get_resource_by_id(self, resource_id) -> NexoResource | None:
        """Look up a resource by its bridge identifier."""
        if int(resource_id) in self.resources:
            return self.resources[int(resource_id)]
        return None

    def get_resources_by_type(self, resource_type):
        """Return resources with exactly the requested concrete type."""
        return list(
            filter(
                lambda x: x.__class__ is resource_type,
                list(self.resources.values()),
            )
        )

    def get_weather_resources(self):
        """Return the local station model, including before its first update."""
        return [self._weather]

    def register_weather_listener(self, callback) -> None:
        """Register a listener for weather observations."""
        self._weather_listeners.add(callback)

    def unregister_weather_listener(self, callback) -> None:
        """Remove a weather observation listener."""
        self._weather_listeners.discard(callback)

    def on_message_set_weather(self, message: dict) -> None:
        """Apply weather observations and notify listeners."""
        data = message.get("data")
        if not isinstance(data, dict):
            return
        self._weather.update_from_bridge(data)
        for callback in tuple(self._weather_listeners):
            self._loop.call_soon_threadsafe(callback)

    def get_ext_commands(self) -> list[str]:
        """Return the discovered external command names."""
        return sorted(self._ext_commands)

    def register_command_listener(self, callback) -> None:
        """Register a listener for newly discovered commands."""
        self._command_listeners.add(callback)

    def unregister_command_listener(self, callback) -> None:
        """Remove a command discovery listener."""
        self._command_listeners.discard(callback)

    def on_message_modifications(self, message: dict) -> None:
        """Discover external commands announced in modification messages."""
        modifications = message.get("modifications")
        if not isinstance(modifications, dict):
            return
        commands = modifications.get("ext_commands")
        if not isinstance(commands, dict):
            return
        discovered = set()
        for scope in ("to_system", "to_user"):
            names = commands.get(scope)
            if isinstance(names, dict):
                discovered.update(name for name in names if isinstance(name, str) and name)
        if discovered - self._ext_commands:
            self._ext_commands.update(discovered)
            for callback in tuple(self._command_listeners):
                self._loop.call_soon_threadsafe(callback)

    async def async_send_ext_command(self, command: str) -> None:
        """Resolve the active socket for every press, including after reconnects."""
        if self.ws is None:
            raise HomeAssistantError("Nexo bridge is disconnected")
        payload = json.dumps({"type": "ext_command", "cmd": command})
        try:
            await self.ws.send(payload)
        except (OSError, WebSocketException) as err:
            raise HomeAssistantError("Unable to send command to Nexo bridge") from err

    def add_resource(self, nexo_resource: dict) -> None:
        """Create the resource model matching the announced bridge type."""
        nexo_resource_type = nexo_resource["type"]

        match nexo_resource_type:
            case "light":
                if "state" in nexo_resource and "brightness" in nexo_resource["state"]:
                    obj = NexoDimmableLight(self.ws, **nexo_resource)
                else:
                    obj = NexoLight(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "led":
                if "state" in nexo_resource and "brightness" in nexo_resource["state"]:
                    obj = NexoDimmableLight(self.ws, **nexo_resource)
                else:
                    obj = NexoLight(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "dimmer":
                obj = NexoDimmer(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "group_led":
                obj = NexoGroupLed(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "group_output":
                obj = NexoGroupOutput(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "sensor":
                if "state" not in nexo_resource:
                    return
                obj = NexoBinarySensor(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "analogsensor":
                obj = NexoAnalogSensor(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "output":
                obj = NexoOutput(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "temperature":
                match nexo_resource["mode"]:
                    case 1:
                        obj = NexoTemperature(self.ws, **nexo_resource)
                    case 2:
                        obj = NexoThermostat(self.ws, **nexo_resource)
                    case _:
                        return
                self.resources[obj.id] = obj

            case "blind":
                obj = NexoBlind(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "group_blind":
                obj = NexoBlindGroup(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "group_dimmer":
                obj = NexoGroupDimmer(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "gate":
                obj = NexoGate(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case "partition":
                obj = NexoPartition(self.ws, **nexo_resource)
                self.resources[obj.id] = obj

            case _:
                _LOGGER.warning(
                    "Unsupported resource type %s, id %s",
                    nexo_resource_type,
                    nexo_resource["id"],
                )
