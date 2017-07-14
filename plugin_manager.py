import asyncio
import collections
import importlib.machinery
import inspect
import logging
import pathlib
from types import ModuleType

from configuration_manager import ConfigurationManager
from packet_parser import PacketParser
from utilities import detect_overrides, DotDict, recursive_dictionary_update


class BaseMeta(type):
    def __new__(mcs, name, bases, clsdict):
        for key, value in clsdict.items():
            if callable(value) and (value.__name__.startswith("on_") or
                                    hasattr(value, "_command")):
                clsdict[key] = asyncio.coroutine(value)
        c = type.__new__(mcs, name, bases, clsdict)
        return c


class BasePlugin(metaclass=BaseMeta):
    """
    Defines an interface for all plugins to inherit from. Note that the init
    method should generally not be overrode; all setup work should be done in
    activate() if possible. If you do override __init__, remember to super()!

    Note that only one instance of each plugin will be instantiated for *all*
    connected clients. self.connection will be changed by the plugin
    manager to the current connection.

    You may access the factory if necessary via self.factory.connections
    to access other clients, but this "Is Not A Very Good Idea" (tm)

    `name` *must* be defined in child classes or else the plugin manager will
    complain quite thoroughly.
    """

    name = "Base Plugin"
    description = "The common class for all plugins to inherit from."
    version = ".1"
    depends = ()
    default_config = None
    plugins = DotDict({})
    auto_activate = True

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.plugin_config = self.config.get_plugin_config(self.name)
        if isinstance(self.default_config, collections.Mapping):
            temp = recursive_dictionary_update(self.default_config,
                                               self.plugin_config)
            self.plugin_config.update(temp)

        else:
            self.plugin_config = self.default_config

    def activate(self):
        pass

    def deactivate(self):
        pass

    def on_protocol_request(self, data, connection):
        """Packet type: 0 """

        return True

    def on_protocol_response(self, data, connection):
        """Packet type: 1 """

        return True

    def on_server_disconnect(self, data, connection):
        """Packet type: 2 """

        return True

    def on_connect_success(self, data, connection):
        """Packet type: 3 """

        return True

    def on_connect_failure(self, data, connection):
        """Packet type: 4 """

        return True

    def on_handshake_challenge(self, data, connection):
        """Packet type: 5 """

        return True

    def on_chat_received(self, data, connection):
        """Packet type: 6 """

        return True

    def on_universe_time_update(self, data, connection):
        """Packet type: 7 """

        return True

    def on_celestial_response(self, data, connection):
        """Packet type: 8 """

        return True

    def on_player_warp_result(self, data, connection):
        """Packet type: 9 """

        return True

    def on_planet_type_update(self, data, connection):
        """Packet type: 10 """

        return True

    def on_pause(self, data, connection):
        """Packet type: 11 """

        return True

    def on_client_connect(self, data, connection):
        """Packet type: 12 """

        return True

    def on_client_disconnect_request(self, data, connection):
        """Packet type: 13 """

        return True

    def on_handshake_response(self, data, connection):
        """Packet type: 14 """

        return True

    def on_player_warp(self, data, connection):
        """Packet type: 15 """

        return True

    def on_fly_ship(self, data, connection):
        """Packet type: 16 """

        return True

    def on_chat_sent(self, data, connection):
        """Packet type: 17 """

        return True

    def on_celestial_request(self, data, connection):
        """Packet type: 18 """

        return True

    def on_client_context_update(self, data, connection):
        """Packet type: 19 """

        return True

    def on_world_start(self, data, connection):
        """Packet type: 20 """

        return True

    def on_world_stop(self, data, connection):
        """Packet type: 21 """

        return True

    def on_world_layout_update(self, data, connection):
        """Packet type: 22 """

        return True

    def on_world_parameters_update(self, data, connection):
        """Packet type: 23 """

        return True

    def on_central_structure_update(self, data, connection):
        """Packet type: 24 """

        return True

    def on_tile_array_update(self, data, connection):
        """Packet type: 25 """

        return True

    def on_tile_update(self, data, connection):
        """Packet type: 26 """

        return True

    def on_tile_liquid_update(self, data, connection):
        """Packet type: 27 """

        return True

    def on_tile_damage_update(self, data, connection):
        """Packet type: 28 """

        return True

    def on_tile_modification_failure(self, data, connection):
        """Packet type: 29 """

        return True

    def on_give_item(self, data, connection):
        """Packet type: 30 """

        return True

    def on_environment_update(self, data, connection):
        """Packet type: 31 """

        return True

    def on_update_tile_protection(self, data, connection):
        """Packet type: 32 """

        return True

    def on_set_dungeon_gravity(self, data, connection):
        """Packet type: 33 """

        return True

    def on_set_dungeon_breathable(self, data, connection):
        """Packet type: 34 """

        return True

    def on_set_player_start(self, data, connection):
        """Packet type: 35 """

        return True

    def on_find_unique_entity_response(self, data, connection):
        """Packet type: 36 """

        return True

    def on_modify_tile_list(self, data, connection):
        """Packet type: 37 """

        return True

    def on_damage_tile_group(self, data, connection):
        """Packet type: 38 """

        return True

    def on_collect_liquid(self, data, connection):
        """Packet type: 39 """

        return True

    def on_request_drop(self, data, connection):
        """Packet type: 40 """

        return True

    def on_spawn_entity(self, data, connection):
        """Packet type: 41 """

        return True

    def on_connect_wire(self, data, connection):
        """Packet type: 42 """

        return True

    def on_disconnect_all_wires(self, data, connection):
        """Packet type: 43 """

        return True

    def on_world_client_state_update(self, data, connection):
        """Packet type: 44 """

        return True

    def on_find_unique_entity(self, data, connection):
        """Packet type: 45 """

        return True

    def on_entity_create(self, data, connection):
        """Packet type: 46 """

        return True

    def on_entity_update(self, data, connection):
        """Packet type: 47 """

        return True

    def on_entity_destroy(self, data, connection):
        """Packet type: 48 """

        return True

    def on_entity_interact(self, data, connection):
        """Packet type: 49 """

        return True

    def on_entity_interact_result(self, data, connection):
        """Packet type: 50 """

        return True

    def on_hit_request(self, data, connection):
        """Packet type: 51 """

        return True

    def on_damage_request(self, data, connection):
        """Packet type: 52 """

        return True

    def on_damage_notification(self, data, connection):
        """Packet type: 53 """

        return True

    def on_entity_message(self, data, connection):
        """Packet type: 54 """

        return True

    def on_entity_message_response(self, data, connection):
        """Packet type: 55 """

        return True

    def on_update_world_properties(self, data, connection):
        """Packet type: 56 """

        return True

    def on_step_update(self, data, connection):
        """Packet type: 57 """

        return True

    def on_system_world_start(self, data, connection):
        """Packet type: 58 """

        return True

    def on_system_world_update(self, data, connection):
        """Packet type: 59 """

        return True

    def on_system_object_create(self, data, connection):
        """Packet type: 60 """

        return True

    def on_system_object_destroy(self, data, connection):
        """Packet type: 61 """

        return True

    def on_system_ship_create(self, data, connection):
        """Packet type: 62 """

        return True

    def on_system_ship_destroy(self, data, connection):
        """Packet type: 63 """

        return True

    def on_system_object_spawn(self, data, connection):
        """Packet type: 64 """

        return True

    def __repr__(self):
        return "<Plugin instance: %s (version %s)>" % (self.name, self.version)


class CommandNameError(Exception):
    """
    Raised when a command name can't be found from the `commands` list in a
    `SimpleCommandPlugin` instance.
    """


class SimpleCommandPlugin(BasePlugin):
    name = "simple_command_plugin"
    description = "Provides a simple parent class to define chat commands."
    version = "0.1"
    depends = ["command_dispatcher"]
    auto_activate = True

    def activate(self):
        super().activate()
        for name, attr in [(x, getattr(self, x)) for x in self.__dir__()]:
            if hasattr(attr, "_command"):
                for alias in attr._aliases:
                    self.plugins['command_dispatcher'].register(attr, alias)


class PluginManager:
    def __init__(self, config: ConfigurationManager, *, base=BasePlugin,
                 factory=None):
        self.base = base
        self.config = config
        self.failed = {}
        self._seen_classes = set()
        self._plugins = {}
        self._activated_plugins = set()
        self._deactivated_plugins = set()
        self._resolved = False
        self._overrides = set()
        self._override_cache = set()
        self._packet_parser = PacketParser(self.config)
        self._factory = factory
        self.logger = logging.getLogger("starrypy.plugin_manager")

    def list_plugins(self):
        return self._plugins

    @asyncio.coroutine
    def do(self, connection, action: str, packet: dict):
        """
        Calls an action on all loaded plugins.
        """

        try:
            if ("on_%s" % action) in self._overrides:
                packet = yield from self._packet_parser.parse(packet)
                send_flag = True
                for plugin in self._plugins.values():
                    p = getattr(plugin, "on_%s" % action)
                    if not (yield from p(packet, connection)):
                        send_flag = False
                return send_flag
            else:
                return True
        except Exception:
            self.logger.exception("Exception encountered in plugin on action: "
                                  "%s", action, exc_info=True)
            return True

    def load_from_path(self, plugin_path: pathlib.Path):
        blacklist = ["__init__", "__pycache__"]
        loaded = set()
        for file in plugin_path.iterdir():
            if file.stem in blacklist:
                continue
            if (file.suffix == ".py" or file.is_dir()) and str(
                    file) not in loaded:
                try:
                    loaded.add(str(file))
                    self.load_plugin(file)
                except (SyntaxError, ImportError) as e:
                    self.failed[file.stem] = str(e)
                    print(e)
                except FileNotFoundError:
                    self.logger.warning("File not found in plugin loader.")

    @staticmethod
    def _load_module(file_path: pathlib.Path):
        """
        Attempts to load a module, either from a straight python file or from
        a python package, by appending __init__.py to the end of the path if it
        is a directory.
        """

        if file_path.is_dir():
            file_path /= '__init__.py'
        if not file_path.exists():
            raise FileNotFoundError("{0} doesn't exist.".format(file_path))
        name = "plugins.%s" % file_path.stem
        loader = importlib.machinery.SourceFileLoader(name, str(file_path))
        module = loader.load_module(name)
        return module

    def load_plugin(self, plugin_path: pathlib.Path):
        module = self._load_module(plugin_path)
        classes = self.get_classes(module)
        for candidate in classes:
            candidate.factory = self._factory
            self._seen_classes.add(candidate)
        self.config.save_config()

    def get_classes(self, module: ModuleType):
        """
        Uses the inspect module to find all classes in a given module that
        are subclassed from `self.base`, but are not actually `self.base`.
        """

        class_list = []
        for _, obj in inspect.getmembers(module):
            if inspect.isclass(obj):
                if issubclass(obj, self.base) and obj is not self.base:
                    obj.config = self.config
                    obj.logger = logging.getLogger("starrypy.plugin.%s" %
                                                   obj.name)
                    class_list.append(obj)

        return class_list

    def load_plugins(self, plugins: list):
        for plugin in plugins:
            self.load_plugin(plugin)

    def close_parser(self):
        self._packet_parser.close()

    def resolve_dependencies(self):
        """
        Resolves dependencies from self._seen_classes through a very simple
        topological sort. Raises ImportError if there is an unresolvable
        dependency, otherwise it instantiates the class and puts it in
        self._plugins.
        """

        deps = {x.name: set(x.depends) for x in self._seen_classes}
        classes = {x.name: x for x in self._seen_classes}
        while len(deps) > 0:
            ready = [x for x, d in deps.items() if len(d) == 0]
            for name in ready:
                p = classes[name]()
                self._plugins[name] = p
                del deps[name]
            for name, depends in deps.items():
                to_load = depends & set(self._plugins.keys())
                deps[name] = deps[name].difference(set(self._plugins.keys()))
                for plugin in to_load:
                    classes[name].plugins[plugin] = self._plugins[plugin]
            if len(ready) == 0:
                raise ImportError("Unresolved dependencies found in: "
                                  "{}".format(deps))
        self._resolved = True

    # noinspection PyTypeChecker
    @asyncio.coroutine
    def get_overrides(self):
        if self._override_cache is self._activated_plugins:
            return self._overrides
        else:
            overrides = set()
            for plugin in self._activated_plugins:
                override = yield from detect_overrides(BasePlugin, plugin)
                overrides.update({x for x in override})
            self._overrides = overrides
            self._override_cache = self._activated_plugins
            return overrides

    def activate_all(self):
        self.logger.info("Activating plugins:")
        for plugin in self._plugins.values():
            self.logger.info(plugin.name)
            plugin.activate()
            self._activated_plugins.add(plugin)

    def deactivate_all(self):
        for plugin in self._plugins.values():
            self.logger.info("Deactivating %s", plugin.name)
            plugin.deactivate()
