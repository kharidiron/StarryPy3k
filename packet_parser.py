import asyncio
import logging
import traceback

from configuration_manager import ConfigurationManager
from data_parser import *
from utilities import BiDict

logger = logging.getLogger("starrypy.packet_parser")

packets = BiDict({
    'protocol_request': 0,
    'protocol_response': 1,
    'server_disconnect': 2,
    'connect_success': 3,
    'connect_failure': 4,
    'handshake_challenge': 5,
    'chat_received': 6,
    'universe_time_update': 7,
    'celestial_response': 8,
    'player_warp_result': 9,
    'planet_type_update': 10,
    'pause': 11,
    'client_connect': 12,
    'client_disconnect_request': 13,
    'handshake_response': 14,
    'player_warp': 15,
    'fly_ship': 16,
    'chat_sent': 17,
    'celestial_request': 18,
    'client_context_update': 19,
    'world_start': 20,
    'world_stop': 21,
    'world_layout_update': 22,
    'world_parameters_update': 23,
    'central_structure_update': 24,
    'tile_array_update': 25,
    'tile_update': 26,
    'tile_liquid_update': 27,
    'tile_damage_update': 28,
    'tile_modification_failure': 29,
    'give_item': 30,
    'environment_update': 31,
    'update_tile_protection': 32,
    'set_dungeon_gravity': 33,
    'set_dungeon_breathable': 34,
    'set_player_start': 35,
    'find_unique_entity_response': 36,
    'modify_tile_list': 37,
    'damage_tile_group': 38,
    'collect_liquid': 39,
    'request_drop': 40,
    'spawn_entity': 41,
    'connect_wire': 42,
    'disconnect_all_wires': 43,
    'world_client_state_update': 44,
    'find_unique_entity': 45,
    'entity_create': 46,
    'entity_update': 47,
    'entity_destroy': 48,
    'entity_interact': 49,
    'entity_interact_result': 50,
    'hit_request': 51,
    'damage_request': 52,
    'damage_notification': 53,
    'entity_message': 54,
    'entity_message_response': 55,
    'update_world_properties': 56,
    'step_update': 57,
    'system_world_start': 58,
    'system_world_update': 59,
    'system_object_create': 60,
    'system_object_destroy': 61,
    'system_ship_create': 62,
    'system_ship_destroy': 63,
    'system_object_spawn': 64})

parse_map = {
    0: ProtocolRequest,
    1: ProtocolResponse,
    2: ServerDisconnect,
    3: ConnectSuccess,
    4: ConnectFailure,
    5: HandshakeChallenge,
    6: ChatReceived,
    7: None,
    8: None,
    9: PlayerWarpResult,
    10: None,
    11: None,
    12: ClientConnect,
    13: ClientDisconnectRequest,
    14: None,
    15: PlayerWarp,
    16: FlyShip,
    17: ChatSent,
    18: None,
    19: ClientContextUpdate,
    20: WorldStart,
    21: WorldStop,
    22: None,
    23: None,
    24: None,
    25: None,
    26: None,
    27: None,
    28: None,
    29: None,
    30: GiveItem,
    31: None,
    32: None,
    33: None,
    34: None,
    35: None,
    36: None,
    37: ModifyTileList,
    38: None,
    39: None,
    40: None,
    41: SpawnEntity,
    42: None,
    43: None,
    44: None,
    45: None,
    46: EntityCreate,
    47: None,
    48: None,
    49: EntityInteract,
    50: EntityInteractResult,
    51: None,
    52: DamageRequest,
    53: DamageNotification,
    54: EntityMessage,
    55: EntityMessageResponse,
    56: DictVariant,
    57: StepUpdate,
    58: None,
    59: None,
    60: None,
    61: None,
    62: None,
    63: None,
    64: None
}


class PacketParser:
    """
    Object for handling the parsing and caching of packets.
    """

    def __init__(self, config: ConfigurationManager):
        self._cache = {}
        self.config = config
        self.loop = asyncio.get_event_loop()
        self._reaper = self.loop.create_task(self._reap())

    @asyncio.coroutine
    def parse(self, packet):
        """
        Given a packet preped packet from the stream, parse it down to its
        parts. First check if the packet is one we've seen before; if it is,
        pull its parsed form from the cache, and run with that. Otherwise,
        pass it to the appropriate parser for parsing.

        :param packet: Packet with header information parsed.
        :return: Fully parsed packet.
        """

        try:
            if packet["size"] >= self.config.config["min_cache_size"]:
                packet["hash"] = hash(packet["original_data"])
                if packet["hash"] in self._cache:
                    self._cache[packet["hash"]].count += 1
                    packet["parsed"] = self._cache[packet["hash"]].packet[
                        "parsed"]
                else:
                    packet = yield from self._parse_and_cache_packet(packet)
            else:
                packet = yield from self._parse_packet(packet)
        except Exception:
            print("Error during parsing.")
            print(traceback.print_exc())
        finally:
            return packet

    @asyncio.coroutine
    def _reap(self):
        """
        Prune packets from the cache that are not being used, and that are
        older than the "packet_reap_time".

        :return: None.
        """

        try:
            while not self.loop.shutting_down:
                yield from asyncio.sleep(self.config.config["packet_reap_time"])
                for h, cached_packet in self._cache.copy().items():
                    cached_packet.count -= 1
                    if cached_packet.count <= 0:
                        del (self._cache[h])
        except asyncio.CancelledError:
            logger.warning("Canceling reaper task.")

    @asyncio.coroutine
    def _parse_and_cache_packet(self, packet):
        """
        Take a new packet and pass it to the parser. Once we get it back,
        make a copy of it to the cache.

        :param packet: Packet with header information parsed.
        :return: Fully parsed packet.
        """

        packet = yield from self._parse_packet(packet)
        self._cache[packet["hash"]] = CachedPacket(packet=packet)
        return packet

    @asyncio.coroutine
    def _parse_packet(self, packet):
        """
        Parse the packet by giving it to the appropriate parser.

        :param packet: Packet with header information parsed.
        :return: Fully parsed packet.
        """

        res = parse_map[packet["type"]]
        if res is None:
            packet["parsed"] = {}
        else:
            packet["parsed"] = res.parse(packet["data"])
        return packet

    def close(self):
        self._reaper.cancel()


class CachedPacket:
    """
    Prototype for cached packets. Keep track of how often it is used,
    as well as the full packet's contents.
    """

    def __init__(self, packet):
        self.count = 1
        self.packet = packet


def build_packet(packet_id, data, compressed=False):
    """
    Convenience method for building a packet.

    :param packet_id: ID value of packet.
    :param data: Contents of packet.
    :param compressed: Whether or not to compress the packet.
    :return: Built packet object.
    """

    return BasePacket.build({"id": packet_id,
                             "data": data,
                             "compressed": compressed})
