"""
StarryPy Player Manager Plugin

Provides core player management features:
- implements roles
- implements bans
- manages player database

Original authors: AMorporkian
Updated for release: kharidiron
"""

import asyncio
from datetime import datetime
import pprint
import re
import json

import sqlalchemy as sqla
from sqlalchemy.orm import relationship

from data_parser import ConnectFailure, ServerDisconnect, WorldStop
from packet_parser import build_packet, packets
from plugin_manager import SimpleCommandPlugin
from plugins.storage_manager import (DeclarativeBase, SessionAccessMixin,
                                     db_session, cache_query, pack, unpack)
from utilities import (Command, State, broadcast, send_message,
                       WarpType, WarpWorldType, WarpAliasType)


class Player(DeclarativeBase):
    __tablename__ = "players"

    # fixed values
    uuid = sqla.Column(sqla.String(32), primary_key=True)
    first_seen = sqla.Column(sqla.DateTime)

    # semi-fixed values
    name = sqla.Column(sqla.String(255))
    alias = sqla.Column(sqla.String(20))
    species = sqla.Column(sqla.String(16))
    ranks = sqla.Column(sqla.String(255))
    permissions = sqla.Column(sqla.Text)
    granted_perms = sqla.Column(sqla.String(255))
    revoked_perms = sqla.Column(sqla.String(255))
    chat_prefix = sqla.Column(sqla.String(255))
    priority = sqla.Column(sqla.Integer)
    muted = sqla.Column(sqla.Boolean)
    banned = sqla.Column(sqla.Boolean)

    # mutable values
    logged_in = sqla.Column(sqla.Boolean)
    client_id = sqla.Column(sqla.Integer)
    last_seen = sqla.Column(sqla.DateTime)
    current_ip = sqla.Column(sqla.String(15))
    location = sqla.Column(sqla.String(255))
    last_location = sqla.Column(sqla.String(255))

    def __repr__(self):
        return "<Player(name={}, uuid={}, logged_in={})>".format(
            self.name, self.uuid, self.logged_in)

    def __str__(self):
        return pprint.pformat(self.__dict__)

    def update_ranks(self, player, ranks):
        """
        Update the player's info to match any changes made to their ranks.

        :return: Null.
        """

        permissions = set()
        highest_rank = None
        for r in unpack(self.ranks):
            if not highest_rank:
                highest_rank = r

            permissions |= ranks[r]['permissions']
            if ranks[r]['priority'] > ranks[highest_rank]['priority']:
                highest_rank = r
        permissions |= unpack(self.granted_perms)
        permissions -= unpack(self.revoked_perms)
        if highest_rank:
            player.priority = ranks[highest_rank]['priority']
            player.chat_prefix = ranks[highest_rank]['prefix']
        else:
            player.priority = 0
            player.chat_prefix = ""
        player.permissions = pack(permissions)

    def perm_check(self, perm):
        if not perm:
            return True
        elif "special.allperms" in unpack(self.permissions):
            return True
        elif perm.lower() in unpack(self.revoked_perms):
            return False
        elif perm.lower() in unpack(self.permissions):
            return True
        else:
            return False


class Ship(DeclarativeBase):
    __tablename__ = "ships"

    uuid = sqla.Column(sqla.String(32), primary_key=True)
    player = sqla.Column(sqla.String(20))

    def __init__(self, uuid, player):
        self.uuid = uuid
        self.player = player

    def __str__(self):
        return "{}'s ship".format(self.player)

    def __repr__(self):
        return "<Ship(player={}, uuid={})>".format(
            self.player, self.uuid)

    @staticmethod
    def location_string(uuid):
        s = list("ClientShipWorld:")
        s.append("{}".format(uuid))
        return "".join(s)

    @staticmethod
    def location_type():
        return "ClientShipWorld"


class Planet(DeclarativeBase):
    __tablename__ = "planets"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    x = sqla.Column(sqla.Integer)
    y = sqla.Column(sqla.Integer)
    z = sqla.Column(sqla.Integer)
    orbit = sqla.Column(sqla.Integer)
    satellite = sqla.Column(sqla.Integer)
    location = sqla.Column(sqla.String(64))
    name = sqla.Column(sqla.String(64))

    def __init__(self, x, y, z, orbit, satellite, name=None):
        self.x = x
        self.y = y
        self.z = z
        self.orbit = orbit
        self.satellite = satellite
        self.name = name
        self.location = self.location_string(self.x, self.y, self.z,
                                             self.orbit, self.satellite)

    def __str__(self):
        return self.location

    def __repr__(self):
        return ("<Planet(x={}, y={}, z={}, orbit={}, satellite={}, name={})>" 
                "".format(self.x, self.y, self.z, self.orbit, self.satellite,
                          self.name))

    @staticmethod
    def location_string(x, y, z, orbit, satellite, short=False):
        s = list("CelestialWorld:")
        s.append("{}:{}:{}:{}".format(x, y, z, orbit))
        if satellite > int(0) or not short:
            s.append(":{}".format(satellite))
        return "".join(s)

    @staticmethod
    def location_type():
        return "CelestialWorld"


class IPBan(DeclarativeBase):
    __tablename__ = "ip_bans"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    ip = sqla.Column(sqla.String(20))
    reason = sqla.Column(sqla.String(255))
    banned_by = sqla.Column(sqla.String(20))
    banned_at = sqla.Column(sqla.DateTime)
    duration = sqla.Column(sqla.String(10))

    def __repr__(self):
        return "<IPBan(ip={}, reason={}, by={}, when={} duration={}".format(
            self.ip, self.reason, self.banned_by, self.banned_at, self.duration)


class UUIDBan(DeclarativeBase):
    __tablename__ = "uuid_bans"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    uuid = sqla.Column(sqla.String(32))
    reason = sqla.Column(sqla.String(255))
    banned_by = sqla.Column(sqla.String(20))
    banned_at = sqla.Column(sqla.DateTime)
    duration = sqla.Column(sqla.String(10))

    def __repr__(self):
        return "<UUIDBan(uuid={}, reason={}, by={}, when={} duration={}".format(
            self.uuid, self.reason, self.banned_by, self.banned_at,
            self.duration)


class IP(DeclarativeBase):
    __tablename__ = "ips"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    ip = sqla.Column(sqla.String(15))
    uuid = sqla.Column(sqla.String(32), sqla.ForeignKey("players.uuid"))
    last_seen = sqla.Column(sqla.DateTime)

    player = relationship("Player", back_populates="ips")

    def __repr__(self):
        return "<IP(ip={}, uuid={})>".format(self.ip, self.uuid)


Player.ips = relationship("IP", order_by=IP.id, back_populates="player")


###

class PlayerManager(SessionAccessMixin, SimpleCommandPlugin):
    name = "player_manager"

    def __init__(self):
        self.default_config = {"owner_uuid": "!--REPLACE IN CONFIG FILE--!",
                               "allowed_species": ["apex", "avian", "glitch",
                                                   "floran", "human", "hylotl",
                                                   "penguin", "novakid"],
                               "owner_ranks": ["Owner"],
                               "new_user_ranks": ["Guest"]}
        super().__init__()
        try:
            with open("config/permissions.json", "r") as file:
                self.rank_config = json.load(file)
        except IOError as e:
            self.logger.error("Fatal: Could not read permissions file!")
            self.logger.error(e)
            raise SystemExit
        except json.JSONDecodeError as e:
            self.logger.error("Fatal: Could not parse permissions.json!")
            self.logger.error(e)
            raise SystemExit
        self._clean_slate()
        self.server_ranks = self._rebuild_ranks(self.rank_config)

    # Packet hooks - look for these packets and act on them

    def on_protocol_request(self, data, connection):
        """
        Catch when a client first pings the server for a connection. Set the
        'state' variable to keep track of this.

        :param data: The packet containing the action.
        :param connection: The connection from which the packet came.
        :return: Boolean: True. Must be true, so that packet get passed on.
        """

        connection.state = State.VERSION_SENT
        return True

    def on_handshake_challenge(self, data, connection):
        """
        Catch when a client tries to handshake with server. Update the 'state'
        variable to keep track of this. Note: This step only occurs when a
        server requires name/password authentication.

        :param data:
        :param connection:
        :return: Boolean: True. Must be true, so that packet get passed on.
        """

        connection.state = State.HANDSHAKE_CHALLENGE_SENT
        return True

    def on_handshake_response(self, data, connection):
        """
        Catch when the server responds to a client's handshake. Update the
        'state' variable to keep track of this. Note: This step only occurs
        when a server requires name/password authentication.

        :param data:
        :param connection:
        :return: Boolean: True. Must be true, so that packet get passed on.
        """

        connection.state = State.HANDSHAKE_RESPONSE_RECEIVED
        return True

    def on_client_connect(self, data, connection):
        """
        Catch when a the client updates the server with its connection
        details. This is a key step to fingerprinting the client, and
        ensuring they stay in the wrapper. This is also where we apply our
        bans.

        :param data:
        :param connection:
        :return: Boolean: True on successful connection, False on a
                 failed connection.
        """

        try:
            player = yield from self._add_or_get_player(**data["parsed"],
                                                        ip=connection.client_ip)
            self.check_bans(connection)
            self.check_species(player.species)
        except (NameError, ValueError) as e:
            yield from connection.raw_write(self.build_rejection(str(e)))
            connection.die()
            return False
        connection.player = player
        return True

    def on_connect_success(self, data, connection):
        """
        Catch when a successful connection is established. Update the 'state'
        variable to keep track of this. Since the client successfully
        connected, update their details in storage (client id, location,
        logged_in state).

        :param data:
        :param connection:
        :return: Boolean: True. Must be true, so that packet get passed on.
        """

        response = data["parsed"]
        player = connection.player
        with db_session(self.session) as session:
            player.logged_in = True
            player.last_seen = datetime.now()
            player.client_id = response["client_id"]
            session.commit()
        connection.state = State.CONNECTED
        connection.client_id = response["client_id"]
        self.logger.info("Player {} [client id: {}] has successfully connected."
                         "".format(player.alias, player.client_id))
        return True

    def on_client_disconnect_request(self, data, connection):
        """
        Catch when a client requests a disconnect from the server. At this
        point, we need to clean up the connection information we have for the
        client (logged_in state, location).

        :param data:
        :param connection:
        :return: Boolean: True. Must be true, so that packet get passed on.
        """

        player = connection.player
        self.logger.info("Player {} is disconnecting.".format(player.name))
        return True

    def on_server_disconnect(self, data, connection):
        """
        Catch when the server disconnects a client. Similar to the client
        disconnect packet, use this as a cue to perform cleanup, if it wasn't
        done already.

        :param data:
        :param connection:
        :return: Boolean: True. Must be true, so that packet get passed on.
        """

        yield from self._set_offline(connection)
        return True

    def on_world_start(self, data, connection):
        """
        Hook when a new world instance is started. Use the details passed to
        determine the location of the world, and update the player's
        information accordingly.

        :param data:
        :param connection:
        :return: Boolean: True. Don't stop the packet here.
        """

        player = connection.player
        planet_data = data["parsed"]["template_data"]
        if planet_data["celestialParameters"] is not None:
            location = yield from self._add_or_get_planet(
                *planet_data[
                    "celestialParameters"]["coordinate"]["location"],
                orbit=planet_data[
                    "celestialParameters"]["coordinate"]["planet"],
                satellite=planet_data[
                    "celestialParameters"]["coordinate"]["satellite"],
                name=planet_data[
                    "celestialParameters"]["name"])

            with db_session(self.session) as session:
                player.location = str(location)
                session.commit()

        self.logger.info("Player {} is now at location: {}".format(
            player.alias, player.location))

        return True

    def on_player_warp(self, data, connection):
        player = connection.player
        self.logger.info("Player {} has initiated a warp.".format(player.alias))
        return True

    def on_player_warp_result(self, data, connection):
        """
        Hook when a player warps to a world. This action is also used when
        a player first logs in. Use the details passed to determine the
        location of the world, and update the player's information accordingly.

        :param data:
        :param connection:
        :return: Boolean: True. Don't stop the packet here.
        """

        def _update_location(player, location):
            with db_session(self.session) as session:
                if player.location:
                    player.last_location = player.location
                player.location = str(location)
                session.commit()

        player = connection.player
        location = "Unknown"
        if data["parsed"]["warp_success"]:
            warp_data = data["parsed"]["warp_action"]
            if warp_data["warp_type"] == WarpType.TO_WORLD:
                if warp_data["world_id"] == WarpWorldType.CELESTIAL_WORLD:
                    location = yield from self._add_or_get_planet(
                        **warp_data["celestial_coordinates"])
                elif warp_data["world_id"] == WarpWorldType.PLAYER_WORLD:
                    location = yield from self._add_or_get_ship(
                        warp_data["ship_id"].decode("utf-8"))
                elif warp_data["world_id"] == WarpWorldType.INSTANCE_WORLD:
                    location = yield from self._add_or_get_instance(
                        warp_data)
            elif warp_data["warp_type"] == WarpType.TO_PLAYER:
                target = self.get_player_by_uuid(
                    warp_data["player_id"].decode("utf-8"))
                location = target.location
            elif warp_data["warp_type"] == WarpType.TO_ALIAS:
                if warp_data["alias_id"] == WarpAliasType.ORBITED:
                    # Pass on this here, and store the value in on_world_start
                    pass
                elif warp_data["alias_id"] == WarpAliasType.SHIP:
                    location = yield from self._add_or_get_ship(
                        player.uuid)
                elif warp_data["alias_id"] == WarpAliasType.RETURN:
                    location = player.last_location
            _update_location(player, location)
        return True

    def on_step_update(self, data, connection):
        """
        Catch when the first heartbeat packet is sent to a player. This is the
        final confirmation in the connection process. Update the 'state'
        variable to reflect this.

        :param data:
        :param connection:
        :return: Boolean: True. Must be true, so that packet get passed on.
        """

        if connection.state != State.CONNECTED_WITH_HEARTBEAT:
            connection.state = State.CONNECTED_WITH_HEARTBEAT
        return True

    # Helper functions - Used by hooks and commands

    def _clean_slate(self):
        """
        Start everything off with a clean slate. Log out players who are still
        marked as 'logged in' when the server starts.

        :return: Boolean. True.
        """

        with db_session(self.session) as session:
            players = session.query(Player).filter_by(logged_in=True).all()
            for player in players:
                self.logger.debug(
                    "Setting {} logged_in to false.".format(player.alias))
                player.logged_in = False
                player.client_id = None
                player.location = ""

            players = session.query(Player).filter(
                Player.client_id.isnot(None)).all()
            for player in players:
                player.client_id = None
                player.location = ""

            session.commit()
        return True

    @asyncio.coroutine
    def _set_offline(self, connection):
        """
        Convenience function to set all the players variables to off.

        :param connection: The connection to turn off.
        :return: Boolean, True. Always True, since called from the on_ packets.
        """

        player = connection.player
        with db_session(self.session) as session:
            player.logged_in = False
            player.location = ""
            player.last_seen = datetime.now()
            player.client_id = None
            session.commit()
        return True

    def clean_name(self, name):
        color_strip = re.compile("\^(.*?);")
        alias = color_strip.sub("", name)
        non_ascii_strip = re.compile("[^ -~]")
        alias = non_ascii_strip.sub("", alias)
        multi_whitespace_strip = re.compile("[\s]{2,}")
        alias = multi_whitespace_strip.sub(" ", alias)
        trailing_leading_whitespace_strip = re.compile("^[ \s]+|[ \s]+$")
        alias = trailing_leading_whitespace_strip.sub("", alias)
        match_non_whitespace = re.compile("[\S]")
        if match_non_whitespace.search(alias) is None:
            return None
        else:
            if len(alias) > 20:
                alias = alias[0:20]
            return alias

    def build_rejection(self, reason):
        """
        Function to build packet to reject connection for client.

        :param reason: String. Reason for rejection.
        :return: Rejection packet.
        """

        return build_packet(packets["connect_failure"],
                            ConnectFailure.build(
                                dict(reason=reason)))

    def _rebuild_ranks(self, ranks):
        """
        Rebuilds rank configuration from file, including inherited permissions.

        :param ranks: The initial rank config.
        :return: Dict: The built rank permissions.
        """

        final = {}

        def build_inherits(inherits):
            finalperms = set()
            for inherit in inherits:
                if 'inherits' in ranks[inherit]:
                    finalperms |= build_inherits(ranks[inherit]['inherits'])
                finalperms |= set(ranks[inherit]['permissions'])
            return finalperms

        for rank, config in ranks.items():
            config['permissions'] = set(config['permissions'])
            if 'inherits' in config:
                config['permissions'] |= build_inherits(config['inherits'])
            final[rank] = config

        return final

    def ban_by_ip(self, ip, reason, connection):
        """
        Ban a player based on their IP address. Should be compatible with both
        IPv4 and IPv6.

        :param ip: String: IP of player to be banned.
        :param reason: String: Reason for player's ban.
        :param connection: Connection of target player to be banned.
        :return: Null
        """

        with db_session(self.session) as session:
            ban = IPBan(ip=ip, reason=reason, banned_at=datetime.now(),
                        banned_by=connection.player.alias)
            session.add(ban)
            session.commit()
        send_message(connection,
                     "Banned IP: {} with reason: {}".format(ip, reason))

    def unban_by_ip(self, ip, connection):
        """
        Unban a player based on their IP address. Should be compatible with both
        IPv4 and IPv6.

        :param ip: String: IP of player to be unbanned.
        :param connection: Connection of target player to be unbanned.
        :return: Null
        """

        with db_session(self.session) as session:
            ban = session.query(IPBan).filter_by(ip=ip).first()
            session.delete(ban)
            session.commit()
        send_message(connection,
                     "Ban removed: {}".format(ip))

    def ban_by_name(self, name, reason, connection):
        """
        Ban a player based on their name. This is the easier route, as it is a
        more user friendly to target the player to be banned. Hooks to the
        ban_by_ip mechanism backstage.

        :param name: String: Name of the player to be banned.
        :param reason: String: Reason for player's ban.
        :param connection: Connection of target player to be banned.
        :return: Null
        """

        p = self.find_player(name)
        if p is not None:
            self.ban_by_ip(p.ip, reason, connection)
        else:
            send_message(connection,
                         "Couldn't find a player by the name {}".format(name))

    def unban_by_name(self, name,  connection):
        """
        Ban a player based on their name. This is the easier route, as it is a
        more user friendly to target the player to be banned. Hooks to the
        ban_by_ip mechanism backstage.

        :param name: String: Name of the player to be banned.
        :param connection: Connection of target player to be banned.
        :return: Null
        """

        p = self.find_player(name)
        if p is not None:
            self.unban_by_ip(p.ip, connection)
        else:
            send_message(connection,
                         "Couldn't find a player by the name {}".format(name))

    def check_bans(self, connection):
        """
        Check if a ban on a player exists. Raise ValueError when true.

        :param connection: The connection of the target player.
        :return: Null.
        :raise: ValueError if player is banned. Pass reason message up with
                exception.
        """

        with db_session(self.session) as session:
            ban = session.query(IPBan).filter_by(
                ip=connection.client_ip).first()
            if ban:
                self.logger.info("Banned IP ({}) tried to log in.".format(
                    connection.client_ip))
                raise ValueError("You are banned!\nReason: {}".format(
                    ban.reason))

    def check_species(self, species):
        """
        Check if a player has an unknown species. Raise ValueError when true.
        Context: http://community.playstarbound.com/threads/119569/

        :param species: The species of the player being checked.
        :return: Null.
        :raise: ValueError if the player has an unknown species.
        """

        if species not in self.plugin_config.allowed_species:
            self.logger.info("Player with unknown species ({}) tried to log in."
                             "".format(species))
            raise ValueError("Connection terminated!\nThe species ({}) is not "
                             "allowed on this server.".format(species))

    def get_player_by_uuid(self, uuid) -> Player:
        """
        Grab a hook to a player by their uuid. Returns player object.

        :param uuid: String: UUID of player to check.
        :return: Mixed: Player object.
        """

        with db_session(self.session) as session:
            player = session.query(Player).filter(
                Player.uuid.ilike(uuid)).first()
        return cache_query(player)

    def get_player_by_name(self, name, check_logged_in=False) -> Player:
        """
        Grab a hook to a player by their name. Return Boolean value if only
        checking login status. Returns player object otherwise.

        :param name: String: Name of player to check.
        :param check_logged_in: Boolean: Whether we just want login status
                                (true), or the player's server object (false).
        :return: Mixed: Boolean on logged_in check, player object otherwise.
        """

        lname = name.lower()
        with db_session(self.session) as session:
            player = session.query(Player).filter(
                Player.name.ilike(lname)).first()
            if not check_logged_in or player.logged_in:
                return cache_query(player)

    def get_player_by_alias(self, alias, check_logged_in=False) -> Player:
        """
        Grab a hook to a player by their name. Return Boolean value if only
        checking login status. Returns player object otherwise.

        :param alias: String: Cleaned name of player to check.
        :param check_logged_in: Boolean: Whether we just want login status
                                (true), or the player's server object (false).
        :return: Mixed: Boolean on logged_in check, player object otherwise.
        """

        lname = alias.lower()
        with db_session(self.session) as session:
            player = session.query(Player).filter(
                Player.alias.ilike(lname)).first()
            if not check_logged_in or player.logged_in:
                return cache_query(player)

    def get_player_by_client_id(self, client_id) -> Player:
        """
        Grab a hook to a player by their client id. Returns player object.

        :param client_id: Integer: Client Id of the player to check.
        :return: Player object.
        """

        with db_session(self.session) as session:
            player = session.query(Player).filter_by(
                client_id=client_id).first()
            if player.client_id:
                return cache_query(player)

    def get_player_by_ip(self, ip, check_logged_in=False) -> Player:
        """
        Grab a hook to a player by their IP. Returns boolean if only
        checking login status. Returns Player object otherwise.

        :param ip: IP of player to check.
        :param check_logged_in: Boolean: Whether we just want login status
                                (true), or the player's server object (false)
        :return: Mixed: Boolean on logged_in check, player object otherwise.
        """

        with db_session(self.session) as session:
            player = session.query(Player).filter_by(current_ip=ip).all()
            if player:
                if len(player) > 1:
                    raise ValueError("Multiple clients logged in from same IP.")

                if not check_logged_in or player.logged_in:
                    return player

    def get_connection(self, current_connection, player):
        for c in current_connection.factory.connections:
            if player.uuid == c.player.uuid:
                return c
        else:
            return False

    def find_player(self, search, check_logged_in=False) -> Player:
        """
        Convenience method to try and find a player by a variety of methods.
        Checks for alias, then raw name, then client id.

        :param search: The alias, raw name, or id of the player to check.
        :param check_logged_in: Boolean: Return the login status only if true.
        :return: Mixed: Boolean on logged_in check, player object otherwise.
        """

        player = self.get_player_by_alias(search, check_logged_in)
        if player is not None:
            return player

        player = self.get_player_by_name(search, check_logged_in)
        if player is not None:
            return player

        try:
            search = int(search)
            player = self.get_player_by_client_id(search)
            if player is not None:
                return player
        except ValueError:
            pass

        if len(search) == 32:
            player = self.get_player_by_uuid(search)
            if player is not None:
                return player

        player = self.get_player_by_ip(search, check_logged_in)
        if player is not None:
            return player

    @asyncio.coroutine
    def _add_or_get_player(self, uuid, species, name="",
                           ip="", **kwargs) -> Player:
        """
        Given a UUID, try to find the player's info in storage. In the event
        that the player has never connected to the server before, add their
        details into storage for future reference. Return a Player object.

        :param uuid: UUID of connecting character
        :param species: Species of connecting character
        :param name: Name of connecting character
        :param roles: Roles granted to character
        :param ip: IP address of connection
        :param kwargs: any other keyword arguments
        :return: Player object.
        """

        if isinstance(uuid, bytes):
            uuid = uuid.decode("ascii")
        if isinstance(name, bytes):
            name = name.decode("utf-8")
        alias = self.clean_name(name)
        if alias is None:
            alias = uuid[0:4]

        with db_session(self.session) as session:
            ip_addr = session.query(IP).filter_by(ip=ip, uuid=uuid).first()
            if not ip_addr:
                ip_addr = IP(ip=ip, uuid=uuid, last_seen=datetime.now())
                session.add(ip_addr)
            else:
                ip_addr.last_seen = datetime.now()
            session.commit()

        with db_session(self.session) as session:
            player = session.query(Player).filter_by(uuid=uuid).first()
            if player:
                self.logger.info("Known player is attempting to log in: "
                                 "{} (UUID: {})".format(alias, uuid))
                if player.logged_in:
                    raise ValueError("Player is already logged in.")
                if player.name != name:
                    player.name = name
                player.alias = alias
                player.current_ip = ip
                player.update_ranks(player, self.server_ranks)
            else:
                self.logger.info("A new player is connecting")
                if self.get_player_by_alias(alias) is not None:
                    raise NameError("A user with that name already exists.")
                self.logger.info("Adding new player to database: {} (UUID: {})"
                                 "".format(alias, uuid))
                if uuid == self.plugin_config.owner_uuid:
                    ranks = set(self.plugin_config.owner_ranks)
                else:
                    ranks = set(self.plugin_config.new_user_ranks)
                player = Player(uuid=uuid,
                                name=name,
                                alias=alias,
                                species=species,
                                current_ip=ip,
                                first_seen=datetime.now(),
                                ranks=pack(ranks),
                                logged_in=False)
                session.add(player)
                player.update_ranks(player, self.server_ranks)
            session.commit()
            return cache_query(player)

    @asyncio.coroutine
    def _add_or_get_ship(self, ship_id) -> Ship:
        """
        Given a ship world's uuid, look up their ship in the ships shelf. If
        ship not in shelf, add it. Return a Ship object.

        :param ship_id: Target player to look up
        :return: Ship object.
        """

        p = self.get_player_by_uuid(ship_id)
        if p:
            with db_session(self.session) as session:
                ship = session.query(Ship).filter_by(uuid=p.uuid).first()
                if not ship:
                    ship = Ship(p.uuid, p.alias)
                    session.add(ship)
                session.commit()

                return cache_query(ship)

    @asyncio.coroutine
    def _add_or_get_planet(self, x=0, y=0, z=0, orbit=0, satellite=0,
                           name="") -> Planet:
        """
        Look up a planet in the planets shelf, return a Planet object. If not
        present, add it to the shelf. Return a Planet object.

        :return: Planet object.
        """

        location = Planet.location_string(x, y, z, orbit, satellite)

        with db_session(self.session) as session:
            planet = session.query(Planet).filter_by(location=location).first()
            if not planet:
                self.logger.info("Logging new planet in database.")
                planet = Planet(x=x, y=y, z=z, orbit=orbit, satellite=satellite,
                                name=name)
                session.add(planet)
            session.commit()

            return cache_query(planet)

    @asyncio.coroutine
    def _add_or_get_instance(self, data):
        """
        Generate instance string. Since these volatile, we don't actually
        store them, contrary to the 'add' used in naming the method.

        :param data:
        :return: String.
        """

        instance = list("InstanceWorld:")
        instance.append("{}".format(data["world_name"]))
        if data["is_instance"]:
            instance.append(":{}".format(data["instance_id"].decode("utf-8")))
        else:
            instance.append(":-")

        return "".join(instance)

    def players_online(self, connection=None):
        """
        Check which players are logged in.

        If a connection is provided, determine online players from the
        connection queue stored in the factory. Otherwise, check the
        database fro all 'online' players (default).

        :param connection: Hook for connection factory (optional).
        :return list: List of players online.
        """

        if connection:
            players = list()
            for c in connection.factory.connections:
                players.append(c.player)
            return players
        else:
            with db_session(self.session) as session:
                players = session.query(Player).filter_by(logged_in=True).all()
                return cache_query(players, collection=True)

    def players_here(self, location):
        """
        Check which players are at a location.

        :param location:
        :return:
        """

        with db_session(self.session) as session:
            players = session.query(Player).filter_by(logged_in=True,
                                                      location=location).all()
            return cache_query(players, collection=True)

    # Commands - In-game actions that can be performed

    @Command("kick",
             perm="player_manager.kick",
             doc="Kicks a player.",
             syntax=("[\"]player name[\"]", "[reason]",
                     "[*dirty] ^red;dirty disconnect crashes target.^reset;"))
    def _kick(self, data, connection):
        """
        Kick a play off the server. You must specify a name. You may also
        specify an optional reason.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        if data[-1] == "*dirty":
            dirty = True
            data.pop()
        else:
            dirty = False

        try:
            alias = data[0]
        except IndexError:
            raise SyntaxWarning("No target provided.")

        try:
            reason = " ".join(data[1:])
        except IndexError:
            reason = "No reason given."

        p = self.find_player(alias)
        if p is None:
            send_message(connection,
                         "Couldn't find a player with name {}".format(alias))
            return
        if p.priority >= connection.player.priority:
            send_message(connection, "Can't kick {}, they are equal or "
                                     "higher than your rank!".format(p.alias))
            return
        if not p.logged_in:
            send_message(connection,
                         "Player {} is not currently logged in.".format(alias))
            return

        target = self.get_connection(connection, p)
        if not target:
            # player not in connection pool
            with db_session(self.session) as session:
                player = session.query(Player).filter_by(uuid=p.uuid).first()
                player.logged_in = False
                player.location = ""
                player.client_id = None
                session.commit()
                send_message(connection, "Kicking offline player {}".format(
                    p.alias))
                self.logger.warning("Kicking offline player.")
                return

        if not dirty:
            worldstop_packet = build_packet(packets["world_stop"],
                                            WorldStop.build(
                                                dict(reason="Removed")))
            yield from target.raw_write(worldstop_packet)
        kick_string = "You were kicked.\n Reason: ^red;{}^reset;".format(reason)
        kick_packet = build_packet(packets["server_disconnect"],
                                   ServerDisconnect.build(
                                       dict(reason=kick_string)))
        yield from target.raw_write(kick_packet)
        broadcast(self, "^red;{} has been kicked for reason: "
                        "{}^reset;".format(alias, reason))
        yield from self._set_offline(target)

    @Command("ban",
             perm="player_manager.ban",
             doc="Bans a user or an IP address.",
             syntax=("(ip | name)", "(reason)"))
    def _ban(self, data, connection):
        """
        Ban a player. You must specify either a name or an IP. You must also
        specify a 'reason' for banning the player. This information is stored
        and, should the player try to connect again, are great with the
        message:

        > You are banned!
        > Reason: <reason shows here>

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        :raise: SyntaxWarning on incorrect input.
        """

        try:
            target, reason = data[0], " ".join(data[1:])
            if self.find_player(target).priority >= connection.player.priority:
                send_message(connection, "Can't ban {}, they are equal or "
                                         "higher than your rank!"
                             .format(target))
                return
            if re.match(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$", target):
                self.ban_by_ip(target, reason, connection)
            else:
                self.ban_by_name(target, reason, connection)
        except:
            raise SyntaxWarning

    @Command("unban",
             perm="player_manager.ban",
             doc="Unbans a user or an IP address.",
             syntax="(ip | name)")
    def _unban(self, data, connection):
        """
        Unban a player. You must specify either a name or an IP.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        :raise: SyntaxWarning on incorrect input.
        """

        try:
            target = data[0]
            if re.match(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$", target):
                self.unban_by_ip(target, connection)
            else:
                self.unban_by_name(target, connection)
        except:
            raise SyntaxWarning

    @Command("list_bans",
             perm="player_manager.ban",
             doc="Lists all active bans.")
    def _list_bans(self, data, connection):
        """
        List the current bans.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        with db_session(self.session) as session:
            bans = session.query(IPBan).all()
            if not bans:
                send_message(connection, "There are no active bans.")
                return

            res = ["Active bans:"]
            for ban in bans:
                res.append("IP: {ip} - "
                           "Reason: {reason} - "
                           "Banned by: {banned_by} - "
                           "Banned at: {banned_at}".format(**ban.__dict__))
            send_message(connection, "\n".join(res))

    @Command("user",
             perm="player_manager.user",
             doc="Manages user permissions; see /user help for details.")
    def _user(self, data, connection):
        @asyncio.coroutine
        def _send_lack_permission(conn):
            yield from send_message(conn,
                                    "You don't have permission to do that!")

        @asyncio.coroutine
        def _send_user_not_found(conn, user):
            yield from send_message(conn, "User {} not found.".format(user))

        @asyncio.coroutine
        def _send_does_not_exit(conn, metathing, thing):
            yield from send_message(conn, "{} {} does not exist."
                                    .format(metathing, thing))

        @asyncio.coroutine
        def _send_not_specified(conn, metathing):
            yield from send_message(conn, "No {} specified.".format(metathing))

        @asyncio.coroutine
        def _send_list(conn, metathing, user, things):
            yield from send_message(conn, "{} for user {}:\n{}"
                                    .format(metathing, user, things))

        @asyncio.coroutine
        def _send_already_has(conn, metathing, user, thing):
            yield from send_message(conn, "Player {} already has {} {}."
                                    .format(user, metathing, thing))

        @asyncio.coroutine
        def _send_does_not_have(conn, metathing, user, thing):
            yield from send_message(conn, "Player {} does not have {} {}."
                                    .format(user, metathing, thing))

        @asyncio.coroutine
        def _send_granted(conn, metathing, thing, user):
            yield from send_message(conn, "You were granted {} {} by {}."
                                    .format(metathing, thing, user))

        @asyncio.coroutine
        def _send_revoked(conn, metathing, thing, user):
            yield from send_message(conn, "{} removed {} {} from you."
                                    .format(user, metathing, thing))

        @asyncio.coroutine
        def _send_log_action(conn, action, metathing, thing, user):
            direction = "from"
            if action is "Granted":
                direction = "to"
            blurb = "{} {} {} {} {}.".format(action, metathing, thing,
                                             direction, user)
            self.logger.debug(blurb)
            yield from send_message(conn, blurb)

        if not data:
            yield from send_message(connection, "No arguments provided. See "
                                                "/user help for usage info.")
        elif data[0].lower() == "help":
            send_message(connection, "Syntax:")
            send_message(connection, "/user addperm (user) (permission)")
            send_message(connection, "Adds a permission to a player. Fails if "
                                     "the user doesn't have the permission.")
            send_message(connection, "/user rmperm (player) (permission)")
            send_message(connection, "Removes a permission from a player. "
                                     "Fails if the user doesn't have the"
                                     " permission, or if the target's priority"
                                     " is higher than the user's.")
            send_message(connection, "/user addrank (player) (rank)")
            send_message(connection, "Adds a rank to a player. Fails if the "
                                     "rank to be added is equal to or "
                                     "greater than the user's highest rank.")
            send_message(connection, "/user rmrank (player) (rank)")
            send_message(connection, "Removes a rank from a player. Fails if "
                                     "the target outranks or is equal in rank"
                                     " to the user.")
            send_message(connection, "/user listperms (player)")
            send_message(connection, "Lists the permissions a player has.")
            send_message(connection, "/user listranks (player)")
            send_message(connection, "Lists the ranks a player has.")
        elif data[0].lower() == "addperm":
            p = self.find_player(data[1])
            if p:
                if not data[2]:
                    yield from _send_not_specified(connection, "permission")
                elif not connection.player.perm_check(data[2]):
                    yield from _send_lack_permission(connection)
                elif data[2].lower() in unpack(p.permissions):
                    yield from _send_already_has(connection, "permission",
                                                 p.alias, data[2])
                else:
                    with db_session(self.session) as session:
                        tmp = unpack(p.granted_perms)
                        tmp.add(data[2].lower())
                        p.granted_perms = pack(tmp)

                        tmp = unpack(p.revoked_perms)
                        tmp.discard(data[2].lower())
                        p.revoked_perms = pack(tmp)

                        p.update_ranks(p, self.server_ranks)
                        session.commit()
                    if p.logged_in:
                        target = self.get_connection(connection, p)
                        yield from _send_granted(target, "permission", 
                                                 data[2].lower(),
                                                 connection.player.alias)
                    yield from _send_log_action(connection, "Granted", 
                                                "permission", data[2], p.alias)
            else:
                yield from _send_user_not_found(connection, data[1])
        elif data[0].lower() == "rmperm":
            p = self.find_player(data[1])
            if p:
                if not data[2]:
                    yield from _send_not_specified(connection, "permission")
                elif not connection.player.perm_check(data[2]):
                    yield from _send_lack_permission(connection)
                elif p.priority >= connection.player.priority:
                    yield from _send_lack_permission(connection)
                elif data[2].lower() not in unpack(p.permissions):
                    yield from _send_does_not_have(connection, "permission", 
                                                   p.alias, data[2])
                else:
                    with db_session(self.session) as session:
                        tmp = unpack(p.granted_perms)
                        tmp.discard(data[2].lower())
                        p.granted_perms = pack(tmp)

                        tmp = unpack(p.revoked_perms)
                        tmp.add(data[2].lower())
                        p.revoked_perms = pack(tmp)

                        p.update_ranks(p, self.server_ranks)
                        session.commit()
                    if p.logged_in:
                        target = self.get_connection(connection, p)
                        yield from _send_revoked(target, "permission", 
                                                 data[2].lower(),
                                                 connection.player.alias)
                    yield from _send_log_action(connection, "Removed", 
                                                "permission", data[2], p.alias)
            else:
                yield from _send_user_not_found(connection, data[1])
        elif data[0].lower() == "addrank":
            p = self.find_player(data[1])
            if p:
                if not data[2]:
                    yield from _send_not_specified(connection, "rank")
                    return
                if data[2] not in self.server_ranks:
                    yield from _send_does_not_exit(connection, "Rank", data[2])
                    return
                rank = self.server_ranks[data[2]]
                if rank["priority"] >= connection.player.priority:
                    yield from _send_lack_permission(connection)
                elif data[2] in unpack(p.ranks):
                    yield from _send_already_has(connection, "rank", p.alias, 
                                                 data[2])
                else:
                    with db_session(self.session) as session:
                        tmp = unpack(p.ranks)
                        tmp.add(data[2])
                        p.ranks = pack(tmp)
                        p.update_ranks(p, self.server_ranks)
                        session.commit()
                    if p.logged_in:
                        target = self.get_connection(connection, p)
                        yield from _send_granted(target, "rank", data[2],
                                                 connection.player.alias)
                    yield from _send_log_action(connection, "Granted", "rank",
                                                data[2], p.alias)
            else:
                yield from _send_user_not_found(connection, data[1])
        elif data[0].lower() == "rmrank":
            p = self.find_player(data[1])
            if p:
                if not data[2]:
                    yield from _send_not_specified(connection, "rank")
                    return
                if data[2] not in self.server_ranks:
                    yield from _send_does_not_exit(connection, "Rank", data[2])
                    return
                if p.priority >= connection.player.priority:
                    yield from _send_lack_permission(connection)
                elif data[2] not in unpack(p.ranks):
                    yield from _send_does_not_have(connection, "rank", p.alias, 
                                                   data[2])
                else:
                    with db_session(self.session) as session:
                        tmp = unpack(p.ranks)
                        tmp.remove(data[2])
                        p.ranks = pack(tmp)
                        p.update_ranks(p, self.server_ranks)
                        session.commit()
                    if p.logged_in:
                        target = self.get_connection(connection, p)
                        yield from _send_revoked(target, "rank", data[2],
                                                 connection.player.alias)
                    yield from _send_log_action(connection, "Removed", "rank",
                                                data[2], p.alias)
            else:
                yield from _send_user_not_found(connection, data[1])
        elif data[0].lower() == "listperms":
            p = self.find_player(data[1])
            if p:
                perms = ", ".join(unpack(p.permissions))
                yield from _send_list(connection, "Permissions", p.alias, perms)
            else:
                yield from _send_user_not_found(connection, data[1])
        elif data[0].lower() == "listranks":
            p = self.find_player(data[1])
            if p:
                ranks = ", ".join(p.ranks.split(","))
                yield from _send_list(connection, "Ranks", p.alias, ranks)
            else:
                yield from _send_user_not_found(connection, data[1])
        else:
            yield from send_message(connection, 
                                    "Argument not recognized. See /user "
                                    "help for usage info.")

    @Command("list_players",
             perm="player_manager.list_players",
             doc="Lists all players.",
             syntax=("[wildcards]",))
    def _list_players(self, data, connection):
        """
        List the players in the database. Wildcard formats are allowed in this
        search (not really. NotImplemementedYet...) Careful, this list can get
        pretty big for a long running or popular server.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        with db_session(self.session) as session:
            players = session.query(Player).order_by(Player.alias).all()
            send_message(connection,
                         "{} players found:".format(len(players)))
            for x, player in enumerate(players):
                player_info = "  {0}. {1}{2}"
                if player.logged_in:
                    l = " (logged-in, ID: {})".format(player.client_id)
                else:
                    l = ""
                send_message(connection, player_info.format(x + 1, player.alias,
                                                            l))

    @Command("del_player",
             perm="player_manager.delete_player",
             doc="Deletes a player",
             syntax=("(username)",
                     "[*force=forces deletion of a logged in player."
                     " ^red;NOT RECOMMENDED^reset;.]"))
    def _delete_player(self, data, connection):
        """
        Removes a player from the player database. By default. you cannot
        remove a logged-in player, so either they need to be removed from
        the server first, or you have to apply the *force operation.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        :raise: NameError if is not available. ValueError if player is
                currently logged in.
        """

        if not data:
            send_message(connection, "No arguments provided.")
            return
        if data[-1] == "*force":
            force = True
            data.pop()
        else:
            force = False
        alias = " ".join(data)
        player = self.find_player(alias)
        if player is None:
            raise NameError
        if player.priority >= connection.player.priority:
            send_message(connection, "Can't delete {}, they are equal or "
                                     "higher rank than you!"
                         .format(player.alias))
            return
        if (not force) and player.logged_in:
            raise ValueError(
                "Can't delete a logged-in player; please kick them first. If "
                "absolutely necessary, append *force to the command.")
        with db_session(self.session) as session:
            session.delete(player.record)
            session.commit()
        send_message(connection, "Player {} has been deleted.".format(alias))
