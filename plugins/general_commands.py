"""
StarryPy General Commands Plugin

Plugin for handling most of the most basic (and most useful) commands.

Original authors: AMorporkian
Updated for release: kharidiron
"""

import asyncio
import datetime

from data_parser import ConnectFailure, GiveItem
from packet_parser import packets, build_packet
from plugin_manager import SimpleCommandPlugin
from plugins.storage_manager import db_session, SessionAccessMixin
from server import shutdown
from utilities import send_message, Command, broadcast


###

class GeneralCommands(SessionAccessMixin, SimpleCommandPlugin):
    name = "general_commands"
    depends = ["command_dispatcher", "player_manager"]
    default_config = {"maintenance_message": "This server is currently in "
                                             "maintenance mode and is not "
                                             "accepting new connections."}

    def __init__(self):
        super().__init__()
        self.maintenance = False
        self.rejection_message = ""
        self.start_time = None

    def activate(self):
        super().activate()
        self.maintenance = False
        self.rejection_message = self.config.get_plugin_config(self.name)[
            "maintenance_message"]
        self.start_time = datetime.datetime.now()

    # Packet hooks - look for these packets and act on them

    def on_client_connect(self, data, connection):
        uuid = data["parsed"]["uuid"]
        player = self.plugins.player_manager.get_player_by_uuid(uuid)
        if self.maintenance and not player.perm_check(
                "general_commands.maintenance_bypass"):
            fail = ConnectFailure.build(dict(reason=self.rejection_message))
            pkt = build_packet(packets['connect_failure'], fail)
            yield from connection.raw_write(pkt)
            return False
        else:
            return True

    # Helper functions - Used by commands

    def generate_whois(self, target):
        """
        Generate the whois data for a player, and return it as a formatted
        string.

        :param target: Player object to be looked up.
        :return: String: The data about the player.
        """

        logged_in = "(^green;Online^reset;)"
        last_seen = "Now"
        if not target.logged_in:
            logged_in = "(^red;Offline^reset;)"
            last_seen = target.last_seen.strftime("%Y-%m-%d %H:%M:%S")
        return ("^orange;Name:^reset; {} {}\n"
                "^orange;Raw Name:^reset; {}\n"
                "^orange;Ranks:^reset; {}\n"
                "^orange;UUID:^reset; {}\n"
                "^orange;IP address: ^cyan;{}^reset;\n"
                "^orange;Current location:^reset; {}\n"
                "^orange;First seen:^reset; {}\n"
                "^orange;Last seen:^reset; {}".format(
                    target.alias, logged_in,
                    target.name,
                    target.ranks,
                    target.uuid,
                    target.current_ip,
                    target.location,
                    target.first_seen.strftime("%Y-%m-%d %H:%M:%S"),
                    last_seen))

    # Commands - In-game actions that can be performed

    @Command("who",
             perm="general_commands.who",
             doc="Lists players who are currently logged in.")
    def _who(self, data, connection):
        """
        Return a list of players currently logged in.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        ret_list = []
        for player in self.plugins.player_manager.players_online():
            if connection.player.perm_check("general_commands.who_clientids"):
                ret_list.append(
                    "[^red;{}^reset;] {}{}^reset;".format(player.client_id,
                                                          player.chat_prefix,
                                                          player.alias))
            else:
                ret_list.append("{}{}^reset;".format(player.chat_prefix,
                                                     player.alias))
        send_message(connection,
                     "{} players online:\n{}".format(len(ret_list),
                                                     ", ".join(ret_list)))

    @Command("whois",
             perm="general_commands.whois",
             doc="Returns client data about the specified user.",
             syntax="(username)")
    def _whois(self, data, connection):
        """
        Display information about a player.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        :raise: SyntaxWarning if no name provided.
        """

        if len(data) == 0:
            raise SyntaxWarning("No target provided.")
        name = " ".join(data)
        info = self.plugins.player_manager.find_player(name)
        if info is not None:
            send_message(connection, self.generate_whois(info))
        else:
            send_message(connection, "Player not found!")

    @Command("give", "item", "give_item",
             perm="general_commands.give_item",
             doc="Gives an item to a player. "
                 "If player name is omitted, give item(s) to self.",
             syntax=("[player=self]", "(item name)", "[count=1]"))
    def _give_item(self, data, connection):
        """
        Give item(s) to a player.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        :raise: SyntaxWarning if too many arguments provided or item count
                cannot be properly converted. NameError if a target player
                cannot be resolved.
        """

        arg_count = len(data)
        player = self.plugins.player_manager.find_player(data[0])
        if arg_count == 1:
            player = connection.player
            item = data[0]
            count = 1
        elif arg_count == 2:
            if data[1].isdigit():
                player = connection.player
                item = data[0]
                count = int(data[1])
            else:
                item = data[1]
                count = 1
        elif arg_count == 3:
            item = data[1]
            if not data[2].isdigit():
                raise SyntaxWarning("Couldn't convert %s to an item count." %
                                    data[2])
            count = int(data[2])
        else:
            raise SyntaxWarning("Too many arguments")
        if player is None:
            raise NameError(player)
        target = self.plugins.player_manager.get_connection(connection, player)
        if count > 10000 and item != "money":
            count = 10000
        item_base = GiveItem.build(dict(name=item,
                                        count=count,
                                        variant_type=7,
                                        description=""))
        item_packet = build_packet(packets['give_item'], item_base)
        yield from target.raw_write(item_packet)
        send_message(connection,
                     "Gave {} (count: {}) to {}".format(
                         item,
                         count,
                         target.player.alias))
        send_message(target, "{} gave you {} (count: {})".format(
            connection.player.alias, item, count))

    @Command("nick",
             perm="general_commands.nick",
             doc="Changes your nickname to another one.",
             syntax="(username)")
    def _nick(self, data, connection):
        """
        Change your name as it is displayed in the chat window.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        if len(data) > 1 and connection.player.perm_check(
                "general_commands.nick_others"):
            target = self.plugins.player_manager.find_player(data[0])
            alias = " ".join(data[1:])
        else:
            alias = " ".join(data)
            target = connection.player
        if len(data) == 0:
            alias = connection.player.name
        if self.plugins.player_manager.get_player_by_alias(alias):
            raise ValueError("There's already a user by that name.")
        else:
            clean_alias = self.plugins.player_manager.clean_name(alias)
            if clean_alias is None:
                send_message(connection,
                             "Nickname contains no valid characters.")
                return
            old_alias = target.alias
            with db_session(self.session) as session:
                target.alias = clean_alias
                session.commit()
            broadcast(connection, "{}'s name has been changed to {}".format(
                old_alias, clean_alias))

    @Command("serverwhoami",
             perm="general_commands.whoami",
             doc="Displays your current nickname for chat.")
    def _whoami(self, data, connection):
        """
        Displays your current nickname and connection information.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        send_message(connection,
                     self.generate_whois(connection.player))

    @Command("here",
             perm="general_commands.here",
             doc="Displays all players on the same planet as you.")
    def _here(self, data, connection):
        """
        Displays all players on the same planet as the user.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        ret_list = []
        location = str(connection.player.location)
        for player in self.plugins.player_manager.players_here(location):
            if connection.player.perm_check(
                    "general_commands.who_clientids"):
                ret_list.append(
                    "[^red;{}^reset;] {}{}^reset;"
                        .format(player.client_id,
                                player.chat_prefix,
                                player.alias))
            else:
                ret_list.append("{}{}^reset;".format(
                    player.chat_prefix, player.alias))
        send_message(connection,
                     "{} players on planet:\n{}".format(len(ret_list),
                                                        ", ".join(ret_list)))

    @Command("uptime",
             perm="general_commands.uptime",
             doc="Displays the time since the server started.")
    def _uptime(self, data, connection):
        """
        Displays the time since the server started.
        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        now = datetime.datetime.now() - self.start_time
        text = "Uptime: {} days, {} hours, {} minutes."
        yield from send_message(connection, text.format(now.days,
                                                        now.seconds//3600,
                                                        (now.seconds//60) % 60))

    @Command("shutdown",
             perm="general_commands.shutdown",
             doc="Shutdown the server after N seconds (default 5).",
             syntax="[time]")
    def _shutdown(self, data, connection):
        """
        Shutdown the StarryPy server, disconnecting everyone.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        self.logger.warning("{} has called for a shutdown.".format(
            connection.player.alias))
        shutdown_time = 5
        if data:
            if data[0].isdigit():
                self.logger.debug("We think it is an int, lets use it.")
                shutdown_time = int(data[0])

        broadcast(self, "^red;(ADMIN) The server is shutting down in {} "
                        "seconds.^reset;".format(shutdown_time))
        yield from asyncio.sleep(shutdown_time)
        self.logger.warning("Shutting down server now.")
        shutdown()

    @Command("maintenance_mode",
             perm="general_commands.maintenance_mode",
             doc="Toggle maintenance mode on the server. While in "
                 "maintenance mode, the server will reject all new "
                 "connection.")
    def _maintenance(self, data, connection):
        if self.maintenance:
            self.maintenance = False
            broadcast(self, "^red;NOTICE: Maintenance mode disabled. "
                            "^reset;New connections are allowed.")
            self.logger.info("Maintenance mode is now turned off.")
        else:
            self.maintenance = True
            broadcast(self, "^red;NOTICE: The server is now in maintenance "
                            "mode. ^reset;No additional clients can connect.")
            self.logger.info("Maintenance mode is now turned on.")
