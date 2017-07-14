"""
StarryPy Chat Enhancements Plugin

Provides enhancements to the vanilla Starbound chat, including colored names
based on user roles, and timestamps per message.

Note: Name colors are in the wrapper only, and do not actually effect the
player's name tag.

Original authors: teihoo, FZFalzar
Updated for release: kharidiron
"""

import asyncio
import re
from datetime import datetime

import sqlalchemy as sqla

from data_parser import ChatSent
from packet_parser import packets, build_packet
from plugin_manager import SimpleCommandPlugin
from plugins.storage_manager import (DeclarativeBase, SessionAccessMixin,
                                     db_session)
from utilities import (Command, ChatSendMode, ChatReceiveMode, send_message,
                       link_plugin_if_available)


###

class Ignore(DeclarativeBase):
    __tablename__ = "ignores"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    player_uuid = sqla.Column(sqla.String(32))
    ignore_uuid = sqla.Column(sqla.String(32))

    def __repr__(self):
        return "<Ignore({} is ignoring {}.)>".format(
            self.player_uuid, self.ignore_uuid)


class ChatEnhancements(SessionAccessMixin, SimpleCommandPlugin):
    name = "chat_enhancements"
    depends = ["player_manager", "command_dispatcher", "chat_manager"]
    default_config = {"chat_timestamps": True,
                      "timestamp_color": "^gray;"}

    def __init__(self):
        super().__init__()
        self.command_dispatcher = None
        self.colors = None
        self.cts = None
        self.cts_color = None
        self.last_whisper = {}
        self.social_spies = set()

    def activate(self):
        super().activate()
        self.command_dispatcher = self.plugins.command_dispatcher.plugin_config
        self.cts = self.config.get_plugin_config(self.name)["chat_timestamps"]
        self.cts_color = self.config.get_plugin_config(self.name)[
            "timestamp_color"]
        self.last_whisper = {}
        self.social_spies = set()
        link_plugin_if_available(self, "irc_bot")

    # Packet hooks - look for these packets and act on them

    def on_connect_success(self, data, connection):
        """
        Catch when a successful connection is established. Set the player's
        chat style to be the server default.

        :param data:
        :param connection:
        :return: Boolean: True. Must be true, so that packet get passed on.
        """
        return True

    def on_chat_received(self, data, connection):
        sender = ""
        if data["parsed"]["name"]:
            if data["parsed"]["name"] == "server":
                joinmsg = re.match(r"Player '(.*)' (dis)?connected",
                                   data['parsed']['message'])
                if joinmsg:
                    joiner = self.plugins.player_manager.find_player(
                        joinmsg.group(1))
                    msg_type = "left" if joinmsg.group(2) is not None \
                        else "joined"
                    data['parsed']['message'] = "{}{}^reset; has {} the " \
                                                "server.".format(
                        joiner.chat_prefix, joiner.alias, msg_type)
                    sender = self.make_timestamp()
            else:
                sender = self.plugins.player_manager.find_player(
                    data["parsed"]["name"])
                if self.check_ignored(connection.player, sender):
                    return False
                try:
                    sender = self.decorate_line(sender)
                except AttributeError:
                    self.logger.warning("Sender {} is sending a message that "
                                        "the wrapper isn't handling correctly"
                                        "".format(data["parsed"]["name"]))
                    sender = data["parsed"]["name"]

        yield from send_message(connection,
                                data["parsed"]["message"],
                                mode=data["parsed"]["header"]["mode"],
                                client_id=data["parsed"]["header"]["client_id"],
                                name=sender,
                                channel=data["parsed"]["header"]["channel"])

    def on_chat_sent(self, data, connection):
        """
        Catch when someone sends a message. Add a timestamp to the message (if
        that feature is turned on). Colorize the player's name based on their
        role.

        :param data: The packet containing the message.
        :param connection: The connection from which the packet came.
        :return: Boolean. True if an error occurred while generating a colored
                 name (so that we don't stop the packet from flowing). Null if
                 the message came from the server (since it doesn't need
                 colors) or if the message is a command.
        """

        message = data['parsed']['message']
        if message.startswith(self.command_dispatcher.command_prefix):
            return True
        if self.plugins.chat_manager.mute_check(connection.player):
            return False

        sender = self.decorate_line(connection.player)
        if data["parsed"]["send_mode"] in [ChatSendMode.LOCAL,
                                           ChatSendMode.PARTY]:
            yield from self.send_to_spies(connection, sender, 
                                          data["parsed"]["message"])

        return True

    # Helper functions - Used by commands

    def decorate_line(self, player):
        timestamp = "{}> <".format(self.make_timestamp())
        try:
            sender = timestamp + player.chat_prefix + player.alias + "^reset;"
        except AttributeError as e:
            self.logger.warning(
                "AttributeError in colored_name: {}".format(str(e)))
            sender = player.alias
        return sender

    def make_timestamp(self):
        if self.cts:
            return self.cts_color + datetime.now().strftime("%H:%M") + "^reset;"
        else:
            return ""

    def check_ignored(self, player, target):
        """
        Check if a player is ignoring another player.

        :param player:
        :param target:
        :return:
        """

        with db_session(self.session) as session:
            ignored = session.query(Ignore).filter_by(
                player_uuid=player.uuid,
                ignore_uuid=target.uuid).first()
            return ignored

    @asyncio.coroutine
    def _send_to_server(self, message, mode, connection):
        msg_base = ChatSent.build(dict(message=" ".join(message),
                                       send_mode=mode))
        msg_packet = build_packet(packets['chat_sent'], msg_base)
        yield from connection.client_raw_write(msg_packet)

    @asyncio.coroutine
    def send_to_spies(self, connection, sender, message):
        ssmsg = "[SS]: " + message
        for p in self.social_spies:
            target = self.plugins.player_manager.get_connection(connection, p)
            yield from send_message(target,
                                    ssmsg,
                                    mode=ChatReceiveMode.BROADCAST,
                                    name=sender)

    # Commands - In-game actions that can be performed

    @Command("l",
             doc="Send message only to people on same world.",
             syntax="(message)")
    def _local(self, data, connection):
        """
        Local chat. Sends a message only to characters who are on the same
        planet.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null
        """

        if self.plugins.chat_manager.mute_check(connection.player):
            send_message(connection, "You are muted and cannot chat.")
            return False
        if data:
            yield from self._send_to_server(data,
                                            ChatSendMode.LOCAL,
                                            connection)
            sender = self.decorate_line(connection.player)
            yield from self.send_to_spies(connection, sender, " ".join(data))
            return True

    @Command("u",
             doc="Send message to the entire universe.",
             syntax="(message)")
    def _universe(self, data, connection):
        """
        Universal chat. Sends a message that everyone can see.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null
        """

        if self.plugins.chat_manager.mute_check(connection.player):
            send_message(connection, "You are muted and cannot chat.")
            return False
        if data:
            yield from self._send_to_server(data,
                                            ChatSendMode.UNIVERSE,
                                            connection)
            if link_plugin_if_available(self, "irc_bot"):
                # Try sending it to IRC if we have that available.
                asyncio.ensure_future(
                    self.plugins.irc_bot.bot_write(
                        "<{}> {}".format(connection.player.alias,
                                         " ".join(data))))
            if link_plugin_if_available(self, "discord_bot"):
                discord = self.plugins.discord_bot
                asyncio.ensure_future(
                    discord.bot_write("**<{}>** {}".format(
                        connection.player.alias, " ".join(data))))
            return True

    @Command("p",
             doc="Send message to only party members.")
    def _party(self, data, connection):
        """
        Party chat. Sends a message to only members of your party.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null
        """

        if self.plugins.chat_manager.mute_check(connection.player):
            send_message(connection, "You are muted and cannot chat.")
            return False
        if data:
            yield from self._send_to_server(data,
                                            ChatSendMode.PARTY,
                                            connection)
            sender = self.decorate_line(connection.player)
            yield from self.send_to_spies(connection, sender, " ".join(data))
            return True

    @Command("whisper", "w",
             perm="chat_enhancements.whisper",
             doc="Send message privately to a person.")
    def _whisper(self, data, connection):
        """
        Whisper. Sends a message to only one person.

        This command shadows the built-in whisper.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null
        """

        try:
            name = data[0]
        except IndexError:
            raise SyntaxWarning("No target provided.")

        recipient = self.plugins.player_manager.find_player(name)
        if recipient is not None:
            if not recipient.logged_in:
                send_message(connection,
                             "Player {} is not currently logged in."
                             "".format(recipient.alias))
                return False
            if self.check_ignored(recipient, connection.player):
                send_message(connection, "Cannot send message to player {} "
                                         "as you are currently ignoring "
                                         "them.".format(recipient.alias))
                return False
            if self.check_ignored(connection.player, recipient):
                send_message(connection, "Player {} is currently ignoring you."
                             .format(recipient.alias))
                return False
            self.last_whisper[recipient.uuid] = connection.player.alias
            self.last_whisper[connection.player.uuid] = recipient.alias
            message = " ".join(data[1:])
            client_id = connection.player.client_id
            sender = self.decorate_line(connection.player)
            send_mode = ChatReceiveMode.WHISPER
            channel = "Private"
            target = self.plugins.player_manager.get_connection(connection,
                                                                recipient)
            yield from send_message(target,
                                    message,
                                    client_id=client_id,
                                    name=sender,
                                    mode=send_mode,
                                    channel=channel)
            yield from send_message(connection,
                                    message,
                                    client_id=client_id,
                                    name=sender,
                                    mode=send_mode,
                                    channel=channel)
            sssender = "{} -> {}{}^reset;".format(sender,
                                                  recipient.chat_prefix,
                                                  recipient.alias)
            yield from self.send_to_spies(connection, sssender, message)
        else:
            yield from send_message(connection,
                                    "Couldn't find a player with name {}"
                                    "".format(name))

    @Command("reply", "r",
             perm="chat_enhancements.whisper",
             doc="Send message privately to the last person who privately "
                 "messaged you.")
    def _reply(self, data, connection):
        """
        Reply. Sends a message to the last person who whispered you.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null
        """

        if connection.player.uuid in self.last_whisper:
            name = self.last_whisper[connection.player.uuid]
            recipient = self.plugins.player_manager.find_player(name)
        else:
            recipient = None
        if recipient is not None:
            if not recipient.logged_in:
                send_message(connection,
                             "Player {} is not currently logged in."
                             "".format(recipient.alias))
                return False
            if self.check_ignored(recipient, connection.player):
                send_message(connection, "Cannot send message to player {} "
                                         "as you are currently ignoring "
                                         "them.".format(recipient.alias))
                return False
            if self.check_ignored(connection.player, recipient):
                send_message(connection, "Player {} is currently ignoring you."
                             .format(recipient.alias))
                return False
            self.last_whisper[recipient.uuid] = connection.player.alias
            self.last_whisper[connection.player.uuid] = recipient.alias
            message = " ".join(data)
            client_id = connection.player.client_id
            sender = self.decorate_line(connection.player)
            send_mode = ChatReceiveMode.WHISPER
            channel = "Private"
            target = self.plugins.player_manager.get_connection(connection,
                                                                recipient)
            yield from send_message(target,
                                    message,
                                    client_id=client_id,
                                    name=sender,
                                    mode=send_mode,
                                    channel=channel)
            yield from send_message(connection,
                                    message,
                                    client_id=client_id,
                                    name=sender,
                                    mode=send_mode,
                                    channel=channel)
            sssender = "{} -> {}{}^reset;".format(sender,
                                                  recipient.chat_prefix,
                                                  recipient.alias)
            yield from self.send_to_spies(connection, sssender, message)
        else:
            yield from send_message(connection,
                                    "You haven't been messaged by anyone.")

    @Command("ignore",
             perm="chat_enhancements.ignore",
             doc="Ignores a player, preventing you from seeing their "
                 "messages. Use /ignore again to toggle.")
    def _ignore(self, data, connection):
        player = connection.player.uuid
        try:
            name = data[0]
        except IndexError:
            raise SyntaxWarning("No target provided.")
        target = self.plugins.player_manager.find_player(name)
        if target is not None:
            if target == connection.player:
                send_message(connection, "Can't ignore yourself!")
                return False
            with db_session(self.session) as session:
                ignored = session.query(Ignore).filter_by(
                    player_uuid=player, ignore_uuid=target.uuid).first()
                if not ignored:
                    ignore = Ignore(player_uuid=player, ignore_uuid=target.uuid)
                    session.add(ignore)
                    yield from send_message(connection,
                                            "User {} added to ignores "
                                            "list.".format(target.alias))
                else:
                    session.delete(ignored)
                    yield from send_message(connection,
                                            "User {} removed from ignores "
                                            "list.".format(target.alias))
                session.commit()

    @Command("socialspy",
             perm="chat_enhancements.socialspy",
             doc="Allows staff to read local, party, and whisper "
                 "communications.")
    def _socialspy(self, data, connection):
        if connection.player in self.social_spies:
            self.social_spies.remove(connection.player)
            yield from send_message(connection, "Social spy disabled.")
            self.logger.debug(
                "{} has disabled social spy.".format(connection.player.alias))
        else:
            self.social_spies.add(connection.player)
            yield from send_message(connection, "Social spy enabled.")
            self.logger.debug(
                "{} has enabled social spy.".format(connection.player.alias))
