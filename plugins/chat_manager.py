"""
StarryPy Chat Manager Plugin

Provides core chat management features, such as mute...and that's it right now.
Future features could be added...

Original authors: AMorporkian
Updated for release: kharidiron
"""

from plugin_manager import SimpleCommandPlugin
from plugins.storage_manager import SessionAccessMixin, db_session
from utilities import Command, send_message


###


class ChatManager(SessionAccessMixin, SimpleCommandPlugin):
    name = "chat_manager"
    depends = ["player_manager", "command_dispatcher"]

    def __init__(self):
        super().__init__()

    def activate(self):
        super().activate()

    # Packet hooks - look for these packets and act on them

    def on_chat_sent(self, data, connection):
        """
        Catch when someone sends a message.

        :param data: The packet containing the message.
        :param connection: The connection from which the packet came.
        :return: Boolean. True if we're done with the packet here, False if the
                 player is muted (preventing packet from being passed along.
                 Commands are treated as truthy values.
        """

        message = data["parsed"]["message"]
        if message.startswith(
                self.plugins.command_dispatcher.plugin_config.command_prefix):
            return True

        if self.mute_check(connection.player):
            send_message(connection, "You are muted and cannot chat.")
            return False

        return True

    # Helper functions - Used by commands

    def mute_check(self, player):
        """
        Utility function to verifying if target player is muted.

        :param player: Target player to check.
        :return: Boolean. True if player is muted, False if they are not.
        """

        return player.muted

    # Commands - In-game actions that can be performed

    @Command("mute",
             perm="chat_manager.mute",
             doc="Mutes a user",
             syntax="(username)")
    def _mute(self, data, connection):
        """
        Mute command. Pulls target's name from data stream. Check if valid
        player. Also check if player can be muted, or is already muted.
        Mute target when possible.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null
        """

        alias = " ".join(data)
        player = self.plugins.player_manager.find_player(alias)
        if player is None:
            raise NameError
        elif self.mute_check(player):
            send_message(connection,
                         "{} is already muted.".format(player.alias))
            return
        elif player.priority >= connection.player.priority:
            send_message(connection,
                         "{} is unmuteable.".format(player.alias))
            return
        else:
            with db_session(self.session) as session:
                player.muted = True
                session.commit()
            # FIXME - replace with get_connection from player manager
            for c in connection.factory.connections:
                if player.uuid == c.player.uuid:
                    target_connection = c
                    send_message(target_connection,
                                 "{} has muted you.".format(
                                     connection.player.alias))
                    break
            else:
                send_message(connection,
                             "{} is not connected.".format(player.alias))
            send_message(connection,
                         "{} has been muted.".format(player.alias))
            self.logger.info("{} has muted {}.".format(connection.player.alias,
                                                       player.alias))

    @Command("unmute",
             perm="chat_manager.mute",
             doc="Unmutes a player",
             syntax="(username)")
    def _unmute(self, data, connection):
        """
        Unmute command. Pulls target's name from data stream. Check if valid
        player. Check that player is actually muted. If possible, unmute
        the target.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null
        """

        alias = " ".join(data)
        player = self.plugins.player_manager.find_player(alias)
        if player is None:
            raise NameError
        elif not self.mute_check(player):
            send_message(connection,
                         "{} isn't muted.".format(player.alias))
            return
        else:
            with db_session(self.session) as session:
                player.muted = False
                session.commit()
            for c in connection.factory.connections:
                if player.uuid == c.player.uuid:
                    target_connection = c
                    send_message(target_connection,
                                 "{} has unmuted you.".format(
                                     connection.player.alias))
                    break
            else:
                send_message(connection,
                             "{} is not connected.".format(player.alias))
            send_message(connection,
                         "{} has been unmuted.".format(player.alias))
            self.logger.info("{} has unmuted {}.".format(connection.player.alias,
                                                         player.alias))
