"""
StarryPy Planet Announcer Plugin

Announces to all players on a world when another player enters the world.
Allows Admins to set a custom greeting message on a world.

Reimplemented for StarryPy3k by medeor413.
"""

import asyncio

import sqlalchemy as sqla

from plugin_manager import SimpleCommandPlugin
from plugins.storage_manager import (DeclarativeBase, SessionAccessMixin,
                                     db_session, cache_query)
from utilities import send_message, Command


###

class Greeting(DeclarativeBase):
    __tablename__ = "greetings"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    location = sqla.Column(sqla.String(255))
    greeting = sqla.Column(sqla.String(255))

    def __repr__(self):
        return "<Greeting(location={}, greeting={})>".format(
            self.location, self.greeting)

    def __str__(self):
        return "{}".format(self.greeting)


class PlanetAnnouncer(SessionAccessMixin, SimpleCommandPlugin):
    name = "planet_announcer"
    depends = ["player_manager", "command_dispatcher"]

    def __init__(self):
        super().__init__()

    def activate(self):
        super().activate()

    # Packet hooks - look for these packets and act on them

    def on_world_start(self, data, connection):
        asyncio.ensure_future(self._announce(connection))
        return True

    # Helper functions - Used by hooks and commands

    @asyncio.coroutine
    def _announce(self, connection):
        """
        Announce to all players in the world when a new player beams in,
        and display the greeting message to the new player, if set.

        :param connection: The connection of the player beaming in.
        :return: Null.
        """

        yield from asyncio.sleep(.5)
        location = str(connection.player.location)
        for p in self.plugins.player_manager.players_online():
            target = self.plugins.player_manager.get_connection(connection, p)
            if str(p.location) == location and target != connection:
                send_message(target, "{} has arrived at this location!"
                             .format(connection.player.alias))
        greeting = self._check_greeting(location)
        if greeting:
            send_message(connection, str(greeting))

    def _check_greeting(self, location) -> Greeting:
        """
        Check if the current location has a greeting.

        :param location:
        :return: String. Location greeting.
        """

        with db_session(self.session) as session:
            greeting = session.query(Greeting).filter_by(
                location=location).first()
            return cache_query(greeting)

    # Commands - In-game actions that can be performed

    @Command("set_greeting",
             perm="planet_announcer.set_greeting",
             doc="Sets the greeting message to be displayed when a player "
                 "enters this planet, or clears it if unspecified.")
    def _set_greeting(self, data, connection):
        """
        Set the greeting on a planet.

        If the planet doesn't already have a greeting, add one. If no
        greeting is provided, remove any existing one. Otherwise, update
        the existing  greeting.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        location = str(connection.player.location)
        msg = " ".join(data)
        with db_session(self.session) as session:
            greeting = session.query(Greeting).filter_by(
                location=location).first()
            if not greeting:
                if not msg:
                    response = "No greeting set."
                else:
                    greeting = Greeting(location=location, greeting=msg)
                    session.add(greeting)
                    response = "Greeting message set to '{}'.".format(msg)
                    self.logger.debug("Greeting at {} set to {} by {}."
                                      "".format(location, msg,
                                                connection.player.alias))
            else:
                if not msg:
                    session.delete(greeting)
                    response = "Greeting message cleared."
                    self.logger.debug("Greeting at {} removed by {}."
                                      "".format(location, 
                                                connection.player.alias))
                else:
                    greeting.greeting = msg 
                    response = "Greeting message set to '{}'.".format(msg)
                    self.logger.debug("Greeting at {} changed to {} by {}."
                                      "".format(location, msg,
                                                connection.player.alias))
            session.commit()
            yield from send_message(connection, response)

