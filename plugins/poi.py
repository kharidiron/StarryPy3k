"""
StarryPy POI Plugin

Plugin to move players' ships to points of interest designated by admins.

Original code by: kharidiron
Reimplemented by: medeor413
"""

import asyncio
import sqlalchemy as sqla

from data_parser import FlyShip
from packet_parser import build_packet, packets
from plugin_manager import SimpleCommandPlugin
from plugins.storage_manager import (DeclarativeBase, SessionAccessMixin,
                                     db_session)
from utilities import Command, send_message, SystemLocationType


###

class POI(DeclarativeBase):
    __tablename__ = "poi"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    location = sqla.Column(sqla.String(64))
    name = sqla.Column(sqla.String(64))

    def __str__(self):
        return

    def __repr__(self):
        return ("<POI(name={}, location={})>" 
                "".format(self.name, self.location))


class PointsOfInterest(SessionAccessMixin, SimpleCommandPlugin):
    name = "poi"
    depends = ["command_dispatcher"]

    def __init__(self):
        super().__init__()

    def activate(self):
        super().activate()

    # Helper functions - Used by commands

    @asyncio.coroutine
    def _move_ship(self, connection, name):
        """
        Generate packet that moves ship.

        :param connection: Player being moved.
        :param name: The intended destination of the player.
        :return: Null.
        :raise: NotImplementedError when POI does not exist.
        """

        with db_session(self.session) as session:
            target = session.query(POI).filter_by(name=name).first()
            if not target:
                send_message(connection, "That POI does not exist!")
                raise NotImplementedError

            from plugins.player_manager import Planet
            location = target.location
            poi = session.query(Planet).filter_by(location=location).first()
            destination = FlyShip.build(dict(
                world_x=poi.x,
                world_y=poi.y,
                world_z=poi.z,
                location=dict(
                    type=SystemLocationType.COORDINATE,
                    world_x=poi.x,
                    world_y=poi.y,
                    world_z=poi.z,
                    world_planet=poi.orbit,
                    world_satellite=poi.satellite
                )
            ))
            flyship_packet = build_packet(packets["fly_ship"], destination)
            yield from connection.client_raw_write(flyship_packet)

    # Commands - In-game actions that can be performed

    @Command("poi",
             perm="poi.poi",
             doc="Moves a player's ship to the specified Point of Interest, "
                 "or prints the POIs if no argument given.",
             syntax="[\"][POI name][\"]")
    def _poi(self, data, connection):
        """
        Move a players ship to the specified POI, free of fuel charge,
        no matter where they are in the universe.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        if len(data) == 0:
            with db_session(self.session) as session:
                results = session.query(POI).all()
            poi_list = []
            for poi in results:
                poi_list.append(poi.name)
            pois = ", ".join(poi_list)
            send_message(connection, "Points of Interest: {}".format(pois))
            return
        location = connection.player.location
        name = " ".join(data).lower()
        if location != "{}'s ship".format(connection.player.alias):
            send_message(connection,
                         "You must be on your ship for this to work.")
            return
        try:
            yield from self._move_ship(connection, name)
            send_message(connection,
                         "Now en route to {}. Please stand by...".format(name))
            self.logger.info(
                "{} flying to poi {}.".format(connection.player.alias, name))
        except NotImplementedError:
            pass

    @Command("set_poi",
             perm="poi.set_poi",
             doc="Set the planet you're on as a POI.",
             syntax="[\"](POI name)[\"]")
    def _set_poi(self, data, connection):
        """
        Set the current planet as a Point of Interest. Note, you must be
        standing on a planet for this to work.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        location = connection.player.location
        if len(data) == 0:
            send_message(connection, "No name for POI specified.")
            return
        if not str(location).startswith("CelestialWorld"):
            send_message(connection,
                         "You must be standing on a planet for this to work.")
            return
        name = " ".join(data).lower()
        with db_session(self.session) as session:
            target = session.query(POI).filter_by(name=name).first()
            if target:
                send_message(connection, "A POI with this name already exists!")
                return
            poi = POI(name=name, location=location)
            session.add(poi)
            session.commit()
            send_message(connection, "POI {} added to list!".format(name))
            self.logger.info(
                "{} added the poi {}.".format(connection.player.alias, name))

    @Command("del_poi",
             perm="poi.set_poi",
             doc="Remove the specified POI from the POI list.",
             syntax="[\"](POI name)[\"]")
    def _del_poi(self, data, connection):
        """
        Remove the specified Point of Interest from the POI list.

        :param data: The packet containing the command.
        :param connection: The connection from which the packet came.
        :return: Null.
        """

        if len(data) == 0:
            send_message(connection, "No POI specified.")
            return
        name = " ".join(data).lower()
        with db_session(self.session) as session:
            poi = session.query(POI).filter_by(name=name).first()
            if not poi:
                send_message(connection, "That POI does not exist.")
                return
            session.delete(poi)
            session.commit()
            send_message(connection, "Deleted POI {}.".format(name))
            self.logger.info(
                "{} deleted the poi {}.".format(connection.player.alias, name))
