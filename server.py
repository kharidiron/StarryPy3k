import asyncio
import logging
import sys
import signal

from configuration_manager import ConfigurationManager
from data_parser import ChatReceived, WorldStop, ServerDisconnect
from packet_parser import build_packet, packets
from plugin_manager import PluginManager
from utilities import path, read_packet, State, Direction, ChatReceiveMode


class GracefulExit(SystemExit):
    code = 1


def raise_graceful_exit():
    raise GracefulExit()


def shutdown():
    raise GracefulExit


class Client:
    """
    Client class.

    Every client that connects to the server instantiates
    as one of these. Store all the client connection information here
    along with some utility functions for working with clients.
    """

    def __init__(self, reader, writer, configuration_manager, factory):
        logger.debug("Initializing connection.")
        self._reader = reader
        self._writer = writer
        self.config = configuration_manager.config
        self.factory = factory

        self.state = None
        self._alive = True
        self.client_ip = reader._transport.get_extra_info('peername')[0]
        self.server_loop = loop.create_task(self.server_listener())
        self.client_loop = None
        self._client_reader = None
        self._client_writer = None
        logger.info("Received connection from {}".format(self.client_ip))

    # noinspection PyTypeChecker,PyTupleAssignmentBalance
    @asyncio.coroutine
    def server_listener(self):
        """
        Server listener. Listens for packets from client bound
        for server.

        First we establish the client-to-server stream connection.
        We then infinitely loop listening for packets from the client.
        If we get one, we check if we can do anything with it, and
        then ultimately pass it to the line to be sent on to the
        server.

        :return:
        """

        logger.debug("Instantiating client->server listener.")
        (self._client_reader, self._client_writer) = \
            yield from asyncio.open_connection(self.config['upstream_host'],
                                               self.config['upstream_port'])
        self.client_loop = loop.create_task(self.client_listener())
        try:
            while not loop.shutting_down:
                packet = yield from read_packet(self._reader,
                                                Direction.TO_SERVER)
                # # # Break in case of emergencies:
                # if packet['type'] in [0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12,
                #                       13, 14, 15, 16, 17, 18, 20, 21]:
                #    logger.debug('c->s  {}'.format(packet['type']))

                if (yield from self.check_plugins(packet)):
                    yield from self.write_client(packet)
        except asyncio.IncompleteReadError:
            # Pass on these errors. These occur when a player disconnects badly
            pass
        except asyncio.CancelledError:
            logger.warning("Connection ended abruptly.")
        except Exception as e:
            logger.error("Server loop exception occurred:"
                         "{}: {}".format(e.__class__.__name__, e))
        finally:
            logger.debug("Canceling server listener.")
            self.client_loop.cancel()
            self.die()

    # noinspection PyTypeChecker
    @asyncio.coroutine
    def client_listener(self):
        """
        Client listener. Listen for packets from server bound for
        this specific client.

        :return:
        """

        logger.debug("Instantiating server->client listener.")
        try:
            while not loop.shutting_down:
                packet = yield from read_packet(self._client_reader,
                                                Direction.TO_CLIENT)
                # # # Break in case of emergencies:
                # if packet['type'] in [0, 1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12,
                #                       13, 14, 15, 16, 17, 18, 20, 21]:
                #    logger.debug('s->c  {}'.format(packet['type']))

                send_flag = yield from self.check_plugins(packet)
                if send_flag:
                    yield from self.write(packet)
        except (asyncio.IncompleteReadError, asyncio.CancelledError):
            # The client has disconnected.
            logger.debug("Canceling client listener.")
        except ConnectionResetError:
            pass
        finally:
            self.die()

    @asyncio.coroutine
    def send_message(self, message, *messages, mode=ChatReceiveMode.BROADCAST,
                     client_id=0, name="", channel=""):
        """
        Convenience function to send chat messages to the client. Note that
        this does *not* send messages to the server at large; broadcast
        should be used for messages to all clients, or manually constructed
        chat messages otherwise.

        :param message: message text
        :param messages: used if there are more that one message to be sent
        :param client_id: who sent the message
        :param name:
        :param channel:
        :param mode:
        :return:
        """

        header = {"mode": mode, "channel": channel, "client_id": client_id}
        try:
            if messages:
                for m in messages:
                    yield from self.send_message(m,
                                                 mode=mode,
                                                 client_id=client_id,
                                                 name=name,
                                                 channel=channel)
            if "\n" in message:
                for m in message.splitlines():
                    yield from self.send_message(m,
                                                 mode=mode,
                                                 client_id=client_id,
                                                 name=name,
                                                 channel=channel)
                return

            if self.state >= State.CONNECTED:
                chat_packet = ChatReceived.build({"message": message,
                                                  "name": name,
                                                  "junk": 0,
                                                  "header": header})
                to_send = build_packet(packets['chat_received'], chat_packet)
                yield from self.raw_write(to_send)
        except Exception as e:
            logger.exception("Error while trying to send message.")
            logger.exception(e)

    @asyncio.coroutine
    def close_connection(self, connection):
        worldstop_packet = build_packet(packets["world_stop"],
                                        WorldStop.build(
                                            dict(reason="Removed")))
        yield from connection.raw_write(worldstop_packet)
        kick_string = ("You have been disconnected.\n"
                       " Reason: ^red;The server was shut down.^reset;")
        kick_packet = build_packet(packets["server_disconnect"],
                                   ServerDisconnect.build(
                                       dict(reason=kick_string)))
        yield from connection.raw_write(kick_packet)

    @asyncio.coroutine
    def raw_write(self, data):
        self._writer.write(data)
        yield from self._writer.drain()

    @asyncio.coroutine
    def client_raw_write(self, data):
        self._client_writer.write(data)
        yield from self._client_writer.drain()

    @asyncio.coroutine
    def write(self, packet):
        self._writer.write(packet['original_data'])
        yield from self._writer.drain()

    @asyncio.coroutine
    def write_client(self, packet):
        yield from self.client_raw_write(packet['original_data'])

    def die(self):
        """
        Handle closeout from player disconnecting.

        :return: Null.
        """

        if self._alive:
            if hasattr(self, "player"):
                logger.info("Removing player {}.".format(self.player.name))
            else:
                logger.info("Removing unknown player")
            self.state = State.DISCONNECTED
            self._alive = False
            self._writer.close()
            self._client_writer.close()
            self.factory.remove(self)

    @asyncio.coroutine
    def check_plugins(self, packet):
        return (yield from self.factory.plugin_manager.do(
            self,
            packets[packet['type']],
            packet))

    def __del__(self):
        try:
            self.die()
        except Exception:
            logger.error("An error occurred while a player was disconnecting.")


class ServerFactory:
    connections = []

    def __init__(self):
        try:
            self.configuration_manager = ConfigurationManager()
            self.configuration_manager.load_config(
                path / 'config' / 'config.json', default=True)
            self.plugin_manager = PluginManager(self.configuration_manager,
                                                factory=self)
            self.plugin_manager.load_from_path(
                path / self.configuration_manager.config.plugin_path)
            self.plugin_manager.resolve_dependencies()
            self.plugin_manager.activate_all()
            loop.create_task(self.plugin_manager.get_overrides())
            #loop.create_task(self._reap_dead_connections())
        except Exception:
            logger.exception("Error during server startup.", exc_info=True)

            loop.stop()
            sys.exit()

    @asyncio.coroutine
    def broadcast(self, messages, *, mode=ChatReceiveMode.RADIO_MESSAGE,
                  **kwargs):
        """
        Send a message to all connected clients.

        :param messages: Message(s) to be sent.
        :param mode: Mode bit of message.
        :return: Null.
        """

        for connection in self.connections:
            try:
                yield from connection.send_message(
                    messages,
                    mode=mode
                )
            except Exception as e:
                logger.exception("Error while trying to broadcast.")
                logger.exception(e)
                continue

    def remove(self, connection):
        """
        Remove a single connection.

        :param connection: Connection to be removed.
        :return: Null.
        """

        self.connections.remove(connection)

    def __call__(self, reader, writer):
        """
        Whenever a client connects, spin up a ClientFactory instance for it.

        :param reader: Reader transport socket.
        :param writer: Writer transport socket.
        :return: Null.
        """

        client = Client(reader, writer, self.configuration_manager,
                        factory=self)
        self.connections.append(client)
        logger.debug("New connection established.")

    @asyncio.coroutine
    def kill_all(self):
        """
        Drop all connections.

        :return: Null.
        """

        logger.debug("Dropping all connections.")
        for connection in self.connections:
            try:
                worldstop_packet = build_packet(packets["world_stop"],
                                                WorldStop.build(
                                                    dict(reason="Removed")))
                yield from connection.raw_write(worldstop_packet)
                # XXX - Doesn't seem to work consistently... Not sure wy not.
                kick_string = "^red;The server was shutdown.^reset;"
                kick_packet = build_packet(packets["server_disconnect"],
                                           ServerDisconnect.build(
                                               dict(reason=kick_string)))
                yield from connection.raw_write(kick_packet)
            except Exception as e:
                logger.exception("Error while closing connection")
                logger.exception(e)
            connection.die()


if __name__ == "__main__":
    DEBUG = True

    if DEBUG:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s # %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
    aiologger = logging.getLogger("asyncio")
    aiologger.setLevel(loglevel)
    logger = logging.getLogger('starrypy')
    logger.setLevel(loglevel)
    fh_d = None
    if DEBUG:
        fh_d = logging.FileHandler("debug.log")
        fh_d.setLevel(loglevel)
        fh_d.setFormatter(formatter)
        aiologger.addHandler(fh_d)
        logger.addHandler(fh_d)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    aiologger.addHandler(ch)

    signal.signal(signal.SIGINT, signal.default_int_handler)

    loop = asyncio.get_event_loop()
    loop.shutting_down = False
    loop.set_debug(False)

    logger.info("Starting server")

    server_factory = ServerFactory()
    config = server_factory.configuration_manager.config
    try:
        coro = asyncio.start_server(server_factory,
                                    port=config['listen_port'])
        server = loop.run_until_complete(coro)
    except OSError as err:
        logger.error("Error while trying to start server.")
        logger.error("{}".format(str(err)))
        sys.exit(1)

    try:
        loop.add_signal_handler(signal.SIGINT, raise_graceful_exit)
        loop.add_signal_handler(signal.SIGTERM, raise_graceful_exit)
    except NotImplementedError:
        pass

    try:
        loop.run_forever()
    except (KeyboardInterrupt, GracefulExit):
        logger.warning("Exiting")
    except Exception as e:
        logger.warning('An exception occurred: {}'.format(e))
    finally:
        loop.shutting_down = True
        shutdown = loop.create_task(server_factory.kill_all())
        loop.run_until_complete(shutdown)
        server_factory.configuration_manager.save_config()
        server_factory.plugin_manager.close_parser()
        server_factory.plugin_manager.deactivate_all()
        server.close()
        loop.run_until_complete(server.wait_closed())
        pending = asyncio.Task.all_tasks()
        loop.run_until_complete(asyncio.gather(*pending,
                                               return_exceptions=True))
    #     loop.stop()

    loop.close()
    logger.info("Finished.")
