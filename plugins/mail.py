"""
StarryPy Mail Plugin

Provides a mail system that allows users to send messages to players that
are not logged in. When the recipient next logs in, they will be notified
that they have new messages.

Author: medeor413
"""

import asyncio
import datetime

import sqlalchemy as sqla

from plugin_manager import SimpleCommandPlugin
from plugins.storage_manager import (DeclarativeBase, SessionAccessMixin,
                                     db_session)
from utilities import Command, send_message


###

class Mail(DeclarativeBase):
    __tablename__ = "mail"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True)
    to = sqla.Column(sqla.String(32))
    message = sqla.Column(sqla.Text)
    time = sqla.Column(sqla.DateTime)
    author = sqla.Column(sqla.String(32))
    unread = sqla.Column(sqla.Boolean)

    def __init__(self, message, author, to):
        self.message = message
        self.time = datetime.datetime.now()
        self.author = author
        self.to = to
        self.unread = True

    def __repr__(self):
        return "<Mail(author={}, time={}, to={}, message={}, unread={})>"\
            .format(self.author, self.time, self.to, self.message, self.unread)


class MailPlugin(SessionAccessMixin, SimpleCommandPlugin):
    name = "mail"
    depends = ["player_manager", "command_dispatcher"]
    default_config = {"max_mail_storage": 25}

    def __init__(self):
        super().__init__()
        self.max_mail = 0
        self.get_player = None
        self.get_connection = None

    def activate(self):
        super().activate()
        self.max_mail = self.plugin_config.max_mail_storage
        self.get_player = self.plugins.player_manager.get_player_by_uuid
        self.get_connection = self.plugins.player_manager.get_connection

    def on_connect_success(self, data, connection):
        """
        Catch when a player successfully connects to the server, and send them
        a new mail message.

        :param data:
        :param connection:
        :return: True. Must always be true so the packet continues.
        """

        asyncio.ensure_future(self._display_unread(connection))
        return True

    def _check_capacity(self, connection, mailbox):
        if not isinstance(mailbox, int):
            mailbox = len(mailbox)

        if mailbox >= int(self.max_mail):
            yield from send_message(connection,
                                    "^red;Your mailbox is full!^reset;")
        elif mailbox >= int(self.max_mail * 0.8):
            yield from send_message(connection,
                                    "^orange;Your mailbox is almost full!")

    @asyncio.coroutine
    def _display_unread(self, connection):
        yield from asyncio.sleep(4)
        with db_session(self.session) as session:
            unread = session.query(Mail).filter_by(
                to=connection.player.uuid, unread=True).count()
            total = session.query(Mail).filter_by(
                to=connection.player.uuid).count()
        if unread > 0:
            yield from send_message(connection, "You have {} unread messages."
                                    .format(unread))
        yield from self._check_capacity(connection, total)

    def send_mail(self, target, author, message):
        """
        A convenience method for sending mail so other plugins can use the
        mail system easily.

        :param target: Player: The recipient of the message.
        :param author: Player: The author of the message.
        :param message: String: The message to be sent.
        :return: None.
        """

        raise NotImplementedError("haven't fixed this yet")
        # mail = Mail(message, author, target)
        # self.storage['mail'][target.uuid].insert(0, mail)

    @Command("sendmail",
             perm="mail.sendmail",
             doc="Send mail to a player, to be read later.",
             syntax="(user) (message)")
    def _sendmail(self, data, connection):
        if data:
            player = self.plugins.player_manager.find_player(data[0])
            if not player:
                raise SyntaxWarning("Couldn't find that player.")
            if not data[1]:
                raise SyntaxWarning("No message provided.")
            with db_session(self.session) as session:
                count = session.query(Mail).filter_by(to=player.uuid).count()
                if count >= self.max_mail:
                    yield from send_message(connection, "{}'s mailbox is full!"
                                            .format(player.alias))
                    return

                mail = Mail(" ".join(data[1:]), connection.player.uuid,
                            player.uuid)
                session.add(mail)
                session.commit()
                yield from send_message(connection, "Mail delivered to {}."
                                        .format(player.alias))
                if player.logged_in:
                    target = self.get_connection(connection, player)
                    yield from send_message(target,
                                            "New mail from {}!".format(
                                                connection.player.alias))
        else:
            raise SyntaxWarning("No target provided.")

    @Command("readmail",
             perm="mail.readmail",
             doc="Read mail recieved from players. Give a number for a "
                 "specific mail, or no number for all unread mails.",
             syntax="[index]")
    def _readmail(self, data=None, connection=None):
        with db_session(self.session) as session:
            all_mail = session.query(Mail).filter_by(
                to=connection.player.uuid).all()
            if not data:
                if not all_mail:
                    yield from send_message(connection,
                                            "No unread mail to display.")
                    return
                mailbox = [msg for msg in enumerate(all_mail, start=1)]
                for idx, mail in mailbox:
                    author = self.get_player(mail.author)
                    yield from send_message(connection, "{}. From {} on {}:\n{}"
                                            .format(idx,
                                                    author.alias,
                                                    mail.time
                                                    .strftime("%d %b %H:%M"),
                                                    mail.message))
                    mail.unread = False
                session.commit()
                return
            else:
                try:
                    idx = int(data[0]) - 1
                    mail = all_mail[idx]
                    author = self.get_player(mail.author)
                    yield from send_message(connection, "{}. From {} on {}:\n{}"
                                            .format(data[0],
                                                    author.alias,
                                                    mail.time
                                                    .strftime("%d %b %H:%M"),
                                                    mail.message))
                    mail.unread = False
                    session.commit()
                except ValueError:
                    yield from send_message(connection,
                                            "Specify a valid number.")
                except IndexError:
                    yield from send_message(connection,
                                            "No mail with that number.")

    @Command("listmail",
             perm="mail.readmail",
             doc="List all mail, optionally in a specified category.",
             syntax="[category]")
    def _listmail(self, data=None, connection=None):
        with db_session(self.session) as session:
            all_mail = session.query(Mail).filter_by(
                to=connection.player.uuid).all()
            if not data:
                if not all_mail:
                    yield from send_message(connection, "No mail in mailbox.")
                    return
                mailbox = [msg for msg in enumerate(all_mail, start=1)]
                for idx, mail in mailbox:
                    author = self.get_player(mail.author)
                    msg = "{}: From {} on {}".format(idx,
                                                     author.alias,
                                                     mail.time.strftime(
                                                         "%d %b %H:%M"))
                    if mail.unread:
                        msg = "* {}".format(msg)
                    yield from send_message(connection, msg)
            elif data[0].lower() == "unread":
                if not all_mail:
                    yield from send_message(connection,
                                            "No unread mail in mailbox.")
                    return
                mailbox = [msg for msg in enumerate(all_mail, start=1)
                           if msg[1].unread]
                for idx, mail in mailbox:
                    author = self.get_player(mail.author)
                    yield from send_message(connection,
                                            "* {}: From {} on {}"
                                            .format(idx,
                                                    author.alias,
                                                    mail.time.strftime(
                                                        "%d %b ""%H:%M")))
            elif data[0].lower() == "read":
                if not all_mail:
                    yield from send_message(connection,
                                            "No read mail in mailbox.")
                    return
                mailbox = [msg for msg in enumerate(all_mail, start=1)
                           if not msg[1].unread]
                for idx, mail in mailbox:
                    author = self.get_player(mail.author)
                    yield from send_message(connection,
                                            "{}: From {} on {}"
                                            .format(idx,
                                                    author.alias,
                                                    mail.time.strftime(
                                                        "%d %b %H:%M")))
            else:
                raise SyntaxWarning("Invalid category. Valid categories are "
                                    "\"read\" and \"unread\".")

            yield from self._check_capacity(connection, all_mail)

    @Command("delmail",
             perm="mail.readmail",
             doc="Delete unwanted mail, by index or category.",
             syntax="(index or category)")
    def _delmail(self, data, connection):
        with db_session(self.session) as session:
            all_mail = session.query(Mail).filter_by(
                to=connection.player.uuid).all()
            if data:
                if data[0] == "all":
                    for mail in all_mail:
                        session.delete(mail)
                    yield from send_message(connection,
                                            "Deleted all mail.")
                elif data[0] == "unread":
                    for mail in all_mail:
                        if mail.unread:
                            session.delete(mail)
                    yield from send_message(connection,
                                            "Deleted all unread mail.")
                elif data[0] == "read":
                    for mail in all_mail:
                        if not mail.unread:
                            session.delete(mail)
                    yield from send_message(connection,
                                            "Deleted all read mail.")
                else:
                    try:
                        idx = int(data[0]) - 1
                        mail = all_mail[idx]
                        print(mail)
                        session.delete(mail)
                        # raise NotImplementedError("haven't fixed this yet")
                        yield from send_message(connection,
                                                "Deleted message # {}."
                                                .format(data[0]))
                    except ValueError:
                        raise SyntaxWarning("Argument must be a category or "
                                            "number. Valid categories: "
                                            "\"read\", \"unread\", \"all\"")
                    except IndexError:
                        yield from send_message(connection,
                                                "No message at that index.")
                session.commit()
            else:
                raise SyntaxWarning("No argument provided.")
