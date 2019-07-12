import threading
import time
import discord
import asyncio
import pymysql
import keys
import traceback
import sys


# ---------------------------------------------
# db init
def connect_to_db():
    connection = pymysql.connect(
        host=keys.dbhost,
        user=keys.dbuser,
        password=keys.dbpass,
        db=keys.db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    return connection


def get_player_from_db(name):
    try:
        connection = connect_to_db()
        with connection.cursor() as cursor:
            sql = "SELECT * FROM playerstats WHERE name = %s"
            cursor.execute(sql, (name.lower(),))
            result = cursor.fetchone()
        connection.close()
    finally:
        pass
    return result


# A function in another file
class TimerException(Exception):
    pass


def timer(t, status=None):
    while t > 0:
        t -= 1
        status.progress = t
        time.sleep(3)
        raise TimerException
    return 42


class Status:
    def __init__(self):
        self.progress = None


class ThreadAnything(threading.Thread):
    def __init__(self, func, args, status=None):
        super(ThreadAnything, self).__init__()

        self.func = func
        self.args = args
        self.rv = None
        self.status = status
        self.exception = None

    def run(self):
        try:
            args = self.args
            if self.status:
                args += (self.status,)
            self.rv = self.func(*args)
        except Exception as exc:
            self.exception = exc


class Message:
    def __init__(self, channel):
        self.last_msg = None
        self.channel = channel
        self.cooldown = 0

    async def send(self, content):
        if self.last_msg is None:
            self.last_msg = await self.channel.send(str(content))
        elif self.last_msg.content == str(content):
            pass
        else:
            try:
                await self.last_msg.edit(content=str(content))
            except discord.errors.NotFound:
                self.last_msg = await self.channel.send(str(content))

        print('>: ' + str(content))

    async def send_status(self, content):
        if self.cooldown < time.time():
            await self.send(content)
            self.cooldown = time.time() + 1


class Client(discord.Client):
    async def on_ready(self):
        print('Logged on as', self.user)

    async def on_message(self, message: discord.message.Message):
        # don't respond to ourselves
        if message.author == self.user:
            return

        print('<: ' + message.content)
        words = message.content.split()
        command = words[0]
        payload = words[1:]
        if command == '!timer' and payload:
            await self.timer_handler(message, payload)
        elif command == '!sd' and payload:
            await self.sd_handler(message, payload)

    @staticmethod
    async def sd_handler(message: discord.message.Message, payload):
        name = payload[0]

        t1 = ThreadAnything(get_player_from_db, (name,))
        t1.start()

        response = Message(message.channel)

        while t1.is_alive():
            await response.send_status('Accessing db..')
            await asyncio.sleep(0.1)

        if t1.rv:
            await response.send(t1.rv)
        else:
            await response.send('No stats on ' + name)

    @staticmethod
    async def timer_handler(message: discord.message.Message, payload):
        t = int(payload[0])

        status = Status()
        t1 = ThreadAnything(timer, (t,), status)
        t1.start()

        response = Message(message.channel)

        while t1.is_alive():
            await response.send_status(status.progress)
            await asyncio.sleep(0.1)

        if t1.exception:
            try:
                raise t1.exception
            except TimerException:
                await response.send('Timer broke.')
        else:
            await response.send(t1.rv)


client = Client()
client.run(keys.TOKEN)
