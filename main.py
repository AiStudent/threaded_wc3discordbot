import threading
import time
import discord
import asyncio
import pymysql
import keys
import requests
import hashlib
import queue
from pathlib import Path
from w3gtest.decompress import decompress_replay
from w3gtest.decompress import CouldNotDecompress
from w3gtest.dota_stats import get_dota_w3mmd_stats
from w3gtest.dota_stats import NotCompleteGame
from w3gtest.dota_stats import DotaPlayer
from elo import teams_update_elo


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


def add_new_player_to_db(db_entry):
    try:
        connection = connect_to_db()

        with connection.cursor() as cursor:
            sql = "INSERT INTO playerstats (name, elo, games, wins, \
            loss, kills, deaths, assists, cskills, csdenies, avgkills, \
            avgdeaths, avgassists, avgcskills, avgcsdenies) VALUES \
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql,  (db_entry.name.lower(),
                                  float(db_entry.elo),
                                  db_entry.games,
                                  db_entry.wins,
                                  db_entry.loss,
                                  db_entry.kills,
                                  db_entry.deaths,
                                  db_entry.assists,
                                  db_entry.cskills,
                                  db_entry.csdenies,
                                  db_entry.avgkills,
                                  db_entry.avgdeaths,
                                  db_entry.avgassists,
                                  db_entry.avgcskills,
                                  db_entry.avgcsdenies
                                  ))
            connection.commit()

        connection.close()
    finally:
        pass


def update_dota_player_in_db(db_entry):
    try:
        connection = connect_to_db()
        with connection.cursor() as cursor:
            sql = "UPDATE playerstats SET elo=%s, games=%s, wins=%s, \
            loss=%s, kills=%s, deaths=%s, assists=%s, cskills=%s, csdenies=%s, \
            avgkills=%s, avgdeaths=%s, avgassists=%s, avgcskills=%s, \
            avgcsdenies=%s where name=%s"
            cursor.execute(
                sql,
                (
                    float(db_entry.elo),
                    db_entry.games,
                    db_entry.wins,
                    db_entry.loss,
                    db_entry.kills,
                    db_entry.deaths,
                    db_entry.assists,
                    db_entry.cskills,
                    db_entry.csdenies,
                    db_entry.avgkills,
                    db_entry.avgdeaths,
                    db_entry.avgassists,
                    db_entry.avgcskills,
                    db_entry.avgcsdenies,
                    db_entry.name.lower()))
            connection.commit()
        connection.close()
    finally:
        pass


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
    def __init__(self, func, args, status=None, status_queue=None):
        super(ThreadAnything, self).__init__()

        self.func = func
        self.args = args
        self.rv = None
        self.status = status
        self.status_queue = status_queue
        self.exception = None

    def run(self):
        try:
            args = self.args
            if self.status:
                args += (self.status,)
            if self.status_queue:
                args += (self.status_queue,)
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


# hashing
BLOCKSIZE = 65536


def get_hash(data):
    hasher = hashlib.md5()
    index = 0
    buf = data[index:index+BLOCKSIZE]
    while len(buf) > 0:
        index += BLOCKSIZE
        hasher.update(buf)
        buf = data[index:index+BLOCKSIZE]
    return hasher.hexdigest()


def check_if_replay_exists(md5):
    file_path = Path('replays/'+md5+'.w3g')
    return file_path.is_file()


def save_file(data, md5):
    f = open('replays/'+md5+'.w3g', 'wb')
    f.write(data)
    f.close()


class DBEntry:
    def __init__(self, de):
        if isinstance(de, dict):
            self.name = de['name']
            self.elo = de['elo']
            self.games = de['games']
            self.wins = de['wins']
            self.loss = de['loss']
            self.kills = de['kills']
            self.deaths = de['deaths']
            self.assists = de['assists']
            self.cskills = de['cskills']
            self.csdenies = de['csdenies']
            self.avgkills = de['avgkills']
            self.avgdeaths = de['avgdeaths']
            self.avgassists = de['avgassists']
            self.avgcskills = de['avgcskills']
            self.avgcsdenies = de['avgcsdenies']
        if isinstance(de, DotaPlayer):
            self.name = de.name
            self.elo = 1000.0
            self.games = 0
            self.wins = 0
            self.loss = 0
            self.kills = 0
            self.deaths = 0
            self.assists = 0
            self.cskills = 0
            self.csdenies = 0
            self.avgkills = 0.0
            self.avgdeaths = 0.0
            self.avgassists = 0.0
            self.avgcskills = 0.0
            self.avgcsdenies = 0.0


def decompress_parse_db_replay(data, status_queue: queue.Queue):
    status_queue.put('Attempting to decompress..')
    data = decompress_replay(data)
    dota_players, winner, mins, secs = get_dota_w3mmd_stats(data)

    # check if already uploaded
    stats_bytes = str([dota_player.get_values() for dota_player in dota_players]).encode('utf-8')
    md5 = get_hash(stats_bytes)
    if check_if_replay_exists(md5):
        return "Replay already uploaded"
    else:
        save_file(data, md5)

    team1 = []
    team2 = []
    db_entries = []
    new_db_entries = []
    old_db_entries = []
    for dota_player in dota_players:
        db_entry = get_player_from_db(dota_player.name)
        if db_entry is None:
            db_entry = DBEntry(dota_player)
            new_db_entries += [db_entry]
        else:
            db_entry = DBEntry(db_entry)
            old_db_entries += [db_entry]

        if dota_player.team == 1:
            team1 += [db_entry]
        elif dota_player.team == 2:
            team2 += [db_entry]

        db_entries += [db_entry]

    # determine elo change
    teams_update_elo(team1, team2, winner)

    # add up stats
    for n in range(len(dota_players)):  # same order as before
        dota_player = dota_players[n]
        db_entry = db_entries[n]
        db_entry.games += 1
        if dota_player.team == winner:
            db_entry.wins += 1
        else:
            db_entry.loss += 1
        db_entry.kills += dota_player.kills
        db_entry.deaths += dota_player.deaths
        db_entry.assists += dota_player.assists
        db_entry.cskills += dota_player.cskills
        db_entry.csdenies += dota_player.csdenies
        db_entry.avgkills = db_entry.kills / db_entry.games
        db_entry.avgdeaths = db_entry.deaths / db_entry.games
        db_entry.avgassists = db_entry.assists / db_entry.games
        db_entry.avgcskills = db_entry.cskills / db_entry.games
        db_entry.avgcsdenies = db_entry.csdenies / db_entry.games
        print(db_entry.name, db_entry.cskills, db_entry.avgcskills)

    status_queue.put('Trying to put into db..')

    # for new_db_entries
    for db_entry in new_db_entries:
        add_new_player_to_db(db_entry)

    # for old_db_entries
    for db_entry in old_db_entries:
        update_dota_player_in_db(db_entry)

    # statistics message
    winner = ['Sentinel', 'Scourge'][winner-1]
    msg = "```Winner: " + winner + ', ' + str(mins) + 'm, ' + str(secs) + 's' + '\n'
    for dota_player in dota_players:
        msg += str((dota_player.name, dota_player.kills, dota_player.deaths, dota_player.assists)) + '\n'
    msg += "```"
    return msg


class Client(discord.Client):
    async def on_ready(self):
        print('Logged on as', self.user)

    async def on_message(self, message: discord.message.Message):
        # don't respond to ourselves
        if message.author == self.user:
            return

        print('<: ' + message.content + str([attachment.filename for attachment in message.attachments]))
        words = message.content.split()
        command = None
        payload = None
        if len(words) > 0:
            command = words[0]
        if len(words) > 1:
            payload = words[1:]

        messager = Message(message.channel)
        if command == '!timer' and payload:
            await self.timer_handler(message, payload)
        elif command == '!sd' and payload:
            await self.sd_handler(message, payload)

        for attachment in message.attachments:
            if attachment.filename[-4:] == '.w3g':
                data = requests.get(attachment.url).content
                await self.replay_handler(message, data)
            else:
                await messager.send('Not a wc3 replay.')

    @staticmethod
    async def replay_handler(message: discord.message.Message, data):
        status_queue = queue.Queue()
        t1 = ThreadAnything(decompress_parse_db_replay, (data,), status_queue=status_queue)
        t1.start()

        while t1.is_alive():
            while not status_queue.empty():
                await message.channel.send(status_queue.get_nowait())
            await asyncio.sleep(0.1)

        while not status_queue.empty():  # can maybe make prettier
            await message.channel.send(status_queue.get_nowait())

        if t1.exception:
            try:
                raise t1.exception
            except CouldNotDecompress:
                await message.channel.send('Decompress error.')
            except NotCompleteGame:
                await message.channel.send('Not complete or not a dota game.')
        else:
            await message.channel.send(t1.rv)

    @staticmethod
    async def sd_handler(message: discord.message.Message, payload):
        name = payload[0]

        t1 = ThreadAnything(get_player_from_db, (name,))
        t1.start()

        response = Message(message.channel)

        while t1.is_alive():
            await response.send_status('Accessing db..')
            await asyncio.sleep(0.1)

        if t1.exception:
            raise t1.exception

        if t1.rv:
            db_entry = DBEntry(t1.rv)
            await response.send(
                "Stats for " + name + ': ' + str(round(db_entry.elo, 1)) + ' elo, ' +
                'W/L (' + str(db_entry.wins) + '/' + str(db_entry.loss) + '), avg KDA (' +
                str(db_entry.avgkills) + '/' + str(db_entry.avgdeaths) + '/' + str(db_entry.avgassists) + ')'
            )
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
