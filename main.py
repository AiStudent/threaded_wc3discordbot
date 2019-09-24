import threading
import time
import discord
import asyncio
from datetime import datetime
from mysql import get_player, insert_player, insert_game, update_player, insert_player_game, get_game
from mysql import fetchall, commit, update_game, get_player_id, update_player_game
import keys
import requests
import hashlib
import queue
from w3gtest.decompress import decompress_replay
from w3gtest.decompress import CouldNotDecompress
from w3gtest.dota_stats import get_dota_w3mmd_stats
from w3gtest.dota_stats import NotCompleteGame, NotDotaReplay, parse_incomplete_game
from w3gtest.dota_stats import DotaPlayer
from elo import teams_update_elo, team_win_elos, avg_team_elo
from elo import avg_team_elo_dict, team_win_elos_dict
import fnmatch
import os
import pymysql
from transferer import transfer_db



class Status:
    def __init__(self):
        self.progress = None
        self.request_queue = queue.Queue()

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
        if self.cooldown < time.time() and content is not None:
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
    for file in os.listdir('replays'):
        if fnmatch.fnmatch(file, '*' + md5 + '*.w3g'):
            return True
    return False


def get_replay_name(upload_time):
    for file in os.listdir('replays'):
        if fnmatch.fnmatch(file, upload_time + '*.w3g'):
            return file

def change_filename(upload_time, new_upload_time):
    filename = get_replay_name(upload_time)
    old_file = os.path.join("replays", filename)
    new_filename = new_upload_time + filename[len(upload_time):]
    new_file = os.path.join("replays", new_filename)
    os.rename(old_file, new_file)

def save_file(data, filename):
    f = open('replays/'+filename+'.w3g', 'wb')
    f.write(data)
    f.close()


class DBEntry:
    def __init__(self, de):
        self.dota_player = None
        self.old_elo = None
        if isinstance(de, dict):
            self.player_id = de['player_id']
            self.name = de['name']
            self.elo = de['elo']
            self.games = de['games']
            self.wins = de['wins']
            self.loss = de['loss']
            self.draw = de['draw']
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
            self.player_id = None
            self.name = de.name
            self.elo = 1000.0
            self.games = 0
            self.wins = 0
            self.loss = 0
            self.draw = 0
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

    def get_hm(self):
        hm = {
            'name': self.name,
            'elo': self.elo,
            'games': self.games,
            'wins': self.wins,
            'loss': self.loss,
            'draw': self.draw,
            'kills': self.kills,
            'deaths': self.deaths,
            'assists': self.assists,
            'cskills': self.cskills,
            'csdenies': self.csdenies,
            'avgkills': self.avgkills,
            'avgdeaths': self.avgdeaths,
            'avgassists': self.avgassists,
            'avgcskills': self.avgcskills,
            'avgcsdenies': self.avgcsdenies
        }
        if self.player_id:
            hm['player_id'] = self.player_id
        return hm


def slash_delimited(*args):
    string = '('
    for n in range(len(args)-1):
        string += str(args[n]) + '/'
    string += str(args[-1]) + ')'
    return string


def strwidth(name: str, width, *args):
    name = str(name)
    string = name.ljust(width)
    for n in range(0, len(args)-2, 2):
        string += (str(args[n])+', ').rjust(args[n+1])
    string += str(args[-2])
    return string


def strwidthleft(name: str, width, *args):
    name = str(name)
    string = name.ljust(width)
    for n in range(0, len(args)-2, 2):
        string += (str(args[n])+' ').ljust(args[n+1])
    string += str(args[-2])
    return string


def sd_player(name: str):
    p = get_player(name.lower())
    if p is None:
        return 'No stats on ' + name
    return name + ': ' + str(round(p['elo'], 1)) + ' elo, ' + \
        'W/L ' + slash_delimited(p['wins'], p['loss']) + ', avg KDA ' + \
        slash_delimited(round(p['avgkills'],1), round(p['avgdeaths'],1), round(p['avgassists'],1))


def structure_game_msg(winner, mins, secs, team1_win_elo_inc, team2_win_elo_inc, dota_players, team1_avg_elo, team2_avg_elo):
    msg = "```Winner: " + winner + ', ' + str(mins) + 'm, ' + str(secs) + 's, elo ratio (' +\
          str(round(team1_win_elo_inc, 1)) + '/' + str(round(team2_win_elo_inc, 1)) + ')\n'

    team1_dp = [dota_player for dota_player in dota_players if dota_player.team == 1]
    team2_dp = [dota_player for dota_player in dota_players if dota_player.team == 2]

    msg += 'sentinel avg elo: ' + str(round(team1_avg_elo, 1)) + '\n'
    for dota_player in team1_dp:
        msg += strwidth(dota_player.name, 15, dota_player.kills, 4,
                        dota_player.deaths, 4, dota_player.assists, 4) + '\n'
    msg += 'scourge avg elo: ' + str(round(team2_avg_elo, 1)) + '\n'
    for dota_player in team2_dp:
        msg += strwidth(dota_player.name, 15, dota_player.kills, 4,
                        dota_player.deaths, 4, dota_player.assists, 4) + '\n'
    msg += "```"
    return msg


def add_dp_dbentries(dota_players, db_entries, winner):
    for n in range(len(dota_players)):  # same order as before
        dota_player = dota_players[n]
        db_entry = db_entries[n]
        db_entry.games += 1
        if dota_player.team == winner:
            db_entry.wins += 1
        else:
            db_entry.loss += 1
        db_entry.dota_player = dota_player
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


def decompress_parse_db_replay(replay, status: Status, status_queue: queue.Queue):
    status_queue.put('Attempting to decompress..')
    data = decompress_replay(replay)
    dota_players, winner, mins, secs, mode = get_dota_w3mmd_stats(data)

    # check if already uploaded
    stats_bytes = str([dota_player.get_values() for dota_player in dota_players]).encode('utf-8')
    md5 = get_hash(stats_bytes)
    if check_if_replay_exists(md5):
        game = get_game(md5, 'hash')
        return "Replay already uploaded with Game ID: " + str(game['game_id'])

    team1, team2, db_entries, new_db_entries, old_db_entries = get_teams_and_dbentries(dota_players)

    if len(team1) != len(team2):
        return "Not an equal amount of players on both teams."

    # determine elo change
    team1_win_elo_inc, team2_win_elo_inc = team_win_elos(team1, team2)
    team1_avg_elo, team2_avg_elo = avg_team_elo(team1), avg_team_elo(team2)

    t1_elo_change, t2_elo_change = teams_update_elo(team1, team2, winner)

    # add up stats
    add_dp_dbentries(dota_players, db_entries, winner)

    # structure statistics return message
    winner = ['Sentinel', 'Scourge'][winner-1]

    msg = structure_game_msg(winner, mins, secs, team1_win_elo_inc, team2_win_elo_inc, dota_players, team1_avg_elo, team2_avg_elo)

    msg += "!confirm or !discard"
    status_queue.put(msg)

    while True:
        request = status.request_queue.get()
        if request is 'discard':
            return "Discarded the replay."
        elif request is 'confirm':
            break

    #status_queue.put("Uploading to db..")
    status.progress = "Uploading to local db"

    date_and_time = datetime.now().strftime("%Y%m%d_%Hh%Mm%Ss")
    #yyyymmdd_xxhxxmxxs_complete_winner_mins_secs_hash
    filename = date_and_time + '_complete_' + winner + '_' + str(mins) + '_' + str(secs) + '_' + md5
    save_file(replay, filename)

    try:
        game_entry = insert_game(
            {   'mode': mode,
                'winner': winner,
                'duration': (60*mins+secs),
                'upload_time': date_and_time,
                'hash': md5,
                'ranked': 1,
                'team1_elo': team1_avg_elo,
                'team2_elo': team2_avg_elo,
                'team1_elo_change': t1_elo_change,
                'elo_alg': '1.0'
            }
        )

        game_id = game_entry['LAST_INSERT_ID()']

        put_entries_in_db(game_id, new_db_entries, old_db_entries)

    except pymysql.err.IntegrityError as e:
        return str(e)

    rank_players(status)
    if keys.REMOTE_DB:
        transfer_db(status)
    return "Replay uploaded to db. Game ID: " + str(game_id)


def list_last_games(nr):
    sql = "select game_id, mode, ranked, upload_time from games order by upload_time ASC limit %s"
    rows = fetchall(sql, (nr,))

    msg = "```"
    msg += strwidthleft('game_id', 10, 'ranked', 10, 'upload_time', 20) + '\n'

    for row in rows:
        msg += strwidthleft(row['game_id'], 10, row['ranked'], 10, row['upload_time'], 20) + '\n'
    msg += '```'
    return msg


def rank_game(game_id, status):
    game = get_game(game_id)
    if game is None:
        return 'Game nr' + str(game_id) + ' not found'
    if game['ranked'] == 1:
        return 'Game nr: ' + str(game_id) + ' is already ranked'

    upload_time = game['upload_time']
    rollback_to(game, status=status)

    game['ranked'] = 1
    update_game(game)

    # ignore current game and recalculate for all after
    recalculate_elo_from_game(upload_time, status=status)
    rank_players(status)
    if keys.REMOTE_DB:
        transfer_db(status)
    return "done"


def unrank_game(game_id, status):
    # mysql unrank
    game = get_game(game_id)
    if game is None:
        return 'Game nr' + str(game_id) + ' not found'
    if game['ranked'] != 1:
        return 'Game nr: ' + str(game_id) + ' is not ranked'

    upload_time = game['upload_time']
    rollback_to(game, status)

    game['ranked'] = 0
    update_game(game)

    recalculate_elo_from_game(upload_time, status=status)
    rank_players(status)
    if keys.REMOTE_DB:
        transfer_db(status)
    return "done"


def rollback_to(game, status=None):
    upload_time = game['upload_time']
    sql = "select * from games where upload_time>=%s AND ranked=1 ORDER BY upload_time DESC;"
    games = fetchall(sql, (upload_time))
    n = 0
    for g in games:
        if status:
            status.progress = "Rolling back.. " + slash_delimited(n, len(games))
        reset_stats_of_latest_game(g['game_id'])
        n += 1


def recalculate_elo_from_game(upload_time, status=None):
    sql = "select * from games where upload_time>=%s AND ranked=1 ORDER BY upload_time ASC;"
    games = fetchall(sql, (upload_time))

    n = 0
    for game in games:
        if status:
            status.progress = 'Recalculating elo ' + slash_delimited(n, len(games))
        # check winner
        winner = game['winner']

        # get pgs
        sql = "select * from player_game where game_id=%s"
        pgs = fetchall(sql, (game['game_id']))

        # get total player elo (assume updated)
        team1 = []
        team2 = []

        # add pg stats to player
        for pg in pgs:
            p = get_player_id(pg['player_id'])
            if pg['slot_nr'] < 5:
                team1 += [p]
                if winner == 'Sentinel':
                    p['wins'] += 1
                else:
                    p['loss'] += 1
            else:
                team2 += [p]
                if winner == 'Scourge':
                    p['wins'] += 1
                else:
                    p['loss'] += 1

            pg['elo_before'] = p['elo']
            update_player_game(pg)
            p['games'] += 1
            p['kills'] += pg['kills']
            p['deaths'] += pg['deaths']
            p['assists'] += pg['assists']
            p['cskills'] += pg['cskills']
            p['csdenies'] += pg['csdenies']
            p['avgkills'] = p['kills'] / p['games']
            p['avgdeaths'] = p['deaths'] / p['games']
            p['avgassists'] = p['assists'] / p['games']
            p['avgcskills'] = p['avgcskills'] / p['games']
            p['avgcsdenies'] = p['avgcsdenies'] / p['games']

        # set game teams avg elo
        team1_avg_elo = avg_team_elo_dict(team1)
        team2_avg_elo = avg_team_elo_dict(team2)
        game['team1_elo'] = team1_avg_elo
        game['team2_elo'] = team2_avg_elo

        # elo calculation
        team1_win_elo, team2_win_elo = team_win_elos_dict(team1_avg_elo, team2_avg_elo)

        # set game teams_elo_change
        if winner == 'Sentinel':
            game['team1_elo_change'] = team1_win_elo
            for p in team1:
                p['elo'] += team1_win_elo
            for p in team2:
                p['elo'] -= team1_win_elo
        else:
            game['team1_elo_change'] = -team2_win_elo
            for p in team1:
                p['elo'] -= team2_win_elo
            for p in team2:
                p['elo'] += team2_win_elo


        update_game(game)
        for p in team1+team2:
            update_player(p)

        n += 1

    status.progress = 'Recalculating elo ' + slash_delimited(n, len(games))

def reset_stats_of_latest_game(game_id):
    # get game
    game = get_game(game_id)
    game['team1_elo'] = 0
    game['team2_elo'] = 0
    game['team1_elo_change'] = 0
    update_game(game)
    # undo stuff
    # pg -> p
    sql = "select * from player_game where game_id=%s"
    pgs = fetchall(sql, game_id)
    for pg in pgs:
        p = get_player_id(pg['player_id'])
        if game['winner'] == 'Sentinel':
            if pg['slot_nr'] < 5:
                p['wins'] -= 1
            else:
                p['loss'] -= 1
        else:
            if pg['slot_nr'] >= 5:
                p['wins'] -= 1
            else:
                p['loss'] -= 1

        p['games'] -= 1

        p['elo'] = pg['elo_before']
        p['kills'] -= pg['kills']
        p['deaths'] -= pg['deaths']
        p['assists'] -= pg['assists']
        p['cskills'] -= pg['cskills']
        p['csdenies'] -= pg['csdenies']
        if p['games'] > 0:
            p['avgkills'] = p['kills'] / p['games']
            p['avgdeaths'] = p['deaths'] / p['games']
            p['avgassists'] = p['assists'] / p['games']
            p['avgcskills'] = p['avgcskills'] / p['games']
            p['avgcsdenies'] = p['avgcsdenies'] / p['games']
        else:
            p['avgkills'] = 0
            p['avgdeaths'] = 0
            p['avgassists'] = 0
            p['avgcskills'] = 0
            p['avgcsdenies'] = 0
        update_player(p)


def cleard_db():
    sql = "delete from games"
    commit(sql, ())
    sql = "ALTER TABLE games AUTO_INCREMENT = 1"
    commit(sql, ())
    sql = "delete from player"
    commit(sql, ())
    sql = "ALTER TABLE player AUTO_INCREMENT = 1"
    commit(sql, ())
    sql = "delete from player_game"
    commit(sql, ())


def put_entries_in_db(game_id, new_db_entries, old_db_entries):
    # for new_db_entries
    for db_entry in new_db_entries:
        response = insert_player(db_entry.get_hm())
        db_entry.player_id = response['LAST_INSERT_ID()']

    for db_entry in new_db_entries + old_db_entries:
        insert_player_game({
            'player_id': db_entry.player_id,
            'game_id': game_id,
            'slot_nr': db_entry.dota_player.slot_order,  # until blizzard fixes slot_nr
            'elo_before': db_entry.old_elo,
            'kills': db_entry.dota_player.kills,
            'deaths': db_entry.dota_player.deaths,
            'assists': db_entry.dota_player.assists,
            'cskills': db_entry.dota_player.cskills,
            'csdenies': db_entry.dota_player.csdenies
        })

        # for old_db_entries
        for db_entry in old_db_entries:
            update_player(db_entry.get_hm(), 'player_id')


def manual_input_replay(replay, status: Status, status_queue: queue.Queue):
    # parse
    data = decompress_replay(replay)
    dota_players, mode, unparsed = parse_incomplete_game(data)

    # check if already uploaded
    stats_bytes = str([dota_player.get_values() for dota_player in dota_players]).encode('utf-8')
    md5 = get_hash(stats_bytes)
    if check_if_replay_exists(md5):
        game = get_game(md5, 'hash')
        return "Replay already uploaded with Game ID: " + str(game['game_id'])

    team1, team2, db_entries, new_db_entries, old_db_entries = get_teams_and_dbentries(dota_players)

    if len(team1) != len(team2):
        return "Not an equal amount of players on both teams."

    # determine elo change
    team1_win_elo_inc, team2_win_elo_inc = team_win_elos(team1, team2)
    team1_avg_elo, team2_avg_elo = avg_team_elo(team1), avg_team_elo(team2)

    msg = structure_game_msg('?', '?', '?', team1_win_elo_inc, team2_win_elo_inc, dota_players, team1_avg_elo, team2_avg_elo)

    #send preliminary message
    msg += "!discard or !manual winner mins secs"
    status_queue.put(msg)

    while True:
        request = status.request_queue.get()

        if request is 'discard':
            return "Discarded the replay."
        elif request[0] is 'manual':
            winner = request[1]
            if winner.lower() == "sentinel":
                winner = 1
            elif winner.lower() == 'scourge':
                winner = 2
            else:
                status_queue.put("winner: sentinel or scourge")
                continue
            try:
                mins = int(request[2])
                secs = int(request[3])
                break
            except ValueError:
                status_queue.put("like: !manual sentinel 34 3")

    t1_elo_change, t2_elo_change = teams_update_elo(team1, team2, winner)

    # add up stats
    add_dp_dbentries(dota_players, db_entries, winner)

    # structure statistics return message
    winner = ['Sentinel', 'Scourge'][winner - 1]

    msg = structure_game_msg(winner, mins, secs, team1_win_elo_inc, team2_win_elo_inc, dota_players, team1_avg_elo, team2_avg_elo)

    msg += "!confirm or !discard"
    status_queue.put(msg)

    while True:
        request = status.request_queue.get()
        if request is 'discard':
            return "Discarded the replay."
        elif request is 'confirm':
            break

    status_queue.put("Uploading to db..")

    date_and_time = datetime.now().strftime("%Y%m%d_%Hh%Mm%Ss")
    # yyyymmdd_xxhxxmxxs_incomplete_winner_mins_secs_hash.w3g
    filename = date_and_time + '_incomplete_' + winner + '_' + str(mins) + '_' + str(secs) + '_' + md5
    save_file(replay, filename)

    try:
        game_entry = insert_game(
            {'mode': mode,
             'winner': winner,
             'duration': (60 * mins + secs),
             'upload_time': date_and_time,
             'hash': md5,
             'ranked': 1,
             'team1_elo': team1_avg_elo,
             'team2_elo': team2_avg_elo,
             'team1_elo_change': t1_elo_change,
             'elo_alg': '1.0'
             }
        )

        game_id = game_entry['LAST_INSERT_ID()']

        put_entries_in_db(game_id, new_db_entries, old_db_entries)

    except pymysql.err.IntegrityError as e:
        return str(e)

    rank_players(status)
    if keys.REMOTE_DB:
        transfer_db(status)
    return "Replay uploaded to db. Game ID: " + str(game_id)


def get_teams_and_dbentries(dota_players):
    team1 = []
    team2 = []
    db_entries = []
    new_db_entries = []
    old_db_entries = []
    for dota_player in dota_players:
        dota_player.name = dota_player.name.lower()
        db_entry = get_player(dota_player.name)
        if db_entry is None:
            db_entry = DBEntry(dota_player)
            new_db_entries += [db_entry]
        else:
            db_entry = DBEntry(db_entry)
            old_db_entries += [db_entry]

        db_entry.old_elo = db_entry.elo

        if dota_player.team == 1:
            team1 += [db_entry]
        elif dota_player.team == 2:
            team2 += [db_entry]

        db_entries += [db_entry]
    return team1, team2, db_entries, new_db_entries, old_db_entries

def auto_replay_upload(replay, date_and_time=None, winner=None, mins=None, secs=None):
    # parse
    data = decompress_replay(replay)

    try:
        dota_players, winner, mins, secs, mode = get_dota_w3mmd_stats(data)
    except NotCompleteGame:
        dota_players, mode, unparsed = parse_incomplete_game(data)
        if winner is None:
            raise Exception('auto_replay_upload incomplete replay with no given arguments')

    stats_bytes = str([dota_player.get_values() for dota_player in dota_players]).encode('utf-8')
    md5 = get_hash(stats_bytes)

    team1, team2, db_entries, new_db_entries, old_db_entries = get_teams_and_dbentries(dota_players)

    if len(team1) != len(team2) != 5:
        raise Exception('Not 5x5 game')

    # determine elo change
    team1_avg_elo, team2_avg_elo = avg_team_elo(team1), avg_team_elo(team2)

    t1_elo_change, t2_elo_change = teams_update_elo(team1, team2, winner)

    # add up stats
    add_dp_dbentries(dota_players, db_entries, winner)

    # structure statistics return message
    winner = ['Sentinel', 'Scourge'][winner - 1]

    try:
        game_entry = insert_game(
            {'mode': mode,
             'winner': winner,
             'duration': (60 * mins + secs),
             'upload_time': date_and_time,
             'hash': md5,
             'ranked': 1,
             'team1_elo': team1_avg_elo,
             'team2_elo': team2_avg_elo,
             'team1_elo_change': t1_elo_change,
             'elo_alg': '1.0'
             }
        )

        game_id = game_entry['LAST_INSERT_ID()']

        put_entries_in_db(game_id, new_db_entries, old_db_entries)

    except pymysql.err.IntegrityError as e:
        return str(e)

    return "Replay uploaded to db. Game ID: " + str(game_id)


def modify_game_upload_time(game_id, new_upload_time):
    game = get_game(game_id)
    if game['ranked'] == 1:
        return "Unrank the game before modifying upload time."
    old_upload_time = game['upload_time']
    game['upload_time'] = new_upload_time
    update_game(game)
    change_filename(old_upload_time, new_upload_time)

    return "Game ID: " + str(game_id) + " upload time set to " + new_upload_time


def rank_players(status):
    status.progress = "Ranking players.."
    sql = "select * from player ORDER BY elo DESC"
    players = fetchall(sql, ())

    for n in range(len(players)):
        player = players[n]
        player['rank'] = n+1
        update_player(player)



def show_game(game_id):
    game = get_game(game_id)
    if game is None:
        return None

    msg = "```"
    msg += "Winner: " + game['winner'] + ", "
    duration = game['duration']
    mins = duration // 60
    secs = duration - 60*mins
    msg += str(mins) + 'm, ' + str(secs) + "s\n"

    msg += "sentinel elo: " + str(round(game['team1_elo'],1)) + ", change: " \
           + str(round(game['team1_elo_change'],1)) + '\n'

    sql = "select * from player_game where game_id=%s order by slot_nr ASC"
    player_games = fetchall(sql, (game_id))
    for pg in player_games[:5]:
        name = get_player_id(pg['player_id'])['name']
        msg += strwidth(name, 15, pg['kills'], 4,
                        pg['deaths'], 4, pg['assists'], 4) + '\n'

    msg += "scourge elo: " + str(round(game['team2_elo'],1)) + ", change: " \
           + str(round(-game['team1_elo_change'],1)) + '\n'

    for pg in player_games[5:]:
        name = get_player_id(pg['player_id'])['name']
        msg += strwidth(name, 15, pg['kills'], 4,
                        pg['deaths'], 4, pg['assists'], 4) + '\n'
    msg += "```"
    return msg


def reupload_all_replays(status:Status, status_queue: queue.Queue):
    cleard_db()
    status_queue.put("cleared db")
    n = 0
    files = sorted(os.listdir('replays'))
    for file in files:
        status.progress = 'Uploading ' + slash_delimited(n+1, len(files))
        f = open('replays/' + file, 'rb')
        data = f.read()
        f.close()

        parts = file.split('_')
        date_and_time = parts[0] + '_' + parts[1] #date, time
        if parts[3] == 'Sentinel':
            winner = 1
        elif parts[3] == 'Scourge':
            winner = 2
        else:
            return "Stopped at a bad filename: " + file + " at " + parts[3]

        mins = int(parts[4])
        secs = int(parts[5])
        auto_replay_upload(data, date_and_time, winner, mins, secs)
        n+=1

    rank_players(status)
    if keys.REMOTE_DB:
        transfer_db(status)
    return str(n) + ' replays uploaded.'


def delete_replay(game_id):
    game = get_game(game_id, 'game_id')
    if game is None:
        return "Game not found in db."
    if game['ranked'] == 1:
        return 'Game is not unranked.'

    game_id = game['game_id']
    for file in os.listdir('replays'):
        if fnmatch.fnmatch(file, '*' + game['hash'] + '*.w3g'):
            filepath = os.path.join("replays", file)
            os.remove(filepath)

            #remove game
            sql = "delete from games where game_id=%s"
            commit(sql, (game_id))
            #remove pgs
            sql = "delete from player_game where game_id=%s"
            commit(sql, (game_id))
            #if the players only have 0 games, remove them aswell?
            #can cause trouble if someone unranks 1 of 2 games, and then deletes the last, and then reranks
            return "Game " + str(game_id) + " is no more."

    return "Replay file on disk not found."




class Client(discord.Client):
    player_queue = []
    current_replay_upload = []
    lock = False

    async def on_ready(self):
        print('Logged on as', self.user)

    async def on_resumed(self):
        print("resumed..")

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

        author = message.author
        roles = [role.name for role in author.roles]
        admin = ('Admin' in roles) or ('Development' in roles)
        messager = Message(message.channel)
        if command == '!sd':
            await self.sd_handler(message, payload)
        elif command == '!confirm':
            await self.confirm_replay_handler(message)
        elif command == '!discard':
            await self.discard_replay_handler(message)
        elif command == '!force_discard' and admin:
            await self.force_discard_replay_handler(message)
        elif command == '!manual' and payload:
            await self.manual_replay_handler(message, payload)
        elif command == '!list':
            await self.list_last_games_handler(message, payload)
        elif command == '!cleardb' and admin:
            await self.clear_db_handler(message)
        elif command == '!rank' and payload and admin:
            await self.rank_handler(message, payload)
        elif command == '!unrank' and payload and admin:
            await self.unrank_handler(message, payload)
        elif command == '!setdate' and payload and admin:
            await self.modify_game_upload_time_handler(message, payload)
        elif command == '!show' and payload:
            await self.show_game_handler(message, payload)
        elif command == '!reupload_all_replays' and admin:
            await self.reupload_all_replays_handler(message)
        elif command == '!delete' and payload and admin:
            await self.delete_replay_handler(message, payload)
        elif command == '!shutdown' and admin:
            await self.shutdown_handler(message)
        elif command == '!force_shutdown' and admin:
            await self.force_shutdown_handler(message)
        elif command == '!help':
            await self.help_handler(message)
        for attachment in message.attachments:
            if attachment.filename[-4:] == '.w3g':
                data = requests.get(attachment.url).content
                await self.replay_handler(message, data)
            else:
                await messager.send('Not a wc3 replay.')

    @staticmethod
    async def help_handler(message):
        commands = ['!sd name',
                    '!list nr_of_last_games',
                    'admin commands:',
                    '!unrank game_id',
                    '!rank game_id',
                    '!setdate game_id yyyymmdd_xxhxxmxxs',
                    '!reupload_all_replays (in order of upload time)',
                    '!delete game_id (deletes an unranked game)']
        msg = '```'
        for command in commands:
            msg += command + '\n'
        msg += '```'
        await message.channel.send(msg)

    @staticmethod
    async def clear_db_handler(message):
        if Client.lock:
            await message.channel.send("db is currently locked")
            return
        Client.lock = True
        cleard_db()
        Client.lock = False
        await message.channel.send("cleared db")

    @staticmethod
    async def show_game_handler(message: discord.message.Message, payload):
        game_id = payload[0]

        t1 = ThreadAnything(show_game, (game_id,))
        t1.start()

        response = Message(message.channel)

        while t1.is_alive():
            await asyncio.sleep(0.1)

        if t1.exception:
            raise t1.exception

        if t1.rv:
            await response.send(str(t1.rv))
        else:
            await response.send('No game found')



    @staticmethod
    async def replay_handler(message: discord.message.Message, data):
        if Client.current_replay_upload:
            author = Client.current_replay_upload[0]
            await message.channel.send('{0.mention} !confirm or !discard previous replay'.format(author))
            return
        elif Client.lock:
            await message.channel.send("db is currently locked")
            return
        Client.lock = True

        status_queue = queue.Queue()
        status = Status()
        t1 = ThreadAnything(decompress_parse_db_replay, (data,), status=status, status_queue=status_queue)
        Client.current_replay_upload = (message.author, t1, status)
        t1.start()

        while t1.is_alive():
            while not status_queue.empty():
                await message.channel.send(status_queue.get_nowait())
            await asyncio.sleep(0.1)

        while not status_queue.empty():  # can maybe make prettier
            await message.channel.send(status_queue.get_nowait())

        if t1.exception:
            Client.lock = False
            try:
                raise t1.exception
            except CouldNotDecompress:
                await message.channel.send('Decompress error.')
            except NotCompleteGame:
                await message.channel.send('Incomplete game.')
                await Client.manual_input_replay_handler(message, data)
            except NotDotaReplay:
                await message.channel.send('Not a dota replay.')
        else:
            await message.channel.send(t1.rv)

        Client.lock = False
        Client.current_replay_upload = []

    @staticmethod
    async def manual_input_replay_handler(message: discord.message.Message, data):
        status_queue = queue.Queue()
        status = Status()
        t1 = ThreadAnything(manual_input_replay, (data,), status=status, status_queue=status_queue)
        Client.current_replay_upload = (message.author, t1, status)
        t1.start()

        while t1.is_alive():
            while not status_queue.empty():
                await message.channel.send(status_queue.get_nowait())
            await asyncio.sleep(0.1)

        while not status_queue.empty():
            await message.channel.send(status_queue.get_nowait())

        if t1.exception:
            Client.lock = False
            Client.current_replay_upload = []
            raise t1.exception
        else:
            await message.channel.send(t1.rv)

        Client.lock = False
        Client.current_replay_upload = []

    @staticmethod
    async def confirm_replay_handler(message: discord.message.Message):
        if message.author in Client.current_replay_upload:
            _, _, status = Client.current_replay_upload
            status.request_queue.put('confirm')

    @staticmethod
    async def discard_replay_handler(message: discord.message.Message):
        if message.author in Client.current_replay_upload:
            _, _, status = Client.current_replay_upload
            status.request_queue.put('discard')

    @staticmethod
    async def force_discard_replay_handler(message: discord.message.Message):
        if Client.current_replay_upload:
            _, _, status = Client.current_replay_upload
            status.request_queue.put('discard')
        else:
            await message.channel.send("No replay to discard")

    @staticmethod
    async def manual_replay_handler(message: discord.message.Message, payload=None):
        if message.author in Client.current_replay_upload:
            if len(payload) != 3:
                await message.channel.send("!manual needs 3 arguments")
                return
            _, _, status = Client.current_replay_upload
            status.request_queue.put(('manual', payload[0], payload[1], payload[2]))
    @staticmethod
    async def sd_handler(message: discord.message.Message, payload=None):
        if payload is None:
            name = str(message.author.display_name)
        else:
            name = payload[0]

        t1 = ThreadAnything(sd_player, (name,))
        t1.start()

        response = Message(message.channel)

        while t1.is_alive():
            await asyncio.sleep(0.1)

        if t1.exception:
            raise t1.exception

        if t1.rv:
            await response.send(str(t1.rv))


    @staticmethod
    async def list_last_games_handler(message: discord.message.Message, payload=None):
        response = Message(message.channel)
        if payload:
            nr = int(payload[0])
        else:
            nr = 10

        t1 = ThreadAnything(list_last_games, (nr,))
        t1.start()

        while t1.is_alive():
            await asyncio.sleep(0.1)

        await response.send(t1.rv)


    @staticmethod
    async def rank_handler(message: discord.message.Message, payload):
        if Client.lock:
            await message.channel.send("db is currently locked")
            return
        Client.lock = True

        response = Message(message.channel)
        nr = int(payload[0])

        status = Status()
        t1 = ThreadAnything(rank_game, (nr,), status=status)
        t1.start()

        while t1.is_alive():
            await response.send_status(status.progress)
            await asyncio.sleep(0.1)

        if t1.exception:
            Client.lock = False
            raise t1.exception

        Client.lock = False
        await response.send(t1.rv)


    @staticmethod
    async def unrank_handler(message: discord.message.Message, payload):
        if Client.lock:
            await message.channel.send("db is currently locked")
            return
        Client.lock = True

        response = Message(message.channel)
        nr = int(payload[0])

        status = Status()
        t1 = ThreadAnything(unrank_game, (nr,), status=status)
        t1.start()

        while t1.is_alive():
            await response.send_status(status.progress)
            await asyncio.sleep(0.1)

        if t1.exception:
            Client.lock = False
            raise t1.exception

        Client.lock = False
        await response.send(t1.rv)

    @staticmethod
    async def modify_game_upload_time_handler(message: discord.message.Message, payload):
        if Client.lock:
            await message.channel.send("db is currently locked")
            return
        Client.lock = True
        game_id = int(payload[0])
        upload_time = payload[1]
        t1 = ThreadAnything(modify_game_upload_time, (game_id, upload_time))
        t1.start()

        while t1.is_alive():
            await asyncio.sleep(0.1)

        if t1.exception:
            message.channel.send("modify_game_upload_time exception")
            raise t1.exception
        else:
            await message.channel.send(t1.rv)
        Client.lock = False

    @staticmethod
    async def delete_replay_handler(message: discord.message.Message, payload):
        if Client.lock:
            await message.channel.send("db is currently locked")
            return
        Client.lock = True
        game_id = int(payload[0])
        t1 = ThreadAnything(delete_replay, (game_id,))
        t1.start()

        while t1.is_alive():
            await asyncio.sleep(0.1)

        if t1.exception:
            message.channel.send("delete_replay exception")
            raise t1.exception
        else:
            await message.channel.send(t1.rv)
        Client.lock = False

    @staticmethod
    async def reupload_all_replays_handler(message: discord.message.Message):
        if Client.lock:
            await message.channel.send("db is currently locked")
            return
        Client.lock = True

        response = Message(message.channel)

        status = Status()
        status_queue = queue.Queue()
        t1 = ThreadAnything(reupload_all_replays, (), status=status, status_queue=status_queue)
        t1.start()

        while t1.is_alive():
            while not status_queue.empty():
                await message.channel.send(status_queue.get_nowait())
            await response.send_status(status.progress)
            await asyncio.sleep(0.1)

        while not status_queue.empty():  # can maybe make prettier
            await message.channel.send(status_queue.get_nowait())

        if t1.exception:
            Client.lock = False
            raise t1.exception

        Client.lock = False
        await response.send(t1.rv)

    @staticmethod
    async def shutdown_handler(message: discord.message.Message):
        if Client.lock:
            await message.channel.send("db is currently busy")
            return
        print("shutdown..")
        await message.channel.send("shutdown..")
        quit()

    @staticmethod
    async def force_shutdown_handler(message: discord.message.Message):
        print("shutdown..")
        await message.channel.send("shutdown..")
        quit()


client = Client()
client.run(keys.TOKEN)
