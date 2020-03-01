from thread_anything import *
import time
import discord
import asyncio
from basic_functions import *
from mysql import get_player, insert_player, insert_game, update_player,\
    insert_player_game, get_game, get_player_bnet, get_player_discord_id
from mysql import fetchall, fetchone, commit, update_game, get_player_id, update_player_game
import keys
import requests
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
import io


class UnregisteredPlayers(Exception):
    def __init__(self, msg):
        self.msg = msg


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


def change_replayfile_upload_time(upload_time, new_upload_time):
    filename = search_replay_on_disk(upload_time)
    old_file = os.path.join("replays", filename)
    new_filename = new_upload_time + filename[len(upload_time):]
    new_file = os.path.join("replays", new_filename)
    os.rename(old_file, new_file)


class DBEntry:
    def __init__(self, de):
        self.dota_player = None
        self.old_elo = None
        if isinstance(de, dict):
            self.player_id = de['player_id']
            self.bnet_tag = de['bnet_tag']
            self.dislay_name = de['name']
            self.discord_id = de['discord_id']
            self.elo = de['elo']
            self.games = de['games']
            self.kdagames = de['kdagames']
            self.csgames = de['csgames']            
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
        if isinstance(de, DotaPlayer) or isinstance(de, str):
            self.player_id = None
            if isinstance(de, DotaPlayer):
                self.bnet_tag = de.name
            else:
                self.bnet_tag = de
            self.elo = 1000.0
            self.games = 0
            self.kdagames = 0
            self.csgames = 0            
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
            'bnet_tag': self.bnet_tag,
            'elo': self.elo,
            'games': self.games,
            'kdagames': self.kdagames,
            'csgames': self.csgames,
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





def sd_player(name: str):
    p = get_player(name.lower())
    if p is None:
        p = get_player_bnet(name.lower())
    if p is None:
        msg = 'No stats on ' + name
    elif p['games'] == 0:
        msg = name + ' has not played any games yet.'
    else:
        msg = name + ': ' + str(round(p['elo'], 1)) + ' elo, ' + \
        'W/L ' + slash_delimited(p['wins'], p['loss']) + ', avg KDA ' + \
        slash_delimited(round(p['avgkills'], 1), round(p['avgdeaths'], 1), round(p['avgassists'], 1))

    return emb(msg)

def sd_players(name: str, name2: str):
    sql = '''select
        g.game_id, g.winner, apg.slot_nr as a_slot_nr, bpg.slot_nr as b_slot_nr
        from
        games g, player_game apg, player_game bpg, player a, player b
        where
        a.name = %s and
        b.name = %s and
        a.player_id = apg.player_id and
        b.player_id = bpg.player_id and
        apg.game_id = bpg.game_id and
        g.game_id = apg.game_id'''
    rows = fetchall(sql, (name, name2))

    same_team_wins = 0
    same_team_loss = 0
    a_wins_over_b = 0
    b_wins_over_a = 0

    for row in rows:
        winner = row['winner']
        a_slot_nr = row['a_slot_nr']
        b_slot_nr = row['b_slot_nr']
        if winner == 'sentinel':
            if a_slot_nr < 5:
                if b_slot_nr < 5:
                    same_team_wins += 1
                else:
                    a_wins_over_b += 1
            else:
                if b_slot_nr < 5:
                    b_wins_over_a += 1
                else:
                    same_team_loss += 1
        else:
            if a_slot_nr < 5:
                if b_slot_nr < 5:
                    same_team_loss += 1
                else:
                    b_wins_over_a += 1
            else:
                if b_slot_nr < 5:
                    a_wins_over_b += 1
                else:
                    same_team_wins += 1

    return '```' + (name + ' vs ' + name2 + ':').ljust(25) + str(a_wins_over_b) + '/' + str(b_wins_over_a) + '\n' \
        + 'same team:'.ljust(25) + str(same_team_wins) + '/' + str(same_team_loss) + '```'


def structure_game_msg(winner, mins, secs, team1_win_elo_inc,
                       team2_win_elo_inc, dota_players, team1_avg_elo, team2_avg_elo):
    msg = "```Winner: " + winner + ', ' + str(mins) + 'm, ' + str(secs) + 's, elo ratio (' +\
          str(round(team1_win_elo_inc, 1)) + '/' + str(round(team2_win_elo_inc, 1)) + ')\n'

    team1_dp = [dota_player for dota_player in dota_players if dota_player.team == 1]
    team2_dp = [dota_player for dota_player in dota_players if dota_player.team == 2]

    msg += 'sentinel avg elo: ' + str(round(team1_avg_elo, 1)) + '\n'
    for dota_player in team1_dp:
        msg += strwidthright(dota_player.name, 17, dota_player.kills, 4,
                        dota_player.deaths, 4, dota_player.assists, 4) + '\n'
    msg += 'scourge avg elo: ' + str(round(team2_avg_elo, 1)) + '\n'
    for dota_player in team2_dp:
        msg += strwidthright(dota_player.name, 17, dota_player.kills, 4,
                        dota_player.deaths, 4, dota_player.assists, 4) + '\n'
    msg += "```"
    return msg


def add_dp_dbentries(dota_players, db_entries, winner):
    for n in range(len(dota_players)):  # same order as before
        dota_player = dota_players[n]
        db_entry = db_entries[n]
        db_entry.games += 1
        db_entry.kdagames += 1
        db_entry.csgames += 1
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
        db_entry.avgkills = db_entry.kills / db_entry.kdagames
        db_entry.avgdeaths = db_entry.deaths / db_entry.kdagames
        db_entry.avgassists = db_entry.assists / db_entry.kdagames
        db_entry.avgcskills = db_entry.cskills / db_entry.csgames
        db_entry.avgcsdenies = db_entry.csdenies / db_entry.csgames


def decompress_parse_db_replay(replay, status: Status, status_queue: queue.Queue):
    status_queue.put('Attempting to decompress..')
    data = decompress_replay(replay)
    dota_players, winner, mins, secs, mode = get_dota_w3mmd_stats(data)

    # check if already uploaded
    stats_bytes = str([dota_player.get_values() for dota_player in dota_players]).encode('utf-8')
    md5 = get_hash(stats_bytes)
    if check_if_file_with_hash_exists(md5):
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
    winner = ['sentinel', 'scourge'][winner-1]

    msg = structure_game_msg(winner, mins, secs, team1_win_elo_inc,
                             team2_win_elo_inc, dota_players, team1_avg_elo, team2_avg_elo)

    msg += "!confirm or !discard"
    status_queue.put(msg)

    while True:
        request = status.request_queue.get()
        if request == 'discard':
            return "Discarded the replay."
        elif request == 'confirm':
            break

    # status_queue.put("Uploading to db..")
    status.progress = "Uploading to local db"

    date_and_time = unixtime_to_datetime(time.time())
    # yyyymmdd_xxhxxmxxs_complete_winner_mins_secs_hash
    filename = date_and_time + '_complete_' + winner + '_' + str(mins) + '_' + str(secs) + '_' + md5
    save_file(replay, filename)

    try:
        game_entry = insert_game(
            {
                'mode': mode,
                'winner': winner,
                'duration': (60*mins+secs),
                'upload_time': date_and_time,
                'hash': md5,
                'ranked': 1,
                'completion': 'complete',
                'withkda': 1,
                'withcs': 1,
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
    games = fetchall(sql, upload_time)
    n = 0
    for g in games:
        if status:
            status.progress = "Rolling back.. " + slash_delimited(n, len(games))
        reset_stats_of_latest_game(g['game_id'])
        n += 1


def recalculate_elo_from_game(upload_time, status=None):
    sql = "select * from games where upload_time>=%s AND ranked=1 ORDER BY upload_time ASC;"
    games = fetchall(sql, upload_time)

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
                if winner == 'sentinel':
                    p['wins'] += 1
                else:
                    p['loss'] += 1
            else:
                team2 += [p]
                if winner == 'scourge':
                    p['wins'] += 1
                else:
                    p['loss'] += 1

            pg['elo_before'] = p['elo']
            update_player_game(pg)
            p['games'] += 1
            if game['withkda'] == 1:
                p['kdagames'] += 1
                p['kills'] += pg['kills']
                p['deaths'] += pg['deaths']
                p['assists'] += pg['assists']
                p['avgkills'] = p['kills'] / p['kdagames']
                p['avgdeaths'] = p['deaths'] / p['kdagames']
                p['avgassists'] = p['assists'] / p['kdagames']
            if game['withcs'] == 1:
                p['csgames'] += 1
                p['cskills'] += pg['cskills']
                p['csdenies'] += pg['csdenies']
                p['avgcskills'] = p['avgcskills'] / p['csgames']
                p['avgcsdenies'] = p['avgcsdenies'] / p['csgames']

        # set game teams avg elo
        team1_avg_elo = avg_team_elo_dict(team1)
        team2_avg_elo = avg_team_elo_dict(team2)
        game['team1_elo'] = team1_avg_elo
        game['team2_elo'] = team2_avg_elo

        # elo calculation
        team1_win_elo, team2_win_elo = team_win_elos_dict(team1_avg_elo, team2_avg_elo)

        # set game teams_elo_change
        if winner == 'sentinel':
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
        if game['winner'] == 'sentinel':
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
        if game['withkda'] == 1:
            p['kdagames'] -= 1
            p['kills'] -= pg['kills']
            p['deaths'] -= pg['deaths']
            p['assists'] -= pg['assists']
        if game['withcs'] == 1:
            p['csgames'] -= 1
            p['cskills'] -= pg['cskills']
            p['csdenies'] -= pg['csdenies']
        if p['kdagames'] > 0:
            p['avgkills'] = p['kills'] / p['kdagames']
            p['avgdeaths'] = p['deaths'] / p['kdagames']
            p['avgassists'] = p['assists'] / p['kdagames']
        else:
            p['avgkills'] = 0
            p['avgdeaths'] = 0
            p['avgassists'] = 0
        if p['csgames'] > 0:
            p['avgcskills'] = p['avgcskills'] / p['csgames']
            p['avgcsdenies'] = p['avgcsdenies'] / p['csgames']
        else:
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
            'csdenies': db_entry.dota_player.csdenies,
            'item1': db_entry.dota_player.item1,
            'item2': db_entry.dota_player.item2,
            'item3': db_entry.dota_player.item3,
            'item4': db_entry.dota_player.item4,
            'item5': db_entry.dota_player.item5,
            'item6': db_entry.dota_player.item6,
            'hero' : db_entry.dota_player.hero
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
    if check_if_file_with_hash_exists(md5):
        game = get_game(md5, 'hash')
        return "Replay already uploaded with Game ID: " + str(game['game_id'])

    team1, team2, db_entries, new_db_entries, old_db_entries = get_teams_and_dbentries(dota_players)

    if len(team1) != len(team2):
        return "Not an equal amount of players on both teams."

    # determine elo change
    team1_win_elo_inc, team2_win_elo_inc = team_win_elos(team1, team2)
    team1_avg_elo, team2_avg_elo = avg_team_elo(team1), avg_team_elo(team2)

    msg = structure_game_msg('?', '?', '?', team1_win_elo_inc,
                             team2_win_elo_inc, dota_players, team1_avg_elo, team2_avg_elo)

    # send preliminary message
    msg += "!discard or !manual winner mins secs"
    status_queue.put(msg)

    while True:
        request = status.request_queue.get()

        if request == 'discard':
            return "Discarded the replay."
        elif request[0] == 'manual':
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
    winner = ['sentinel', 'scourge'][winner - 1]

    msg = structure_game_msg(winner, mins, secs, team1_win_elo_inc,
                             team2_win_elo_inc, dota_players, team1_avg_elo, team2_avg_elo)

    msg += "!confirm or !discard"
    status_queue.put(msg)

    while True:
        request = status.request_queue.get()
        if request == 'discard':
            return "Discarded the replay."
        elif request == 'confirm':
            break

    status_queue.put("Uploading to db..")

    date_and_time = unixtime_to_datetime(time.time())
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
             'completion': 'incomplete',
             'withkda': 1,
             'withcs': 1,
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
    unregistered = []
    for dota_player in dota_players:
        dota_player.name = dota_player.name.lower()
        db_entry = get_player_bnet(dota_player.name)
        if db_entry is None:
            db_entry = DBEntry(dota_player)
            #new_db_entries += [db_entry]
            unregistered += [dota_player]
        else:
            db_entry = DBEntry(db_entry)
            old_db_entries += [db_entry]

        db_entry.old_elo = db_entry.elo

        if dota_player.team == 1:
            team1 += [db_entry]
        elif dota_player.team == 2:
            team2 += [db_entry]

        db_entries += [db_entry]

    if unregistered:
        raise UnregisteredPlayers('Unregistered players: ' + str([dota_player.name for dota_player in unregistered]))

    return team1, team2, db_entries, new_db_entries, old_db_entries


def auto_replay_upload(replay, date_and_time=None, winner=None, mins=None, secs=None, completion='unset'):
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
    winner = ['sentinel', 'scourge'][winner - 1]

    try:
        game_entry = insert_game(
            {'mode': mode,
             'winner': winner,
             'duration': (60 * mins + secs),
             'upload_time': date_and_time,
             'hash': md5,
             'ranked': 1,
             'completion': completion,
             'withkda': 1,
             'withcs': 1,
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
    change_replayfile_upload_time(old_upload_time, new_upload_time)
    update_game(game)
    return "Game ID: " + str(game_id) + " upload time set to " + new_upload_time


def rank_players(status):
    status.progress = "Ranking players.."
    sql = "select * from player where games>0 ORDER BY elo DESC"
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

    msg += "sentinel elo: " + str(round(game['team1_elo'], 1)) + ", change: " \
           + str(round(game['team1_elo_change'], 1)) + '\n'

    sql = "select * from player_game where game_id=%s order by slot_nr ASC"
    player_games = fetchall(sql, game_id)
    for pg in player_games[:5]:
        player = get_player_id(pg['player_id'])
        if player['name']:
            name = player['name']
        else:
            name = player['bnet_tag']
        msg += strwidthright(name, 17)
        if game['withkda'] == 1:
            msg += strwidthright(pg['kills'], 4, pg['deaths'], 4, pg['assists'], 4)
        msg += '\n'

    msg += "scourge elo: " + str(round(game['team2_elo'], 1)) + ", change: " \
           + str(round(-game['team1_elo_change'], 1)) + '\n'

    for pg in player_games[5:]:
        player = get_player_id(pg['player_id'])
        if player['name']:
            name = player['name']
        else:
            name = player['bnet_tag']
        msg += strwidthright(name, 17)
        if game['withkda'] == 1:
            msg += strwidthright(pg['kills'], 4, pg['deaths'], 4, pg['assists'], 4)
        msg += '\n'
    msg += "```"
    return msg


def reupload_all_replays(status: Status, status_queue: queue.Queue):
    cleard_db()
    status_queue.put("cleared db")
    n = 0
    files = sorted(os.listdir('replays'))
    for file in files:
        status.progress = 'Uploading ' + slash_delimited(n+1, len(files))
        
        parts = file[:-4].split('_')
        date_and_time = parts[0] + '_' + parts[1]  # date, time
        completion = parts[2]
        if parts[3] == 'sentinel':
            winner = 1
        elif parts[3] == 'scourge':
            winner = 2
        else:
            return "Stopped at a bad filename: " + file + " at " + parts[3]
        mins = int(parts[4])
        secs = int(parts[5])    
        
        if completion == 'incomplete' or completion == 'complete':
            f = open('replays/' + file, 'rb')
            data = f.read()
            f.close()        
            auto_replay_upload(data, date_and_time, winner, mins, secs, completion)
        elif parts[2] == 'custom':
            f = open('replays/' + file, 'r')
            lines = f.readlines()
            f.close()          
            winner = ['sentinel', 'scourge'][winner-1]
            auto_upload_typed(lines, winner, mins, secs)
        n += 1

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
        if fnmatch.fnmatch(file, game['upload_time'] + '*'):
            filepath = os.path.join("replays", file)
            os.remove(filepath)

            # remove game
            sql = "delete from games where game_id=%s"
            commit(sql, game_id)
            # remove pgs
            sql = "delete from player_game where game_id=%s"
            commit(sql, game_id)
            # if the players only have 0 games, remove them aswell?
            # can cause trouble if someone unranks 1 of 2 games, and then deletes the last, and then reranks
            return "Game " + str(game_id) + " is no more."

    return "Replay file on disk not found."


def auto_upload_typed(lines, winner, mins, secs):
    withkda = withcs = False
    k, d, a = None, None, None
    longline = ""
    for line in lines:
        longline += line + " "
    words = longline.split()
    if len(words) == 10:
        player_names = [words[n] for n in range(0, 10, 1)]
    elif len(words) == 40:
        withkda = True
        player_names = [words[n] for n in range(0, 40, 4)]
        k = [int(words[n]) for n in range(1, 40, 4)]
        d = [int(words[n]) for n in range(2, 40, 4)]
        a = [int(words[n]) for n in range(3, 40, 4)]
    else:
        raise Exception('auto_upload_typed wrong nr of arguments')

    # search in database
    team1 = []
    team2 = []
    db_entries = []
    new_db_entries = []
    old_db_entries = []
    for n in range(10):
        player_name = player_names[n].lower()
        db_entry = get_player(player_name)
        if db_entry is None:
            db_entry = DBEntry(player_name)
            new_db_entries += [db_entry]
        else:
            db_entry = DBEntry(db_entry)
            old_db_entries += [db_entry]

        db_entry.old_elo = db_entry.elo

        if n < 5:
            team1 += [db_entry]
        else:
            team2 += [db_entry]

        db_entries += [db_entry]
    
    # determine elo change
    team1_avg_elo, team2_avg_elo = avg_team_elo(team1), avg_team_elo(team2)
    t1_elo_change, t2_elo_change = teams_update_elo(team1, team2, winner)

    # save in database
    upload_time = unixtime_to_datetime(time.time())
    game_entry = insert_game(
        {
            'mode': None,
            'winner': winner,
            'duration': (60*mins+secs),
            'upload_time': upload_time,
            'ranked': 1,
            'completion': 'custom',
            'withkda': withkda,
            'withcs': withcs,
            'hash': None,
            'team1_elo': team1_avg_elo,
            'team2_elo': team2_avg_elo,
            'team1_elo_change': t1_elo_change,
            'elo_alg': '1.0'
        }
    )
    game_id = game_entry['LAST_INSERT_ID()']
 
    # add player stats
    for n in range(10):
        db_entry = db_entries[n]
        db_entry.games += 1
        db_entry.old_elo = db_entry.elo
        if n < 5:
            if winner == 'sentinel':
                db_entry.wins += 1
            else:
                db_entry.loss += 1
        else:
            if winner == 'scourge':
                db_entry.wins += 1
            else:
                db_entry.loss += 1

        if withkda:
            db_entry.kills += k[n]
            db_entry.deaths += d[n]
            db_entry.assists += a[n]
            db_entry.kdagames += 1
            db_entry.avgkills = db_entry.kills / db_entry.kdagames
            db_entry.avgdeaths = db_entry.deaths / db_entry.kdagames
            db_entry.avgassists = db_entry.assists / db_entry.kdagames

    # insert new players
    for db_entry in new_db_entries:
        response = insert_player(db_entry.get_hm())
        db_entry.player_id = response['LAST_INSERT_ID()']
    for db_entry in old_db_entries:
        update_player(db_entry.get_hm())

    # pgs   
    for n in range(10):
        db_entry = db_entries[n]
        if withkda:
            insert_player_game({
                'player_id': db_entry.player_id,
                'game_id': game_id,
                'slot_nr': n,
                'elo_before': db_entry.old_elo,
                'kills': k[n],
                'deaths': d[n],
                'assists': a[n]
            })
        else:
            insert_player_game({
                'player_id': db_entry.player_id,
                'game_id': game_id,
                'slot_nr': n,
                'elo_before': db_entry.old_elo
            })


def upload_typed_replay(payload: list, status: Status, status_queue: queue.Queue):
    withkda = withcs = False
    k, d, a = None, None, None
    if len(payload) == 13:
        player_names = [payload[n] for n in range(3, 13, 1)]
    elif len(payload) == 43:
        withkda = True
        player_names = [payload[n] for n in range(3, 43, 4)]
        k = [int(payload[n]) for n in range(4, 43, 4)]
        d = [int(payload[n]) for n in range(5, 43, 4)]
        a = [int(payload[n]) for n in range(6, 43, 4)]
    else:
        return 'Need 13 arguments: winner mins secs player1 ... player10'
    if len(set(player_names)) != 10:
        return "Not 10 different player names"
    winner = payload[0]
    mins = int(payload[1])
    secs = int(payload[2])

    # get players, winner, mins, secs
    # optional kda

    # search in database
    team1 = []
    team2 = []
    db_entries = []
    new_db_entries = []
    old_db_entries = []
    for n in range(10):
        player_name = player_names[n].lower()
        db_entry = get_player(player_name)
        if db_entry is None:
            db_entry = DBEntry(player_name)
            new_db_entries += [db_entry]
        else:
            db_entry = DBEntry(db_entry)
            old_db_entries += [db_entry]

        db_entry.old_elo = db_entry.elo
        
        if n < 5:
            team1 += [db_entry]
        else:
            team2 += [db_entry]

        db_entries += [db_entry]
    
    # determine elo change
    team1_win_elo_inc, team2_win_elo_inc = team_win_elos(team1, team2)
    team1_avg_elo, team2_avg_elo = avg_team_elo(team1), avg_team_elo(team2)
    t1_elo_change, t2_elo_change = teams_update_elo(team1, team2, winner)

    # confirm - note new players with (new?) to prevent spelling errors
    msg = '```'
    msg += 'Winner: ' + winner + ', ' + str(mins) + 'm ' + str(secs) + 's elo: ('\
           + str(round(team1_win_elo_inc, 1)) + '/' + str(round(team2_win_elo_inc, 1)) + ')\n'
    msg += 'sentinel elo: ' + str(team1_avg_elo) + '\n'
    for n in range(10):
        player_name = player_names[n]
        msg += player_name
        if player_name in [db_entry.name for db_entry in new_db_entries]:
            msg += '(new?)'
        if withkda:
            msg += ' ' + str(k[n]) + ',' + str(d[n]) + ',' + str(a[n])
        if n == 4:
            msg += '\n--------\n'
            msg += 'scourge elo: ' + str(team2_avg_elo)
        msg += '\n'
    msg += '```'

    # confirm
    msg += "!confirm or !discard"
    status_queue.put(msg)

    while True:
        request = status.request_queue.get()
        if request == 'discard':
            return "Discarded the replay."
        elif request == 'confirm':
            break

    status_queue.put("Uploading to db..")

    # save in database
    upload_time = unixtime_to_datetime(time.time())
    game_entry = insert_game(
        {
            'mode': None,
            'winner': winner,
            'duration': (60*mins+secs),
            'upload_time': upload_time,
            'ranked': 1,
            'completion': 'custom',
            'withkda': withkda,
            'withcs': withcs,
            'hash': None,
            'team1_elo': team1_avg_elo,
            'team2_elo': team2_avg_elo,
            'team1_elo_change': t1_elo_change,
            'elo_alg': '1.0'
        }
    )
    game_id = game_entry['LAST_INSERT_ID()']
 
    # add player stats
    for n in range(10):
        db_entry = db_entries[n]
        db_entry.games += 1
        if n < 5:
            if winner == 'sentinel':
                db_entry.wins += 1
            else:
                db_entry.loss += 1
        else:
            if winner == 'scourge':
                db_entry.wins += 1
            else:
                db_entry.loss += 1
        if withkda:
            db_entry.kills += k[n]
            db_entry.deaths += d[n]
            db_entry.assists += a[n]
            db_entry.kdagames += 1
            db_entry.avgkills = db_entry.kills / db_entry.kdagames
            db_entry.avgdeaths = db_entry.deaths / db_entry.kdagames
            db_entry.avgassists = db_entry.assists / db_entry.kdagames

    # insert new players
    for db_entry in new_db_entries:
        response = insert_player(db_entry.get_hm())
        db_entry.player_id = response['LAST_INSERT_ID()']
    for db_entry in old_db_entries:
        update_player(db_entry.get_hm())

    # pgs   
    for n in range(10):
        db_entry = db_entries[n]
        if withkda:
            insert_player_game({
                'player_id': db_entry.player_id,
                'game_id': game_id,
                'slot_nr': n,
                'elo_before': db_entry.old_elo,
                'kills': k[n],
                'deaths': d[n],
                'assists': a[n]
            })
        else:
            insert_player_game({
                'player_id': db_entry.player_id,
                'game_id': game_id,
                'slot_nr': n,
                'elo_before': db_entry.old_elo
            })
        
    # save in a file
    filename = upload_time + '_custom_' + winner + '_' + str(mins) + '_' + str(secs) + '.txt'
    f = open('replays/' + filename, 'w')
    for n in range(10):
        print(player_names[n], end="", file=f)
        if len(payload) == 43:
            print('', k[n], d[n], a[n], end='', file=f)
        print(file=f)
    f.close()

    rank_players(status)
    if keys.REMOTE_DB:
        transfer_db(status)
    return "Replay uploaded to db. Game ID: " + str(game_id)
    

def capt_rank():
    sql = """
        select g.game_id, g.winner, blue.name as blue, pink.name as pink 
        from
        games g, player_game bluepg, player_game pinkpg, player blue, player pink
        where 
        g.game_id=bluepg.game_id and
        g.game_id=pinkpg.game_id and
        bluepg.slot_nr=0 and
        pinkpg.slot_nr=5 and
        blue.player_id=bluepg.player_id and
        pink.player_id=pinkpg.player_id;
        """

    rows = fetchall(sql, ())

    #add up ranks
    players = {}
    for row in rows:
        blue = row['blue']
        pink = row['pink']
        winner = row['winner']
        
        if blue not in players:
            players[blue] = [0, 0]
        if pink not in players:
            players[pink] = [0, 0]
    
        if winner == 'sentinel':
            players[blue][0] += 1
            players[pink][1] += 1
        elif winner == 'scourge':
            players[blue][1] += 1
            players[pink][0] += 1


    list_players = []
    for name in players:
        list_players += [(name, players[name][0], players[name][1])]

    list_players.sort(key=lambda x: x[1], reverse=True)

    msg = ""
    rank = 1
    for player in list_players:
        name, wins, loss = player
        msg += strwidthleft(rank, 4, name, 18, wins, 4, loss, 4) + "\n"
        rank += 1

    # return all
    return msg


def get_all_stats():
    # normal ranking
    sql = "select * from player order by rank"
    rows = fetchall(sql, ())

    player_stats = ""
    for player in rows:
        player_stats += strwidthleft(
            player['rank'], 4,
            player['name'], 18,
            round(player['elo'],1), 8,
            player['wins'], 4,
            player['loss'], 4,
            round(player['avgkills'],1), 7,
            round(player['avgdeaths'],1), 7,
            round(player['avgassists'],1), 7
        ) + '\n'

    # captain ranking
    captain_stats = capt_rank()

    return player_stats, captain_stats


def new_season(status: Status, status_queue: queue.Queue):
    # confirmation - season nr

    msg = "!confirm or !discard on archiving the current season."
    status_queue.put(msg)

    while True:
        request = status.request_queue.get()
        if request == 'discard':
            return "Did not archive the season."
        elif request == 'confirm':
            break

    # get stats
    player_stats, captain_stats = get_all_stats()

    # stash replays -> season1/...
    past_seasons = os.listdir('./past_seasons/')
    current_season = len(past_seasons) + 1

    new_season_path = 'past_seasons/season'+str(current_season)
    os.mkdir(new_season_path)
    os.rename('replays', new_season_path + '/replays')
    os.mkdir('replays')

    # save stats
    f = open(new_season_path + '/player_stats.txt', 'w')
    print(player_stats, file=f, end='')
    f.close()

    f = open(new_season_path + '/captain_stats.txt', 'w')
    print(captain_stats, file=f, end='')
    f.close()

    # cleardb
    cleard_db()
    if keys.REMOTE_DB:
        transfer_db(status)

    return "Season " + str(current_season) + ' has been archived.'


class Client(discord.Client):
    player_queue = []
    current_replay_upload = []
    lock = False

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

        author = message.author
        roles = [role.name for role in author.roles]
        admin = ('DotA-Admin' in roles) or ('Development' in roles)
        dota_role = any([role in roles for role in ['DotA-Admin', 'DotA-Trial']])
        messager = Message(message.channel)
        if command == '!sd':
            await self.sd_handler(message, payload)
        elif command == '!get_captain_rank':
            await self.capt_rank_handler(message)
        elif command == '!get_all_stats':
            await self.get_all_stats_handler(message)
        elif command == '!confirm':
            await self.confirm_replay_handler(message)
        elif command == '!discard':
            await self.discard_replay_handler(message)
        elif command == '!force_discard' and admin:
            await self.force_discard_replay_handler(message)
        elif command == '!force_unlock' and admin:
            await self.force_unlock_handler(message)
        elif command == '!new_season' and admin:
            await self.new_season_handler(message)
        elif command == '!manual' and payload:
            await self.manual_replay_handler(message, payload)
        elif command == '!clear_db' and admin:
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
        elif command == '!upload' and admin and payload:
            await self.upload_typed_replay_handler(message, payload)
        elif command == '!delete' and payload and admin:
            await self.delete_replay_handler(message, payload)
        elif command == '!shutdown' and admin:
            await self.shutdown_handler(message)
        elif command == '!force_shutdown' and admin:
            await self.force_shutdown_handler(message)
        elif command == '!help':
            await self.help_handler(message)
        elif command == '!link' and payload and (dota_role or admin):
            await self.link_handler(message, payload)
        elif command == '!checklink' and (dota_role or admin):
            await self.checklink_handler(message)
        elif command == '!force_register' and admin:
            await self.force_register(message, payload)
        for attachment in message.attachments:
            if admin:
                if attachment.filename[-4:] == '.w3g':
                    data = requests.get(attachment.url).content
                    await self.replay_handler(message, data)
                elif attachment.filename == "users.txt":
                    data = requests.get(attachment.url).content
                    await self.users_upload_handler(message, data)
                else:
                    await messager.send('Not a wc3 replay.')

    @staticmethod
    async def force_register(message, payload):
        discord_id = payload[0]
        name = payload[1]
        bnet_tag = payload[2]
        insert_player({
            'name': name,
            'bnet_tag': bnet_tag,
            'discord_id': int(discord_id)
        })
        msg = 'User added as discord id ' + discord_id + ', name ' + name + ', bnet tag ' + bnet_tag
        await message.channel.send(emb(msg))

    @staticmethod
    async def users_upload_handler(message, data):
        lines = data.decode('utf-8')
        lines = lines.split('\n')
        n = 0
        #try:
        for line in lines:
            n += 1
            words = line.split()
            if not words:
                break

            discord_id, bnet_tag = words

            if get_player_discord_id(discord_id):
                continue

            member = message.guild.get_member(int(discord_id))
            if member is None:
                continue

            name = member.nick
            if name is None:
                name, _ = member.__str__().split('#')

            insert_player({
                'name': name.lower(),
                'bnet_tag': bnet_tag.lower(),
                'discord_id': int(discord_id)
            })
        await message.channel.send(str(n) + ' users imported.')

        #except Exception as e:
        #    await message.channel.send(str(e))


    @staticmethod
    async def checklink_handler(message, payload):
        user = message.author
        player_discord_id = get_player_discord_id(user.id)
        if player_discord_id:
            msg = "Your dota profile:\nBnet tag: " + player_discord_id['bnet_tag'] + ', Name: ' + player_discord_id['name']
        else:
            msg = "You don't have a dota profile. Type !link bnet_tag"
        await message.channel.send(emb(msg))

    @staticmethod
    async def link_handler(message, payload):
        bnet_tag = payload[0].lower()
        user = message.author
        discord_id = user.id

        name = message.guild.get_member(int(discord_id)).nick
        if name is None:
            name, _ = user.__str__().split('#')

        name = name.lower()
        # check if bnet_tag exists in players
        player_bnet = get_player_bnet(bnet_tag)
        player_discord_id = get_player_discord_id(user.id)

        if not player_bnet:
            if not player_discord_id:
                insert_player({
                    'name': name,
                    'bnet_tag': bnet_tag,
                    'discord_id': discord_id
                })
                msg = "Created dota profile:\nBnet tag: " + bnet_tag + '\nName: ' + user.display_name

            else:
                player_discord_id['bnet_tag'] = bnet_tag
                player_discord_id['name'] = name
                update_player(player_discord_id)
                msg = "Your dota profile have changed to:\nBnet tag: " + player_discord_id['bnet_tag'] + '\nName: ' + player_discord_id['name']
        else:
            if player_bnet['discord_id'] == user.id:
                player_discord_id['name'] = name
                update_player(player_discord_id)
                msg = "Your dota profile have changed to:\nBnet tag: " + player_discord_id['bnet_tag'] + '\nName: ' + player_discord_id['name']

            else:
                msg = "A dota profile with the bnet tag " + player_bnet['bnet_tag'] + ' is already used by ' + player_bnet['name']

        msg = emb(msg)
        await message.channel.send(msg)

    @staticmethod
    async def force_unlock_handler(message):
        if Client.lock:
            Client.lock = False
            if Client.current_replay_upload:
                _, t1, status = Client.current_replay_upload
                status.request_queue.put('discard')
                await message.channel.send("One thread handed !discard")
            await message.channel.send("Db unlocked.")
        else:
            await message.channel.send("Db already unlocked.")


    @staticmethod
    async def new_season_handler(message):
        if Client.lock:
            await message.channel.send("Db is currently locked.")
            return
        Client.lock = True

        status_queue = queue.Queue()
        status = Status()
        t1 = ThreadAnything(new_season, (), status=status, status_queue=status_queue)
        Client.current_replay_upload = (message.author, t1, status)
        t1.start()

        while t1.is_alive():
            while not status_queue.empty():
                await message.channel.send(status_queue.get_nowait())
            await asyncio.sleep(0.1)

        while not status_queue.empty():  # can maybe make prettier
            await message.channel.send(status_queue.get_nowait())

        Client.lock = False
        Client.current_replay_upload = None
        if t1.exception:
            await message.channel.send("Exception in new_season function.")
            raise t1.exception

        if t1.rv:
            await message.channel.send(str(t1.rv))
        else:
            await message.channel.send("Error: this shouldn't happen.")


    @staticmethod
    async def get_all_stats_handler(message):

        t1 = ThreadAnything(get_all_stats, ())
        t1.start()

        response = Message(message.channel)

        while t1.is_alive():
            await asyncio.sleep(0.1)

        if t1.exception:
            raise t1.exception

        if t1.rv:
            fio = io.StringIO(t1.rv[0])     # player stats
            fio2 = io.StringIO(t1.rv[1])    # captain stats
            f1 = discord.File(fio, "player_stats.txt")
            f2 = discord.File(fio2, "captain_stats.txt")
            await message.channel.send(files=[f1,f2])
        else:
            await response.send('No return from capt_rank.')

    @staticmethod
    async def help_handler(message):
        commands = ['!sd name',
                    '!sd name1 name2',
                    '!register bnet_tag',
                    '!get_captain_rank',
                    '!get_all_stats',
                    '!show game_id',
                    'admin commands:',
                    '!unrank game_id',
                    '!rank game_id',
                    '!setdate game_id yyyymmdd_xxhxxmxxs',
                    '!reupload_all_replays (in order of upload time)',
                    '!delete game_id (deletes an unranked game)',
                    '!force_discard',
                    '!force_unlock',
                    '!new_season (past can be seen on http://134.209.173.188 )']
        msg = '```'
        for command in commands:
            msg += command + '\n'
        msg += '```'
        await message.channel.send(msg)

    @staticmethod
    async def clear_db_handler(message):
        if Client.lock:
            await message.channel.send("Db is currently locked.")
            return
        Client.lock = True
        cleard_db()
        Client.lock = False
        await message.channel.send("Cleared db.")

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
    async def capt_rank_handler(message: discord.message.Message):

        t1 = ThreadAnything(capt_rank, ())
        t1.start()

        response = Message(message.channel)

        while t1.is_alive():
            await asyncio.sleep(0.1)

        if t1.exception:
            raise t1.exception

        if t1.rv:
            fio = io.StringIO(t1.rv)
            f1 = discord.File(fio, "captain_stats.txt")
            #await response.send("hi", files=[f1])
            await message.channel.send(files=[f1])
        else:
            await response.send('No return from capt_rank')

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
            except UnregisteredPlayers:
                await message.channel.send(str(t1.exception.msg))

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
    async def upload_typed_replay_handler(message: discord.message.Message, payload):
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
        t1 = ThreadAnything(upload_typed_replay, (payload,), status=status, status_queue=status_queue)
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
            await message.channel.send(t1.exception)
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
            await message.channel.send("One thread handed !discard")
        else:
            await message.channel.send("Nothing to discard")

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
            t1 = ThreadAnything(sd_player, (name,))
        elif len(payload) == 2:
            name = payload[0]
            name2 = payload[1]
            t1 = ThreadAnything(sd_players, (name, name2))
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
            await message.channel.send("modify_game_upload_time exception")
            Client.lock = False
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
