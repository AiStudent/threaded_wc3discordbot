
import pymysql
import keys


def connect_to_db():
    connection = pymysql.connect(
        host=keys.dbhost,
        user=keys.dbuser,
        password=keys.dbpass,
        db=keys.db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    return connection


def commit(sql, args):
    connection = connect_to_db()
    with connection.cursor() as cursor:
        cursor.execute(sql, args)
        connection.commit()
    connection.close()


def commit_and_check(sql, args):
    connection = connect_to_db()
    with connection.cursor() as cursor:
        result = cursor.execute(sql, args)
        connection.commit()

    connection.close()
    return result

def fetch(sql, args, one: bool):
    connection = connect_to_db()
    with connection.cursor() as cursor:
        cursor.execute(sql, args)
        if one:
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()
    connection.close()
    return result


def fetchall(sql, args):
    return fetch(sql, args, one=False)


def fetchone(sql, args):
    return fetch(sql, args, one=True)


def values_subs(fields):
    subs = "("
    for n in range(len(fields)-1):
        subs += "%s, "
    subs += "%s)"
    return subs


def inline_subs(fields):
    subs = ""
    for field in fields[:-1]:
        subs += field + "=%s, "
    subs += fields[-1] + "=%s"
    return subs


def keys2str(hm):
    fields = [key for key in hm]
    str_fields = "("
    for field in fields[:-1]:
        str_fields += str(field) + ", "
    str_fields += str(fields[-1]) + ")"
    return str_fields


def insert_dict(table: str, dictionary: dict):
    hm = dictionary.copy()
    values = tuple([hm[key] for key in hm])
    sql = "INSERT INTO " + table + " " + keys2str(hm) + " VALUES " + values_subs(values)
    commit(sql, values)


def insert_dict_and_check(table: str, dictionary: dict):
    hm = dictionary.copy()
    values = tuple([hm[key] for key in hm])
    sql = "INSERT INTO " + table + " " + keys2str(hm) + " VALUES " + values_subs(values)
    return commit_and_check(sql, values)


def update_dict(table: str, game_hm: dict, where: tuple):
    hm = game_hm.copy()
    where_values = [hm[w] for w in where]
    for w in where:
        del hm[w]
    fields = [key for key in hm]
    where_str = ""
    for where_arg in where[:-1]:
        where_str += where_arg + "=%s AND "
    where_str += where[-1] + "=%s"
    sql = "UPDATE " + table + " SET " + inline_subs(fields) + " where " + where_str
    args = [hm[key] for key in hm] + where_values
    commit(sql, args)


# -------------- usable ----------------
# game
def update_game(game_hm: dict, where='game_id'):
    update_dict('games', game_hm, (where,))


def get_game(value, field='game_id'):
    sql = "SELECT * FROM games WHERE " + field + " = %s"
    return fetchone(sql, (value,))


def insert_game(game):
    return insert_dict_and_check('games', game)


def rank_game(game_id):
    fields = ['ranked']
    sql = "UPDATE games SET " + inline_subs(fields) + " where game_id=%s"
    commit(sql, (1, game_id))


def unrank_game(game_id):
    fields = ['ranked']
    sql = "UPDATE games SET " + inline_subs(fields) + " where game_id=%s"
    commit(sql, (0, game_id))


# player
def insert_player(player):
    return insert_dict_and_check('player', player)


def get_player(name):
    sql = "SELECT * FROM player WHERE name = %s"
    return fetchone(sql, (name,))


def get_player_id(player_id):
    sql = "SELECT * FROM player WHERE player_id = %s"
    return fetchone(sql, (player_id,))


def get_player_bnet(bnet_tag):
    sql = "SELECT * FROM player WHERE bnet_tag = %s OR bnet_tag2 =%s"
    return fetchone(sql, (bnet_tag, bnet_tag,))


def get_player_discord_id(discord_id):
    sql = "SELECT * FROM player WHERE discord_id = %s"
    return fetchone(sql, (discord_id,))


def update_player(player: dict, where = 'player_id'):
    update_dict('player', player, (where,))


# player game
def insert_player_game(player_game):
    insert_dict('player_game', player_game)


def get_player_games(player_id):
    sql = "SELECT * FROM player_game WHERE player_id=%s"
    return fetchall(sql, (player_id,))


def update_player_game(player_game):
    update_dict('player_game', player_game, ('player_id', 'game_id'))



testgame = {
            'mode': 'cdzm',
            'winner': 'scourge',
            'duration': 2000,
            'upload_time': '12345678_01h02m03s',
            'ranked': 2,
            'hash': None,
            'team1_elo': 1050.0,
            'team2_elo': 1025.0,
            'team1_elo_change': 25.0,
            'elo_alg': '1.0'
        }


def get_elo_history(bnet_tag):
    sql = """(
    select 
        pg.elo_before 
    from 
        player_game pg, 
        player p 
    where 
        pg.player_id = p.player_id and 
        p.bnet_tag=%s 
    order by pg.game_id asc
    ) UNION (
    select
        elo 
    from 
        player 
    where 
        bnet_tag=%s
    );
    """
    return fetchall(sql, (bnet_tag, bnet_tag))


if __name__ == '__main__':
    sql = "describe player;"
    res = commit_and_check(sql, ())
    print(res)


"""
select g.game_id, g.ranked, g.upload_time, blue.name, pink.name from
games g, player_game bluepg, player_game pinkpg, player blue, player pink where
g.game_id=bluepg.game_id and
g.game_id=pinkpg.game_id and
bluepg.slot_nr=0 and
pinkpg.slot_nr=5 and
blue.player_id=bluepg.player_id and
pink.player_id=pinkpg.player_id;

select * from player where name='one_legion'

 select g.game_id, g.ranked, g.upload_time, blue.name, pink.name from games g, player_game bluepg, player_game pinkpg, player blue, player pink where g.game_id=bluepg.game_id and g.game_id=pinkpg.game_id and bluepg.slot_nr=0 and pinkpg.slot_nr=5 and blue.player_id=bluepg.player_id and pink.player_id=pinkpg.player_id order by upload_time ASC;

 select g.game_id, g.ranked, g.upload_time, blue.name as blue, pink.name as pink from games g, player_game bluepg, player_game pinkpg, player blue, player pink where g.game_id=bluepg.game_id and g.game_id=pinkpg.game_id and bluepg.slot_nr=0 and pinkpg.slot_nr=5 and blue.player_id=bluepg.player_id and pink.player_id=pinkpg.player_id order by upload_time ASC;
firstbl5_dotastats_v2.

# all games a certain player has played?
select g.game_id, g.ranked, g.winner, g.upload_time, blue.name as blue, pink.name as pink 
from
games g, player_game bluepg, player_game pinkpg, player blue, player pink, player who 
where 
g.game_id=bluepg.game_id and
g.game_id=pinkpg.game_id and
bluepg.slot_nr=0 and
pinkpg.slot_nr=5 and
blue.player_id=bluepg.player_id and
pink.player_id=pinkpg.player_id and
who.name='one_legion'
order by upload_time ASC";


select g.game_id, g.winner, apg.slot_nr as a_slot_nr, bpg.slot_nr as b_slot_nr
from
games g, player_game apg, player_game bpg, player  a, player b
where
a.name = 'wc3addict' and
b.name = 'fook' and
a.player_id = apg.player_id and
b.player_id = bpg.player_id and
apg.game_id = bpg.game_id and
g.game_id = apg.game_id;
    

# get captain win/loss
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