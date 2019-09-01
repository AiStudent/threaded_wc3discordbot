
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
        cursor.execute(sql, args)
        cursor.execute("SELECT LAST_INSERT_ID()", ())
        result = cursor.fetchone()
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



testplayer =         {
            'name' : "holybear",
            'elo' : 1050.0,
            'games' : 2,
            'wins' : 1,
            'loss' : 1,
            'draw' : 0,
            'kills' : 30,
            'deaths' : 40,
            'assists' : 50,
            'cskills' : 1,
            'csdenies' : 2,
            'avgkills' : 3,
            'avgdeaths' : 4,
            'avgassists' : 5,
            'avgcskills' : 6,
            'avgcsdenies' : 7
        }

test_playergame = {
    'player_id' : 1,
    'game_id' : 3,
    'elo' : 1000,
    'kills' : 10,
    'deaths' : 5,
    'assists' : 9,
    'cskills' : 10,
    'csdenies' : 0
}

testgame = {
            'mode': 'cdzd',
            'winner': 1,
            'duration': 2,
            'upload_time': 'date..',
            'hash': '4',
            'ranked': 1,
            'elo_alg': 'v1.0'
        }


if __name__ == '__main__':

    #insert_into_games((1, 2, 3, '7', 5, 6, 7))

    #unrank_game(4)111

    #update_game('6', (1, 2, 3, 5, 6, 7))

    #g = get_game('6')
    #print(g)
    #g['winner'] = 4
    #update_game(g, 'game_id')
    #print(get_game('6'))

    #print(get_player_game(3))
    #update_player_game

    #p = get_player('fantom')
    #p['elo'] = 2000
    #update_player(p, 'player_id')
    #print(get_player('fantom'))

    #pg = get_player_games(1)[0]
    #print(pg)
    #pg['kills'] = 4
    #update_player_game(pg)
    #pg = get_player_games(1)[0]
    #pg = get_player_games(1)[0]
    #print(pg)

    #insert_player_game(test_playergame)
    #insert_player(testplayer)
    #insert_game(testgame)
    # get player_id game stuff
    sql = "select g.*, pg.* from games g, player_game pg where g.game_id=pg.game_id and pg.player_id=%s"
    #print(fetchall(sql, (1,)))

    # get all games each player was in
    sql = "select p.name, pg.game_id from games g, player_game pg, player p where g.game_id=pg.game_id and p.player_id=pg.player_id"

    sql = "insert into person (name) values ('asdasd')"
    #print(commit_and_check(sql, ()))
    #sql = "SELECT LAST_INSERT_ID()"
    #print(fetchall(sql, ()))

    p = get_player_games()

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

"""