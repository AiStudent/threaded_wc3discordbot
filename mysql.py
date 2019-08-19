
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
    print(sql)
    commit(sql, values)


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
def update_game(game_hm: dict, where: str):
    update_dict('games', game_hm, (where,))


def get_game(hash):
    sql = "SELECT * FROM games WHERE hash = %s"
    return fetchone(sql, (hash,))


def insert_game(game):
    insert_dict('games', game)


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
    insert_dict('player', player)

def get_player(name):
    sql = "SELECT * FROM player WHERE name = %s"
    return fetchone(sql, (name,))

def update_player(player: dict, where: str):
    update_dict('player', player, (where,))

# player game
def insert_player_game(player_game):
    insert_dict('player_game', player_game)


def get_player_games(player_id):
    sql = "SELECT * FROM player_game WHERE player_id=%s"
    return fetchall(sql, (player_id,))

# TODO
def update_player_game(player_game):
    update_dict('player_game', player_game, ('player_id', 'game_id'))



testplayer =         {
            'name' : "fantom",
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
    'player_id' : 3,
    'game_id' : 2,
    'elo' : 1000,
    'kills' : 10,
    'deaths' : 5,
    'assists' : 9,
    'cskills' : 10,
    'csdenies' : 0
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

    pg = get_player_games(1)[0]
    print(pg)
    pg['kills'] = 4
    update_player_game(pg)
    #pg = get_player_games(1)[0]
    pg = get_player_games(1)[0]
    print(pg)

