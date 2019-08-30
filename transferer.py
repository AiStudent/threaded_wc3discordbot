
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

def connect_to_bdb():
    connection = pymysql.connect(
        host=keys.bhost,
        user=keys.buser,
        password=keys.bpass,
        db=keys.bdb,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    return connection


def commit(sql, args):
    connection = connect_to_bdb()
    with connection.cursor() as cursor:
        cursor.execute(sql, args)
        connection.commit()
    connection.close()

def commitmany(sql, args):
    connection = connect_to_bdb()
    with connection.cursor() as cursor:
        cursor.executemany(sql, args)
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


def keys2str(fields):
    str_fields = "("
    for field in fields[:-1]:
        str_fields += str(field) + ", "
    str_fields += str(fields[-1]) + ")"
    return str_fields


def insert_dict(table: str, rows: list):
    dictionary = rows[0]
    hm = dictionary.copy()
    fields = [key for key in hm]
    values = tuple([hm[key] for key in fields])
    sql = "INSERT INTO " + table + " " + keys2str(fields) + " VALUES " + values_subs(values)

    #list of hashmaps to list of list of values
    rows = [[hm[key] for key in fields] for hm in rows]

    commitmany(sql, rows)


def clear_db():
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


def transfer_db(prints=False):
    if prints:
        print('fetching')
    sql="select * from player"
    player = fetchall(sql, ())
    sql="select * from games"
    games = fetchall(sql, ())
    sql="select * from player_game"
    pg = fetchall(sql, ())
    if prints:
        print('clear db')
    clear_db()
    if prints:
        print('ins player')
    insert_dict('player', player)
    if prints:
        print('ins games')
    insert_dict('games', games)
    if prints:
        print('ins pg')
    insert_dict('player_game', pg)

if __name__ == '__main__':

    transfer_db(True)

