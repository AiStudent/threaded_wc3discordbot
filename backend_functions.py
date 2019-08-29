from w3gtest.decompress import decompress_replay
from w3gtest.decompress import CouldNotDecompress
from w3gtest.dota_stats import get_dota_w3mmd_stats
from w3gtest.dota_stats import NotCompleteGame, NotDotaReplay, parse_incomplete_game
from w3gtest.dota_stats import DotaPlayer
import os
from datetime import datetime
import hashlib

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


def is_time(word:str):
    return word[:2].isnumeric() and word[2] is 'h' and word[3:5].isnumeric() \
        and word[5] and word[6:8].isnumeric() and word[8]

#only complete replays
def modified_time_to_dota_replay_format(old_path, new_path):
    files = os.listdir(old_path)

    for file in files:

        words = file.split('_')
        if len(words) == 7:
            if words[0].isnumeric() and is_time(words[1]) and words[4].isnumeric() and words[5].isnumeric():
                print('skipping', file)
                continue



        stat = os.stat(old_path+file)
        ts = int(stat.st_mtime)
        date_and_time = datetime.utcfromtimestamp(ts).strftime('%Y%m%d_%Hh%Mm%Ss')

        f = open(old_path + file, 'rb')
        replay = f.read()
        f.close()

        data = decompress_replay(replay)
        try:
            dota_players, winner, mins, secs, mode = get_dota_w3mmd_stats(data)
            winner = ['Sentinel', 'Scourge'][winner - 1]
            stats_bytes = str([dota_player.get_values() for dota_player in dota_players]).encode('utf-8')
            md5 = get_hash(stats_bytes)

            new_filename = date_and_time + '_complete_' + winner\
                           + '_' + str(mins) + '_' + str(secs) + '_' + str(md5) + '.w3g'

            print(new_filename)
            #os.rename('replays/' + file, 'replays/'+new_filename)
            f = open(new_path + new_filename, 'wb')
            f.write(replay)
            f.close()
        except NotCompleteGame:
            dota_players, mode, unparsed = parse_incomplete_game(data)
            stats_bytes = str([dota_player.get_values() for dota_player in dota_players]).encode('utf-8')
            md5 = get_hash(stats_bytes)
            new_filename = date_and_time + '_incomplete_none_none_none_' + md5 + '.w3g'
            print(new_filename)
            #os.rename('replays/' + file, 'replays/'+new_filename)
            f = open(new_path + new_filename, 'wb')
            f.write(replay)
            f.close()
        except NotDotaReplay:
            print('skipping NotDotaReplay', file)


import sys
modified_time_to_dota_replay_format(sys.argv[1], sys.argv[2])