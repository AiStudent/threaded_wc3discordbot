
import hashlib
from datetime import datetime
import fnmatch
import os

def unixtime_to_datetime(ut):
    return datetime.utcfromtimestamp(ut).strftime('%Y%m%d_%Hh%Mm%Ss')


def strwidthright(name: str, width, *args):
    name = str(name)
    string = name.ljust(width)
    for n in range(0, len(args)-2, 2):
        string += (str(args[n])+', ').rjust(args[n+1])
    if len(args) > 2:
        string += str(args[-2])
    return string


def strwidthleft(name: str, width, *args):
    name = str(name)
    string = name.ljust(width)
    for n in range(0, len(args)-2, 2):
        string += (str(args[n])+' ').ljust(args[n+1])
    if len(args) > 2:
        string += str(args[-2])
    return string


def slash_delimited(*args):
    string = '('
    for n in range(len(args)-1):
        string += str(args[n]) + '/'
    string += str(args[-1]) + ')'
    return string


def get_hash(data):
    blocksize = 65536
    hasher = hashlib.md5()
    index = 0
    buf = data[index:index+blocksize]
    while len(buf) > 0:
        index += blocksize
        hasher.update(buf)
        buf = data[index:index+blocksize]
    return hasher.hexdigest()


def check_if_file_with_hash_exists(md5):
    for file in os.listdir('replays'):
        if fnmatch.fnmatch(file, '*' + md5 + '*.w3g'):
            return True
    return False

def save_file(data, filename):
    f = open('replays/'+filename+'.w3g', 'wb')
    f.write(data)
    f.close()


def search_replay_on_disk(upload_time):
    for file in os.listdir('replays'):
        if file[:len(upload_time)] == upload_time:
            return file
    raise Exception('File not found: ' + upload_time + '*.w3g')


def emb(string: str) -> str:
    return "```" + string + "```"
