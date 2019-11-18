
import threading
import queue

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