import queue
import socket
import threading


class TCPQueue(object):

    def __init__(self, op_type="client", host=None, port=3030, listen=1024):
        if op_type not in ["client", "server"]:
            raise Exception("Error not support operation type. [%s]" % op_type)
        if op_type == "client":
            if host is None:
                raise Exception("Error it is not input.")
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((host, port))
        else:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(("", port))
            self.sock.listen(listen)
        self.op_type = op_type
        self.workers = []
        self.queue = {}

    def server_worker(self, wid, client):
        clisock = client[0]
        buf = b""
        while 1:
            msg = clisock.recv(32768)
            if not msg:
                break
            if msg.endswith(b"\n") and (len(msg) < 2 or msg.endswith(b"\t\n")):
                msg = msg.rstrip()
                msg = buf + msg
                buf = b""
            else:
                buf += msg
                continue
            if msg.startswith(b"get"):
                msg = msg.split(b" ")
                if len(msg) == 2:
                    queue_name = msg[1]
                else:
                    res = b"Error\n"
                    clisock.send(res)
                    continue
                try:
                    if queue_name in self.queue:
                        res = self.queue[queue_name].get_nowait()
                        res += b"\t\n"
                    else:
                        res = b"\n"
                except queue.Empty:
                    res = b"\n"
            elif msg.startswith(b"put"):
                msg = msg.split(b" ")
                if len(msg) > 2:
                    queue_name = msg[1]
                else:
                    res = b"Error\n"
                    clisock.send(res)
                    continue
                try:
                    if queue_name in self.queue:
                        self.queue[queue_name].put(b" ".join(msg[2:]))
                        res = b"successful\n"
                    else:
                        self.queue[queue_name] = queue.Queue()
                        self.queue[queue_name].put(b" ".join(msg[2:]))
                        res = b"successful\n"
                except queue.Empty:
                    res = b"\n"
            elif msg == b"fin":
                res = b"fin"
            elif msg.startswith(b"len"):
                msg = msg.split(b" ")
                if len(msg) > 2:
                    queue_name = msg[1]
                else:
                    res = b"Error\n"
                    clisock.send(res)
                    continue
                try:
                    if queue_name in self.queue:
                        qsize = str(self.queue[queue_name].qsize())
                        res = (qsize + "\n").encode("utf-8")
                    else:
                        self.queue[queue_name] = queue.Queue()
                        res = b"0\n"
                except queue.Empty:
                    res = b"\n"
            else:
                res = b"\n"
            clisock.send(res)
            if res == b"fin":
                clisock.close()
                break

    def server(self):
        wid = 0
        while 1:
            client = self.sock.accept()
            th = threading.Thread(
                    target=self.server_worker, args=(wid, client))
            th.setDaemon(True)
            th.start()
            self.workers.append(th)

    def put(self, queue_name, data):
        if not isinstance(queue_name, bytes):
            raise Exception("Required type is bytes")
        if not isinstance(data, bytes):
            raise Exception("Required type is bytes")
        if self.op_type == "client":
            self.sock.send(b"put %s %s\t\n" % (queue_name, data))
            res = self.sock.recv(1024)
            if res != b"successful\n":
                raise Exception("Put message error")
        else:
            if queue_name in self.queue:
                self.queue[queue_name].put(data)
            else:
                self.queue[queue_name] = queue.Queue()
                self.queue[queue_name].put(data)

    def len(self, queue_name):
        if not isinstance(queue_name, bytes):
            raise Exception("Required type is bytes")
        if self.op_type == "client":
            self.sock.send(b"len %s\t\n" % queue_name)
            res = self.sock.recv(1024)
            qsize = int(res.rstrip())
        else:
            if queue_name in self.queue:
                qsize = self.queue[queue_name].qsize()
            else:
                self.queue[queue_name] = queue.Queue()
                qsize = 0
        return qsize

    def get(self, queue_name):
        if not isinstance(queue_name, bytes):
            raise Exception("Required type is bytes")
        if self.op_type == "client":
            self.sock.send(b"get %s\t\n" % queue_name)
            buf = b""
            while 1:
                res = self.sock.recv(32768)
                if res.endswith(b"\n") and (
                        len(res) < 2 or res.endswith(b"\t\n")):
                    res = buf + res
                    res = res.rstrip()
                    break
                else:
                    buf += res
            if not res:
                raise Exception("Empty")
        else:
            try:
                if queue_name in self.queue:
                    res = self.queue[queue_name].get_nowait()
                else:
                    raise Exception("Empty")
            except queue.Empty:
                raise Exception("Empty")
        return res

    def run(self):
        if self.op_type == "server":
            self.server()
        else:
            raise Exception("Not supported operation type")

    def start(self):
        if self.op_type == "server":
            th = threading.Thread(target=self.run)
            th.setDaemon(True)
            th.start()
        else:
            raise Exception("Not supported operation type")


if __name__ == "__main__":
    tcp_queue = TCPQueue(op_type="server")
    tcp_queue.run()
