"""End-to-end tests against the real TCP server over a real socket."""

import os
import socket
import tempfile
import threading
import time
import unittest

from kvs import resp
from kvs.client import KVClient
from kvs.engine import Engine
from kvs.server import KVSServer


def recv_until(sock, expected_length, timeout=5.0):
    sock.settimeout(timeout)
    data = b""
    while len(data) < expected_length:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    return data


class ServerTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.engine = Engine(self._tmp.name, snapshot_interval=0, snapshot_writes=0).open()
        self.server = KVSServer(self.engine, host="127.0.0.1", port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.host, self.port = self.server.address[:2]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.engine.close()
        self._tmp.cleanup()

    def client(self):
        return KVClient(self.host, self.port)

    # -- basic command coverage -------------------------------------------
    def test_set_get_del_exists_roundtrip(self):
        with self.client() as client:
            self.assertEqual(client.command("SET", "k", "v"), "OK")
            self.assertEqual(client.command("GET", "k"), b"v")
            self.assertEqual(client.command("EXISTS", "k"), 1)
            self.assertEqual(client.command("DEL", "k"), 1)
            self.assertEqual(client.command("EXISTS", "k"), 0)
            self.assertIsNone(client.command("GET", "k"))

    def test_get_missing_key_returns_null(self):
        with self.client() as client:
            self.assertIsNone(client.command("GET", "nope"))

    def test_keys_pattern_over_the_wire(self):
        with self.client() as client:
            client.command("SET", "user:1", "a")
            client.command("SET", "user:2", "b")
            client.command("SET", "other", "c")
            self.assertEqual(sorted(client.command("KEYS", "user:*")), [b"user:1", b"user:2"])
            self.assertEqual(len(client.command("KEYS", "*")), 3)

    def test_ping_and_echo(self):
        with self.client() as client:
            self.assertEqual(client.command("PING"), "PONG")
            self.assertEqual(client.command("PING", "hi"), b"hi")
            self.assertEqual(client.command("ECHO", "hi"), b"hi")

    def test_unknown_command_returns_error(self):
        with self.client() as client:
            reply = client.command("NOPE")
            self.assertIsInstance(reply, resp.ErrorReply)

    def test_wrong_arity_returns_error(self):
        with self.client() as client:
            reply = client.command("GET")
            self.assertIsInstance(reply, resp.ErrorReply)
            self.assertIn("wrong number of arguments", str(reply))

    def test_set_with_ex_sets_ttl(self):
        with self.client() as client:
            client.command("SET", "k", "v", "EX", "100")
            ttl = client.command("TTL", "k")
            self.assertTrue(0 < ttl <= 100)

    def test_set_with_invalid_expire_returns_error(self):
        with self.client() as client:
            reply = client.command("SET", "k", "v", "EX", "0")
            self.assertIsInstance(reply, resp.ErrorReply)

    def test_expire_and_ttl_commands(self):
        with self.client() as client:
            self.assertEqual(client.command("TTL", "missing"), -1)
            client.command("SET", "k", "v")
            self.assertEqual(client.command("TTL", "k"), -1)
            self.assertEqual(client.command("EXPIRE", "k", "100"), 1)
            self.assertTrue(0 < client.command("TTL", "k") <= 100)
            self.assertEqual(client.command("EXPIRE", "missing", "10"), 0)

    def test_expire_with_non_positive_seconds_deletes_key(self):
        with self.client() as client:
            client.command("SET", "k", "v")
            self.assertEqual(client.command("EXPIRE", "k", "0"), 1)
            self.assertEqual(client.command("EXISTS", "k"), 0)

    # -- protocol handling --------------------------------------------------
    def test_inline_commands_from_netcat(self):
        sock = socket.create_connection((self.host, self.port), timeout=5)
        try:
            sock.sendall(b"SET inline hello\r\nGET inline\r\n")
            data = recv_until(sock, len(b"+OK\r\n$5\r\nhello\r\n"))
        finally:
            sock.close()
        self.assertEqual(data, b"+OK\r\n$5\r\nhello\r\n")

    def test_pipelined_commands_get_all_replies(self):
        with self.client() as client:
            client.send_command("SET", "a", "1")
            client.send_command("SET", "b", "2")
            client.send_command("GET", "a")
            self.assertEqual(client.read_reply(), "OK")
            self.assertEqual(client.read_reply(), "OK")
            self.assertEqual(client.read_reply(), b"1")

    def test_multiple_clients_share_state(self):
        with self.client() as first, self.client() as second:
            first.command("SET", "shared", "1")
            self.assertEqual(second.command("GET", "shared"), b"1")

    def test_binary_safe_values(self):
        payload = b"\x00\xff\x80binary\r\n"
        with self.client() as client:
            client.send_command(b"SET", b"bin", payload)
            self.assertEqual(client.read_reply(), "OK")
            self.assertEqual(client.command("GET", "bin"), payload)

    # -- concurrency over the network --------------------------------------
    def test_concurrent_clients(self):
        thread_count = 8
        per_thread = 60
        errors = []
        barrier = threading.Barrier(thread_count)

        def worker(index):
            try:
                with self.client() as client:
                    barrier.wait(timeout=10)
                    for step in range(per_thread):
                        key = "c%d:%d" % (index, step)
                        client.command("SET", key, str(step))
                        if client.command("GET", key) != b"%d" % step:
                            raise AssertionError("mismatch for %s" % key)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)

        self.assertEqual(errors, [])
        for index in range(thread_count):
            for step in range(per_thread):
                self.assertEqual(self.engine.get("c%d:%d" % (index, step)), str(step))

    def test_data_is_persisted_while_server_runs(self):
        with self.client() as client:
            client.command("SET", "durable", "yes")
        # The engine wrote through to the AOF before replying.
        with open(os.path.join(self._tmp.name, "appendonly.aof"), "rb") as handle:
            self.assertIn(b"durable", handle.read())


if __name__ == "__main__":
    unittest.main()