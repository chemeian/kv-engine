"""End-to-end tests against a real TCP server: RESP, inline commands, recovery."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import threading
import unittest

from kvengine import Config, Engine, resp
from kvengine.server import KVServer
from tests.helpers import Client, ErrorReply


class ServerTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.data_dir = os.path.join(self._tmp.name, "data")
        self.engine = Engine(
            Config(
                data_dir=self.data_dir,
                snapshot_interval=3600.0,
                snapshot_every_writes=10 ** 9,
            )
        )
        self.addCleanup(self.engine.close)
        self.server = KVServer(("127.0.0.1", 0), self.engine)
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        self._thread.start()
        self.addCleanup(self._stop_server)

    def _stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self._thread.join(timeout=5)

    def connect(self, timeout=5.0):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=timeout)
        client = Client(sock)
        self.addCleanup(client.close)
        return client


class CommandTests(ServerTestCase):
    def test_set_get_exists_delete(self):
        client = self.connect()
        self.assertEqual(client.call("SET", "k", "v"), "OK")
        self.assertEqual(client.call("GET", "k"), "v")
        self.assertEqual(client.call("EXISTS", "k"), 1)
        self.assertEqual(client.call("DEL", "k"), 1)
        self.assertEqual(client.call("DEL", "k"), 0)
        self.assertEqual(client.call("EXISTS", "k"), 0)

    def test_get_missing_returns_nil(self):
        self.assertIsNone(self.connect().call("GET", "missing"))

    def test_empty_value_round_trips(self):
        client = self.connect()
        client.call("SET", "k", "")
        self.assertEqual(client.call("GET", "k"), "")

    def test_values_with_spaces_and_unicode(self):
        client = self.connect()
        client.call("SET", "greeting", "hello world 世界")
        self.assertEqual(client.call("GET", "greeting"), "hello world 世界")

    def test_expire_and_ttl(self):
        client = self.connect()
        client.call("SET", "k", "v")
        self.assertEqual(client.call("TTL", "k"), -1)
        self.assertEqual(client.call("EXPIRE", "k", "100"), 1)
        self.assertEqual(client.call("TTL", "k"), 100)
        self.assertEqual(client.call("EXPIRE", "missing", "100"), 0)
        self.assertEqual(client.call("TTL", "missing"), -1)

    def test_set_with_ex_and_expired_key_disappears(self):
        client = self.connect()
        client.call("SET", "k", "v", "EX", "1")
        self.assertEqual(client.call("TTL", "k"), 1)
        self.assertEqual(client.call("GET", "k"), "v")
        self.engine.store.expire("k", 0)
        self.assertIsNone(client.call("GET", "k"))
        self.assertEqual(client.call("EXISTS", "k"), 0)

    def test_keys_pattern(self):
        client = self.connect()
        for key in ("user:1", "user:2", "other"):
            client.call("SET", key, "x")
        self.assertEqual(client.call("KEYS", "user:*"), ["user:1", "user:2"])
        self.assertEqual(client.call("KEYS", "*"), ["other", "user:1", "user:2"])
        self.assertEqual(client.call("KEYS", "nope*"), [])

    def test_ping_echo_and_command(self):
        client = self.connect()
        self.assertEqual(client.call("PING"), "PONG")
        self.assertEqual(client.call("PING", "hello"), "hello")
        self.assertEqual(client.call("ECHO", "abc"), "abc")
        self.assertEqual(client.call("COMMAND"), [])

    def test_unknown_command_is_reported_as_an_error(self):
        reply = self.connect().call("FLUSHALL")
        self.assertIsInstance(reply, ErrorReply)
        self.assertIn("unknown command", reply)

    def test_wrong_arity_is_reported_as_an_error(self):
        client = self.connect()
        self.assertIn("wrong number of arguments", client.call("GET"))
        self.assertIn("wrong number of arguments", client.call("SET", "only-key"))

    def test_invalid_expire_value_is_reported_as_an_error(self):
        client = self.connect()
        self.assertIn("not an integer", client.call("SET", "k", "v", "EX", "soon"))
        self.assertIn("invalid expire time", client.call("SET", "k", "v", "EX", "0"))

    def test_unknown_set_option_is_a_syntax_error(self):
        self.assertIn("syntax error", self.connect().call("SET", "k", "v", "WAT"))

    def test_quit_closes_the_connection(self):
        client = self.connect()
        self.assertEqual(client.call("QUIT"), "OK")
        with self.assertRaises(EOFError):
            client.read_reply()

    def test_pipelined_commands_in_a_single_write(self):
        client = self.connect()
        payload = resp.encode_command(["SET", "a", "1"])
        payload += resp.encode_command(["SET", "b", "2"])
        payload += resp.encode_command(["GET", "a"])
        client.send_raw(payload)
        self.assertEqual(client.read_reply(), "OK")
        self.assertEqual(client.read_reply(), "OK")
        self.assertEqual(client.read_reply(), "1")


class InlineCommandTests(ServerTestCase):
    """What a person typing into netcat sends: plain text, not RESP arrays."""

    def test_inline_commands_with_crlf(self):
        client = self.connect()
        client.send_raw(b"SET foo bar\r\nGET foo\r\n")
        self.assertEqual(client.read_reply(), "OK")
        self.assertEqual(client.read_reply(), "bar")

    def test_inline_commands_with_bare_lf(self):
        client = self.connect()
        client.send_raw(b"SET foo bar\nGET foo\n")
        self.assertEqual(client.read_reply(), "OK")
        self.assertEqual(client.read_reply(), "bar")

    def test_inline_ping(self):
        client = self.connect()
        client.send_raw(b"PING\r\n")
        self.assertEqual(client.read_reply(), "PONG")

    def test_inline_error_keeps_the_connection_usable(self):
        client = self.connect()
        client.send_raw(b"BOGUS\r\nPING\r\n")
        self.assertIsInstance(client.read_reply(), ErrorReply)
        self.assertEqual(client.read_reply(), "PONG")


class BadProtocolTests(ServerTestCase):
    def test_reply_style_byte_from_client_is_a_protocol_error(self):
        client = self.connect()
        client.send_raw(b"+OK\r\n")
        reply = client.read_reply()
        self.assertIsInstance(reply, ErrorReply)
        self.assertIn("Protocol error", reply)

    def test_reopening_after_a_protocol_error_works(self):
        broken = self.connect()
        broken.send_raw(b"*1\r\n$5\r\nNOTABULK\r\n")
        self.assertIn("Protocol error", broken.read_reply())
        self.assertEqual(self.connect().call("PING"), "PONG")

    def test_client_disconnect_does_not_kill_the_server(self):
        client = self.connect()
        client.call("SET", "k", "v")
        client.close()
        self.assertEqual(self.connect().call("GET", "k"), "v")


class ConcurrencyOverNetworkTests(ServerTestCase):
    def test_many_clients_writing_in_parallel(self):
        clients = 5
        per_client = 40
        errors = []

        def worker(index):
            try:
                client = self.connect()
                payload = b"".join(
                    resp.encode_command(["SET", f"c{index}:{step}", str(step)])
                    for step in range(per_client)
                )
                client.send_raw(payload)
                for _ in range(per_client):
                    if client.read_reply() != "OK":
                        raise AssertionError("unexpected reply")
            except Exception as exc:  # noqa: BLE001 - surfaced to the test
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(index,)) for index in range(clients)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        self.assertEqual(errors, [])

        client = self.connect()
        for index in range(clients):
            for step in range(per_client):
                self.assertEqual(client.call("GET", f"c{index}:{step}"), str(step))


class RestartOverNetworkTests(ServerTestCase):
    def test_written_data_is_recovered_after_a_restart(self):
        client = self.connect()
        client.call("SET", "kept", "value")
        client.call("SET", "with-ttl", "value", "EX", "100")
        client.close()

        crash_dir = os.path.join(self._tmp.name, "crash")
        shutil.copytree(self.data_dir, crash_dir)
        recovered = Engine(Config(data_dir=crash_dir))
        self.addCleanup(recovered.close)
        self.assertEqual(recovered.store.get("kept"), "value")
        self.assertEqual(recovered.store.ttl("with-ttl"), 100)


class RedisCliTests(ServerTestCase):
    @unittest.skipUnless(shutil.which("redis-cli"), "redis-cli is not installed")
    def test_redis_cli_can_talk_to_the_server(self):
        def cli(*args):
            return subprocess.run(
                ["redis-cli", "-p", str(self.port), *args],
                capture_output=True,
                text=True,
                timeout=20,
            )

        self.assertEqual(cli("SET", "cli:key", "cli-value").stdout.strip(), "OK")
        self.assertEqual(cli("GET", "cli:key").stdout.strip(), "cli-value")
        self.assertEqual(cli("TTL", "cli:key").stdout.strip(), "-1")
        self.assertEqual(cli("EXISTS", "cli:key").stdout.strip(), "1")
        self.assertEqual(cli("DEL", "cli:key").stdout.strip(), "1")


if __name__ == "__main__":
    unittest.main()

