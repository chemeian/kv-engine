"""Unit tests for the RESP encoder and parser."""

from __future__ import annotations

import io
import unittest

from kvengine import resp


class EncoderTests(unittest.TestCase):
    def test_scalar_encoders(self):
        self.assertEqual(resp.encode_simple("OK"), b"+OK\r\n")
        self.assertEqual(resp.encode_error("ERR nope"), b"-ERR nope\r\n")
        self.assertEqual(resp.encode_integer(42), b":42\r\n")
        self.assertEqual(resp.encode_bulk("hi"), b"$2\r\nhi\r\n")
        self.assertEqual(resp.encode_bulk(""), b"$0\r\n\r\n")
        self.assertEqual(resp.encode_bulk(None), b"$-1\r\n")

    def test_array_of_bulk_strings(self):
        encoded = resp.encode_array([resp.encode_bulk("a"), resp.encode_bulk("bb")])
        self.assertEqual(encoded, b"*2\r\n$1\r\na\r\n$2\r\nbb\r\n")
        self.assertEqual(resp.encode_array([]), b"*0\r\n")

    def test_encode_command_round_trips_through_reader(self):
        argv = ["SET", "a key", "a value with \n inside"]
        reader = resp.RespReader(io.BytesIO(resp.encode_command(argv)))
        self.assertEqual(reader.read_command(), argv)

    def test_utf8_payload(self):
        reader = resp.RespReader(io.BytesIO(resp.encode_command(["SET", "k", "值"])))
        self.assertEqual(reader.read_command(), ["SET", "k", "值"])


class ReaderTests(unittest.TestCase):
    def read(self, data: bytes):
        return resp.RespReader(io.BytesIO(data)).read_command()

    def test_inline_command_with_crlf(self):
        self.assertEqual(self.read(b"SET foo bar\r\n"), ["SET", "foo", "bar"])

    def test_inline_command_with_bare_lf(self):
        self.assertEqual(self.read(b"PING\n"), ["PING"])

    def test_blank_inline_line_is_ignored(self):
        self.assertIsNone(self.read(b"\r\n"))

    def test_multibulk_with_empty_argument(self):
        data = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n"
        self.assertEqual(self.read(data), ["SET", "k", ""])

    def test_several_commands_from_one_buffer(self):
        reader = resp.RespReader(io.BytesIO(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\nPING\r\n"))
        self.assertEqual(reader.read_command(), ["GET", "k"])
        self.assertEqual(reader.read_command(), ["PING"])

    def test_unterminated_line_raises(self):
        with self.assertRaises(resp.ProtocolError):
            self.read(b"PING")

    def test_reply_style_type_from_client_raises(self):
        with self.assertRaises(resp.ProtocolError):
            self.read(b"+OK\r\n")

    def test_expected_bulk_inside_command(self):
        with self.assertRaises(resp.ProtocolError):
            self.read(b"*1\r\n+OK\r\n")

    def test_null_bulk_inside_command_raises(self):
        with self.assertRaises(resp.ProtocolError):
            self.read(b"*1\r\n$-1\r\n")

    def test_truncated_multibulk_raises_connection_closed(self):
        with self.assertRaises(resp.ConnectionClosed):
            self.read(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n")

    def test_empty_stream_raises_connection_closed(self):
        with self.assertRaises(resp.ConnectionClosed):
            self.read(b"")

