"""RESP protocol tests: parsing, encoding and incomplete-input handling."""

import unittest

from kvs import resp


class RequestParsingTests(unittest.TestCase):
    def test_parse_multibulk_command(self):
        data = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
        args, position = resp.parse_request(data)
        self.assertEqual(args, [b"SET", b"k", b"v"])
        self.assertEqual(position, len(data))

    def test_incomplete_multibulk_returns_none(self):
        self.assertIsNone(resp.parse_request(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n"))

    def test_incomplete_bulk_header_returns_none(self):
        self.assertIsNone(resp.parse_request(b"*1\r\n$3\r\nPI"))

    def test_parse_inline_command(self):
        args, position = resp.parse_request(b"PING\r\n")
        self.assertEqual(args, [b"PING"])
        self.assertEqual(position, 6)

    def test_parse_inline_with_quotes(self):
        args, _ = resp.parse_request(b'SET "a b" c\n')
        self.assertEqual(args, [b"SET", b"a b", b"c"])

    def test_empty_inline_line_yields_no_command(self):
        args, position = resp.parse_request(b"\r\n")
        self.assertEqual(args, [])
        self.assertEqual(position, 2)

    def test_two_pipelined_commands_parse_in_sequence(self):
        data = (b"*1\r\n$4\r\nPING\r\n"
                b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        first, position = resp.parse_request(data)
        second, position = resp.parse_request(data, position)
        self.assertEqual(first, [b"PING"])
        self.assertEqual(second, [b"GET", b"k"])
        self.assertEqual(position, len(data))

    def test_binary_payload_is_preserved(self):
        payload = b"\x00\xff\x80binary\r\nbytes"
        encoded = resp.encode_command([b"SET", b"k", payload])
        args, _ = resp.parse_request(encoded)
        self.assertEqual(args[2], payload)

    def test_malformed_multibulk_raises(self):
        with self.assertRaises(resp.ProtocolError):
            resp.parse_request(b"*1\r\nx1\r\nfoo\r\n")

    def test_malformed_inline_quotes_raise(self):
        with self.assertRaises(resp.ProtocolError):
            resp.parse_request(b'SET "unterminated\n')


class EncodingTests(unittest.TestCase):
    def test_encode_command(self):
        self.assertEqual(resp.encode_command([b"SET", b"k", b"v"]),
                         b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")

    def test_encode_simple(self):
        self.assertEqual(resp.encode_simple("OK"), b"+OK\r\n")

    def test_encode_error(self):
        self.assertEqual(resp.encode_error("ERR boom"), b"-ERR boom\r\n")

    def test_encode_integer(self):
        self.assertEqual(resp.encode_integer(-2), b":-2\r\n")

    def test_encode_bulk(self):
        self.assertEqual(resp.encode_bulk("v"), b"$1\r\nv\r\n")

    def test_encode_null_bulk(self):
        self.assertEqual(resp.encode_bulk(None), b"$-1\r\n")

    def test_encode_array(self):
        self.assertEqual(resp.encode_array(["a", "bb"]),
                         b"*2\r\n$1\r\na\r\n$2\r\nbb\r\n")

    def test_non_ascii_helpers_round_trip(self):
        self.assertEqual(resp.to_str(resp.to_bytes("\u4e2d\u6587")), "\u4e2d\u6587")


class ReplyParsingTests(unittest.TestCase):
    def test_simple_string(self):
        value, position = resp.parse_reply(b"+OK\r\n")
        self.assertIsInstance(value, resp.SimpleString)
        self.assertEqual(value, "OK")
        self.assertEqual(position, 5)

    def test_error(self):
        value, _ = resp.parse_reply(b"-ERR nope\r\n")
        self.assertIsInstance(value, resp.ErrorReply)
        self.assertEqual(value, "ERR nope")

    def test_integer(self):
        self.assertEqual(resp.parse_reply(b":42\r\n")[0], 42)

    def test_bulk(self):
        self.assertEqual(resp.parse_reply(b"$3\r\nfoo\r\n")[0], b"foo")

    def test_null_bulk(self):
        self.assertIsNone(resp.parse_reply(b"$-1\r\n")[0])

    def test_array_with_mixed_types(self):
        value, _ = resp.parse_reply(b"*2\r\n$1\r\na\r\n:1\r\n")
        self.assertEqual(value, [b"a", 1])

    def test_incomplete_reply_returns_none(self):
        self.assertIsNone(resp.parse_reply(b"$3\r\nfo"))


if __name__ == "__main__":
    unittest.main()