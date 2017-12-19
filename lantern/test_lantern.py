import asyncio
import pytest
from functools import reduce
from threading import Thread

from main import CommandTransport
from main import CommandProtocol
from main import CommandColor, CommandOn, CommandOff, CommandUnknown, \
    BaseCommand
from main import LanternClient, LanternServer
from main import DEFAULT_HOST, DEFAULT_PORT


class DefaultReader:
    """Reads data as much as requested until the data depletes."""
    def __init__(self, data):
        self._data = data

    async def read(self, n):
        chunk = self._data[:n]
        self._data = self._data[n:]
        return chunk

    def at_eof(self):
        return len(self._data) == 0


class OneByteAtATimeReader(DefaultReader):
    """Reads one byte of data at a time until the data depletes."""
    async def read(self, n):
        return await super().read(1)


class TestCommand:
    def test_compare(self):
        assert CommandOn() == CommandOn()
        assert CommandOff() == CommandOff()
        assert CommandColor.from_rgb(1, 2, 3) == \
            CommandColor.from_rgb(1, 2, 3)


class TestCommandProtocol:
    def test_encode(self):
        assert CommandProtocol.encode(CommandOn()) == b'\x12\x00\x00'
        assert CommandProtocol.encode(CommandOff()) == b'\x13\x00\x00'
        assert CommandProtocol.encode(CommandColor.from_rgb(1, 2, 3)) == \
            b'\x20\x00\x03\x01\x02\x03'

    def test_decode(self):
        assert CommandProtocol.decode(0x12, None) == CommandOn()
        assert CommandProtocol.decode(0x13, None) == CommandOff()
        assert CommandProtocol.decode(0x20, b'\x01\x02\x03') == \
            CommandColor.from_rgb(1, 2, 3)

    def test_register(self):
        class CommandTest(BaseCommand):
            type = 0x98
        with pytest.raises(ValueError):
            CommandProtocol.register(CommandTest)


class TestClientServerInteraction:
    def test_client_server(self):
        sent_commands = [
            CommandUnknown(),
            CommandColor.from_rgb(10, 20, 30),
            CommandOn(),
            CommandOff(),
            CommandColor.from_rgb(40, 50, 60),
        ]
        received_commands = []
        host, port = DEFAULT_HOST, DEFAULT_PORT

        def run_server():
            loop = asyncio.new_event_loop()
            num_connections_to_serve = 1
            server = LanternServer(host, port, loop,
                                   num_connections_to_serve,
                                   sent_commands)
            server.run_forever()
            loop.close()

        def run_client():
            loop = asyncio.new_event_loop()
            num_connections_to_process = 1
            client = LanternClient(host, port, loop,
                                   num_connections_to_process,
                                   received_commands.append)
            loop.run_until_complete(client.loop())
            loop.close()

        # TODO: This patching is bit ugly. But as long as we're in a hurry
        # due to upcoming release (that is scheduled on yesterday), let's
        # leave this as is. It could serve as a great starting task for the
        # junior that is joining us next month.
        saved = CommandProtocol._registry[CommandUnknown.type]
        del CommandProtocol._registry[CommandUnknown.type]

        server_thread = Thread(target=run_server)
        server_thread.start()
        client_thread = Thread(target=run_client)
        client_thread.start()

        server_thread.join()
        client_thread.join()

        CommandProtocol._registry[CommandUnknown.type] = saved
        # Unknown command should not be seen by the receiver.
        assert received_commands == sent_commands[1:]


@pytest.fixture()
def command():
    return CommandColor.from_rgb(1, 2, 3)


@pytest.fixture()
def data(command):
    return CommandProtocol.encode(command)


class TestCommandTransport:
    @pytest.mark.asyncio
    async def test_recv_command_partial_header(self, data):
        reader = DefaultReader(data[:1])
        transport = CommandTransport(reader, writer=None)
        result = await transport.recv()
        assert None is result
        assert transport.at_eof()

    @pytest.mark.asyncio
    async def test_recv_command_partial_body(self, data):
        reader = OneByteAtATimeReader(data[:5])
        transport = CommandTransport(reader, writer=None)
        result = await transport.recv()
        assert None is result
        assert transport.at_eof()

    @pytest.mark.asyncio
    async def test_recv_command_ok(self, command, data):
        reader = DefaultReader(data)
        transport = CommandTransport(reader, writer=None)
        result = await transport.recv()
        assert command == result
        assert transport.at_eof()

    @pytest.mark.asyncio
    async def test_recv_command_one_by_at_a_time(self, command, data):
        reader = OneByteAtATimeReader(data)
        transport = CommandTransport(reader, writer=None)
        result = await transport.recv()
        assert command == result
        assert transport.at_eof()

    @pytest.mark.asyncio
    async def test_recv_several_commands(self):
        commands = [
            CommandColor.from_rgb(10, 20, 30),
            CommandOn(),
            CommandOff(),
            CommandColor.from_rgb(40, 50, 60),
        ]
        data = reduce(
            lambda acc, x: acc + CommandProtocol.encode(x),
            commands, bytearray())
        reader = OneByteAtATimeReader(data)
        transport = CommandTransport(reader, writer=None)
        result = [await transport.recv() for _ in range(len(commands) - 1)]

        assert not transport.at_eof()
        result.append(await transport.recv())

        assert transport.at_eof()
        assert commands == result
        assert transport.at_eof()
