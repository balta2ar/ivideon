import asyncio
import logging
from signal import SIGTERM, SIGINT
from struct import pack, unpack, unpack_from
from functools import wraps
from typing import Optional

import fire


logging.basicConfig(
    format='%(asctime)-15s %(name)-5s %(levelname)-8s %(message)s',
    level=logging.DEBUG)

_logger = logging.getLogger(__name__)


COMMAND_HEADER_LENGTH = 3
READ_DELAY = 0.1
RECONNECT_DELAY = 1.0
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 9999


class Lantern:
    """
    The main class to represent Lantern. It carries current color and state.
    It also knows how to react to commands. Should you need to add a command,
    handle it here as well.
    """
    def __init__(self):
        self._color = (0, 0, 0)
        self._active = False

    def on_command(self, command):
        if isinstance(command, CommandOn):
            self._active = True
            _logger.info('Lantern has been turned ON')
        elif isinstance(command, CommandOff):
            self._active = False
            _logger.info('Lantern has been turned OFF')
        elif isinstance(command, CommandColor):
            self._color = command.rgb
            _logger.info('Lantern is now glittering with a marvelous RGB %s '
                         'colour! Everyone shall gaze in awe!', self._color)


class CommandProtocol:
    """
    This class knows how to encode and decode messages. It also contains the
    registry of known messages.
    """
    _registry: dict = dict()

    @staticmethod
    def encode(command) -> bytes:
        message = pack('>BH', command.type, command.length)
        if command.length > 0:
            message += pack(f'>{command.length}s', command.value)
        return message

    @classmethod
    def decode(cls, type_, value):
        if type_ not in cls._registry:
            # Ignore unknown command
            return None

        return cls._registry[type_](value)

    @classmethod
    def register(cls, command) -> None:
        if command.type in cls._registry:
            raise ValueError('Command of type %s is already registered',
                             command.type)
        cls._registry[command.type] = command


class Meta(type):
    """This helper metaclass automatically registers a command."""
    def __new__(mcs, name, bases, class_dict):
        mcs = super().__new__(mcs, name, bases, class_dict)
        CommandProtocol.register(mcs)
        return mcs


class BaseCommand(object, metaclass=Meta):
    type = 0x00

    def __init__(self, value=None):
        self.value = value
        self.length = len(value) if value is not None else 0

    def __repr__(self):
        return f'{self.__class__.__name__}()'

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        return hash((self.type, self.length, self.value))


class CommandUnknown(BaseCommand):
    type = 0x99


class CommandOn(BaseCommand):
    type = 0x12


class CommandOff(BaseCommand):
    type = 0x13


class CommandColor(BaseCommand):
    type = 0x20
    @property
    def rgb(self) -> tuple:
        return unpack('>BBB', self.value)

    @staticmethod
    def from_rgb(r: int, g: int, b: int) -> BaseCommand:
        value = pack('>BBB', r, g, b)
        return CommandColor(value)

    def __repr__(self):
        r, g, b = self.rgb
        return f'{self.__class__.__name__}.from_rgb({r}, {g}, {b})'


class CommandTransport:
    """
    This class handles sending of reading of messages of the protocol. It can
    reconstruct a message from the data even if it arrives one byte at a time.
    """
    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer
        self._read_buffer = bytearray()

    def send(self, command: BaseCommand) -> None:
        message = CommandProtocol.encode(command)
        _logger.info('Sending message len %s, %s',
                     len(message), hex_encode(message))
        self._writer.write(message)

    async def _retry_read(self, n: int) -> bytearray:
        read_so_far = bytearray()
        while len(read_so_far) < n:
            read_so_far += await self._reader.read(n - len(read_so_far))
            if self.at_eof():
                return read_so_far
            await asyncio.sleep(READ_DELAY)
        return read_so_far

    def _bytes_missing(self, required_length: int) -> int:
        return required_length - len(self._read_buffer)

    async def _process_buffer(self) -> Optional[BaseCommand]:
        type_, length = unpack('>BH', self._read_buffer)
        value = None

        if length > 0:
            while self._bytes_missing(length + COMMAND_HEADER_LENGTH) > 0:
                if self.at_eof():
                    return None
                self._read_buffer += await self._retry_read(
                    self._bytes_missing(length + COMMAND_HEADER_LENGTH))

            value, = unpack_from(
                f'>{length}s', self._read_buffer, COMMAND_HEADER_LENGTH)

        self._read_buffer = self._read_buffer[length + COMMAND_HEADER_LENGTH:]
        return CommandProtocol.decode(type_, value)

    def at_eof(self) -> bool:
        return self._reader.at_eof()

    async def recv(self) -> Optional[BaseCommand]:
        while self._bytes_missing(COMMAND_HEADER_LENGTH) > 0:
            if self.at_eof():
                return None
            self._read_buffer += await self._retry_read(
                self._bytes_missing(COMMAND_HEADER_LENGTH))
        return await self._process_buffer()


def hex_encode(string):
    return ":".join("{:02x}".format(c) for c in string)


class LanternServer:
    """
    Simple Lantern server simulation for testing purposes.
    """
    def __init__(self, host, port, loop,
                 num_connections_to_serve=None,
                 commands=None):
        self._host = host
        self._port = port
        self._loop = loop
        self._num_connections_to_serve = num_connections_to_serve
        self._commands = commands

        if self._commands is None:
            self._commands = [
                CommandColor.from_rgb(1, 2, 10),
                CommandOn(),
                CommandColor.from_rgb(100, 2, 10),
                CommandOff(),
                CommandUnknown(),
            ]

    async def _handle_server(self, reader, writer):
        _logger.info('Client connected')
        transport = CommandTransport(reader, writer)

        for command in self._commands:
            _logger.info('Writing command: %s', command)
            transport.send(command)
            await asyncio.sleep(1.0)
        _logger.info('Closing client connection')
        writer.close()

        if self._num_connections_to_serve is not None:
            self._num_connections_to_serve -= 1
            if self._num_connections_to_serve == 0:
                self._loop.stop()

    def run_forever(self):
        coro = asyncio.start_server(self._handle_server,
                                    self._host, self._port,
                                    loop=self._loop)
        server = self._loop.run_until_complete(coro)

        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            pass

        server.close()
        self._loop.run_until_complete(server.wait_closed())


def simple_server(host=DEFAULT_HOST, port=DEFAULT_PORT):
    _logger.info('Starting simple server on %s:%s', host, port)
    loop = asyncio.get_event_loop()
    num_connections_to_serve = None
    server = LanternServer(host, port, loop, num_connections_to_serve)
    server.run_forever()
    loop.close()
    _logger.info('simple_server done')


class LanternClient:
    """
    Lantern client - handles the communication with the server and notifies
    upon new messages.
    """
    def __init__(self, host, port, loop,
                 num_connections_to_process=None,
                 on_command_callback=None):
        self._host = host
        self._port = port
        self._loop = loop
        self._running = True
        self._num_connections_to_process = num_connections_to_process
        self._on_command_callback = on_command_callback

    async def _connect(self) -> Optional[CommandTransport]:
        while self._running and self._num_connections_to_process != 0:
            try:
                _logger.info('Connecting to server %s:%s',
                             self._host, self._port)
                reader, writer = await asyncio.open_connection(
                    self._host, self._port, loop=self._loop)
                if self._num_connections_to_process is not None:
                    self._num_connections_to_process -= 1
                return CommandTransport(reader, writer)

            except ConnectionRefusedError as e:
                _logger.info('Could not connect to %s:%s: %s',
                             self._host, self._port, e)
                await asyncio.sleep(RECONNECT_DELAY, loop=self._loop)
        return None

    async def loop(self):
        while self._running:
            transport = await self._connect()
            if transport is None:
                break
            while self._running and not transport.at_eof():
                command = await transport.recv()
                if command is not None:
                    _logger.info('Received command: %s', command)
                    if self._on_command_callback is not None:
                        try:
                            self._on_command_callback(command)
                        except Exception as e:
                            _logger.exception('Error while running callback: %s', e)
            _logger.info('Server disconnected')

    def handle_interrupt(self):
        _logger.info('Interrupt received')
        self._running = False


def simple_client(host=DEFAULT_HOST, port=DEFAULT_PORT):
    loop = asyncio.get_event_loop()
    lantern = Lantern()
    num_connections_to_process = None
    client = LanternClient(host, port, loop,
                           num_connections_to_process,
                           lantern.on_command)

    loop.add_signal_handler(SIGTERM, client.handle_interrupt)
    loop.add_signal_handler(SIGINT, client.handle_interrupt)
    loop.run_until_complete(client.loop())
    loop.close()
    _logger.info('simple_client done')


class LanternRunner:
    """
    Helper class that when fed to google-fire makes it a nice CLI.
    """
    def server(self, host=DEFAULT_HOST, port=DEFAULT_PORT):
        simple_server(host, port)

    def client(self, host=DEFAULT_HOST, port=DEFAULT_PORT):
        simple_client(host, port)


def main():
    fire.Fire(LanternRunner)


if __name__ == '__main__':
    main()
