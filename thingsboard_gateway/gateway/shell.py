import cmd
import multiprocessing.managers
import signal
import sys
from threading import Thread
from time import sleep

from thingsboard_gateway.gateway.tb_gateway_service import TBGatewayService
from thingsboard_gateway.gateway.tb_gateway_service import GatewayManager


class ShellSyntaxError(Exception):
    pass


class Shell(cmd.Cmd, Thread):
    prompt = '(gateway) |> '

    def __init__(self, stdin=None, stdout=None):
        cmd.Cmd.__init__(self, stdin=stdin, stdout=stdout)
        Thread.__init__(self, name='Gateway Shell', daemon=False)

        self.manager = GatewayManager(address='/tmp/gateway', authkey=b'gateway')
        try:
            self.manager.connect()
            GatewayManager.register('get_gateway')
            self.gateway: TBGatewayService = self.manager.get_gateway()
        except (FileNotFoundError, AssertionError):
            self.stdout.write('Gateway is not running!\n')
            return

        self.connected = False
        self.stdout.write('Connecting to Gateway')
        while not self.connected:
            try:
                self.gateway.ping()
                self.stdout.write('\n')
                self.connected = True
                self.stdout.write('Connected to Gateway\n')
            except (multiprocessing.managers.RemoteError, KeyError, AttributeError):
                self.stdout.write('.')
                self.gateway = self.manager.get_gateway()
                self.manager.connect()
                sleep(1)

        self.interactive = sys.__stdin__.isatty()

        if hasattr(signal, 'SIGINT'):
            signal.signal(signal.SIGINT, self.interrupt)

        self.start()

    def run(self):
        self.cmdloop()

    @staticmethod
    def parser(arg: str, args):
        input_args = arg.strip().split(' ')
        opt, val = None, None
        try:
            (opt, val) = input_args
        except ValueError:
            opt = input_args[0]

        try:
            for keys, func in args.items():
                if opt in keys:
                    return func(val) if val else func()
        except Exception as _:
            raise ShellSyntaxError('Invalid syntax (see `help storage`)')

    def _print(self, *args):
        for arg in args:
            if isinstance(arg, list):
                self.stdout.write(','.join(arg))
            elif isinstance(arg, dict):
                for key, value in arg.items():
                    self.stdout.write(str(key).capitalize() + ': ' + str(value) + '\n')
            else:
                self.stdout.write(arg + '\n')

    def do_storage(self, arg):
        """Storage
        -n/--name:   name of storage
        -c/--count:  events in storage
        """
        args = {
            ('-n', '--name'): self.gateway.get_storage_name,
            ('-c', '--count'): self.gateway.get_storage_events_count
        }
        try:
            self._print(self.parser(arg, args))
        except ShellSyntaxError as e:
            self.stdout.write(e.__str__() + '\n')

    def do_connector(self, arg):
        """Connector
        -a/--available:        list of available connectors
        -s/--status <name>:    get connector status
        -n/--name <name>:      get connector name
        -c/--config <name>:
        """
        args = {
            ('-a', '--available'): self.gateway.get_available_connectors,
            ('-s', '--status'): self.gateway.get_connector_status,
            ('-n', '--name'): self.gateway.get_connector_name,
            ('-c', '--config'): self.gateway.get_connector_config
        }

        try:
            self._print(self.parser(arg, args))
        except ShellSyntaxError as e:
            self.stdout.write(e.__str__() + '\n')

    # ---
    @staticmethod
    def interrupt(_, __):
        sys.stderr.write("\nKeyboard interrupt trapped - use EOF to end\n")

    def precmd(self, line: str) -> str:
        return line.strip()

    def postcmd(self, stop: bool, line: str) -> bool:
        return stop

    def emptyline(self) -> bool:
        pass

    @staticmethod
    def do_exit(_):
        return -1

    def do_EOF(self, args):
        return self.do_exit(args)


if __name__ == '__main__':
    shell = Shell()
