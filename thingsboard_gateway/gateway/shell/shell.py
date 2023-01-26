import cmd
import sys
import signal
import multiprocessing.managers
from re import findall
from threading import Thread
from time import sleep

from thingsboard_gateway.gateway.shell.proxy import AutoProxy
from thingsboard_gateway.gateway.tb_gateway_service import GatewayManager


class ShellSyntaxError(Exception):
    def __str__(self):
        return 'Invalid syntax'


class Shell(cmd.Cmd, Thread):
    prompt = '(gateway) |> '

    def __init__(self, stdin=None, stdout=None):
        cmd.Cmd.__init__(self, stdin=stdin, stdout=stdout)
        Thread.__init__(self, name='Gateway Shell', daemon=False)

        self.stdout.write('Gateway Shell\n')
        self.stdout.write('=============\n')

        self.gateway_manager, self.gateway = self.create_manager()
        if self.gateway_manager is None or self.gateway is None:
            self.stdout.write('Gateway is not running!\n')
            return

        self.command_config = {
            'storage': [
                {
                    'arg': ('-n', '--name'),
                    'func': self.gateway.get_storage_name
                },
                {
                    'arg': ('-c', '--count'),
                    'func': self.gateway.get_storage_events_count
                }
            ],
            'connector': [
                {
                    'arg': ('-l', '--list'),
                    'func': self.gateway.get_available_connectors
                },
                {
                    'arg': ('-s', '--status'),
                    'val_required': True,
                    'func': self.gateway.get_connector_status
                },
                {
                    'arg': ('-c', '--config'),
                    'val_required': True,
                    'func': self.gateway.get_connector_config
                }
            ],
            'gateway': [
                {
                    'arg': ('-s', '--status'),
                    'func': self.gateway.get_status
                }
            ]
        }

        self.interactive = sys.__stdin__.isatty()

        if hasattr(signal, 'SIGINT'):
            signal.signal(signal.SIGINT, self.interrupt)

        self.start()

    def create_manager(self, timeout=5):
        elapsed_time = 0
        gateway_manager = None
        gateway = None
        self.stdout.write('Establish connection...\n')
        while elapsed_time <= timeout:
            try:
                gateway_manager = GatewayManager(address='/tmp/gateway', authkey=b'gateway')
                gateway_manager.connect()
                GatewayManager.register('get_gateway', proxytype=AutoProxy)
                GatewayManager.register('gateway')
                gateway = self.connect(gateway_manager)
            except (FileNotFoundError, AssertionError):
                sleep(1)
                continue
            else:
                break
            finally:
                elapsed_time += 1

        return gateway_manager, gateway

    def connect(self, manager, timeout=5):
        elapsed_time = 0
        gateway = None
        self.connected = False
        self.stdout.write('Connecting to Gateway')
        while not self.connected and elapsed_time <= timeout:
            try:
                gateway = manager.get_gateway()
                gateway.ping()
                self.connected = True
            except (multiprocessing.managers.RemoteError, KeyError, AttributeError, BrokenPipeError):
                self.stdout.write('.')
                gateway = manager.get_gateway()
                self.connected = True
                sleep(1)
            except FileNotFoundError:
                self.stdout.write('.')
                sleep(1)
                continue
            finally:
                elapsed_time += 1

        if not self.connected:
            raise RuntimeError('Timeout error')

        self.stdout.write('\n')
        return gateway

    def run(self):
        self.cmdloop()

    @staticmethod
    def parser(arg, args):
        input_args = findall(r'(-{1,2}[A-Za-z]+)[?^\s]?([A-Za-z\d\s]*)', arg.strip())

        for input_arg in input_args:
            for arg in args:
                opt, val = None, None
                try:
                    (opt, val) = input_arg
                except ValueError:
                    opt = input_arg[0]

                if opt in arg['arg']:
                    if arg.get('val_required') and not val:
                        continue

                    return arg['func'](val.strip()) if val else arg['func']()

        raise ShellSyntaxError()

    def _print(self, *args):
        for arg in args:
            if isinstance(arg, list):
                self.stdout.write(','.join(arg) + '\n')
            elif isinstance(arg, dict):
                for key, value in arg.items():
                    self.stdout.write(str(key).capitalize() + ': ' + str(value) + '\n')
            else:
                self.stdout.write(str(arg) + '\n')

    def wrapper(self, arg, label, config):
        try:
            self._print(self.parser(arg, config))
        except ShellSyntaxError as e:
            self.stdout.write(e.__str__() + ' (see `help ' + label + '`)\n')
            self.do_help(label)
        except (FileNotFoundError, AttributeError, BrokenPipeError, multiprocessing.managers.RemoteError):
            self.stdout.write('Connection is lost...\n')
            self.gateway_manager, self.gateway = self.create_manager()

    # ---
    def do_gateway(self, arg):
        """Gateway
        -s/--status: get gateway status
        """
        self.wrapper(arg, 'gateway', self.command_config['gateway'])

    def do_storage(self, arg):
        """Storage
        -n/--name:   name of storage
        -c/--count:  events in storage
        """
        self.wrapper(arg, 'storage', self.command_config['storage'])

    def do_connector(self, arg):
        """Connector
        -l/--list:             list of available connectors
        -s/--status <name>:    get connector status
        -c/--config <name>:    get connector config
        """
        self.wrapper(arg, 'connector', self.command_config['connector'])

    # ---
    @staticmethod
    def interrupt(_, __):
        sys.stderr.write("\nKeyboard interrupt trapped - use Ctrl+d or Cmd+d to end\n")

    def precmd(self, line: str) -> str:
        return line.strip()

    def postcmd(self, stop: bool, line: str) -> bool:
        return stop

    def emptyline(self) -> bool:
        pass

    @staticmethod
    def do_exit(_):
        """Exit"""
        return -1

    def do_EOF(self, args):
        """EOF"""
        return self.do_exit(args)


def main():
    Shell()


if __name__ == '__main__':
    shell = Shell()
