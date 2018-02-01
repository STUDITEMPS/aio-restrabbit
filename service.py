import argparse
import logging
import sys
import time

import aiorestrabbit.auth
import aiorestrabbit.config
import aiorestrabbit.client
import aiorestrabbit.server


def setup_verbose_console_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch.setFormatter(formatter)
    root.addHandler(ch)


def start(args):
    aiorestrabbit.client.StartStopService.run()
    conf = aiorestrabbit.config.Config()
    if conf.get('DEBUG') or args.verbose:
        setup_verbose_console_logging()
    server = aiorestrabbit.server.AioWebServer(conf)
    server.run_app()


def stop(args):
    if not aiorestrabbit.client.StartStopService.is_running():
        return
    aiorestrabbit.client.StartStopService.stop()
    while aiorestrabbit.client.StartStopService.is_running():
        time.sleep(0.1)


def restart(args):
    stop(args)
    start(args)


def clean(args):
    aiorestrabbit.client.StartStopService.cleanup()


if __name__ == "__main__":
    action_mapper = {
        'start': start,
        'stop': stop,
        'restart': restart,
        'clean': clean
    }
    parser = argparse.ArgumentParser(description='AIORestRabbit Service')
    parser.add_argument('-v', '--verbose', action='store_const', const=True)
    parser.add_argument(
        'action',
        nargs='?',
        default="restart",
        choices=action_mapper.keys()
    )
    args = parser.parse_args()
    action_mapper[args.action](args)
