import argparse

from core.delete_log import delete_log
from core.server.alibaba.check_machine import check_machine
from scheduler import scheduler


start = {
    "default": scheduler.run_forever,
    "check_machine": check_machine,
    "delete_log": delete_log
}


def main():
    parser = argparse.ArgumentParser(description='Program start args')
    parser.add_argument('--args', help="Program start args", default="default")
    args = parser.parse_args()
    args = args.args
    start[args]()


if __name__ == "__main__":
    main()
