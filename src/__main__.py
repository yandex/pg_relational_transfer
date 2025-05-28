from src.cli import cli
from src.utils.logs import setup_logging


def main():
    setup_logging()
    cli()


if __name__ == "__main__":
    main()
