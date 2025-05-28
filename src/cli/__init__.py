import click

from .commands import clear_data, clear_schema, clone_data, clone_schema, print_schema


@click.group
def cli() -> None:
    pass


cli.add_command(print_schema)
cli.add_command(clone_schema)
cli.add_command(clone_data)
cli.add_command(clear_data)
cli.add_command(clear_schema)


__all__ = ["cli"]
