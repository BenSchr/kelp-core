"""Output utilities for CLI commands.

Provides consistent, testable output functions to replace typer.echo and typer.secho.
Uses standard print() with optional ANSI color codes.
"""

import sys
from enum import StrEnum


class ColorCode(StrEnum):
    """ANSI color codes for terminal output."""

    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def _format_message(message: str, color: ColorCode | None = None, bold: bool = False) -> str:
    """Format a message with optional color and bold styling.

    Args:
        message: Message text.
        color: Optional color code (from ColorCode enum).
        bold: Whether to apply bold formatting.

    Returns:
        Formatted message with ANSI codes.
    """
    if not sys.stdout.isatty():
        return message

    codes = []
    if bold:
        codes.append(ColorCode.BOLD)
    if color:
        codes.append(color.value)
    if codes:
        return f"{''.join(codes)}{message}{ColorCode.RESET.value}"
    return message


def print_message(message: str) -> None:
    """Print a plain message.

    Args:
        message: Message to print.
    """
    print(message)


def print_success(message: str) -> None:
    """Print a success message in green.

    Args:
        message: Message to print.
    """
    print(_format_message(message, ColorCode.GREEN))


def print_warning(message: str) -> None:
    """Print a warning message in yellow.

    Args:
        message: Message to print.
    """
    print(_format_message(message, ColorCode.YELLOW))


def print_error(message: str) -> None:
    """Print an error message in red to stderr.

    Args:
        message: Message to print.
    """
    print(_format_message(message, ColorCode.RED), file=sys.stderr)


def print_info(message: str) -> None:
    """Print an info message in blue.

    Args:
        message: Message to print.
    """
    print(_format_message(message, ColorCode.BLUE))
