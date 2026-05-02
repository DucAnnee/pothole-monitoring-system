from __future__ import annotations

from . import format_available_modules


def main() -> None:
    """Print package-level command discovery."""
    print(format_available_modules())


if __name__ == "__main__":
    main()
