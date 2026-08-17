"""Entry point for child worker processes: ``python -m flexiq.prefork``."""

from flexiq.prefork.child import main

if __name__ == "__main__":
    main()
