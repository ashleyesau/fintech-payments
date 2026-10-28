"""
errors.py: what stops a Bronze run.

Every one of these means "stop with nothing committed to Bronze". The landed objects are never
touched by this package, so a stop never loses anything.
"""


class BronzeError(Exception):
    """Base class: the Bronze command must stop."""


class ConfigError(BronzeError):
    """A setting or credential the command needs is missing or unusable (exit code 1)."""
