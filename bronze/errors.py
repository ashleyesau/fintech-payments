"""
errors.py: what stops a Bronze run.

Every one of these means "stop with nothing committed to Bronze". The landed objects are never
touched by this package, so a stop never loses anything.
"""


class BronzeError(Exception):
    """Base class: the Bronze command must stop."""


class ConfigError(BronzeError):
    """A setting or credential the command needs is missing or unusable (exit code 1)."""


class LandingError(BronzeError):
    """The landing prefix holds something the CDC contract does not allow, or a request would leave the
    prefix: an object name that does not follow contract section 2, a listed key outside the run's
    prefix, a read of a key outside it (exit code 2)."""
