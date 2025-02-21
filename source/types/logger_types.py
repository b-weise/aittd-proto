from dataclasses import dataclass


@dataclass(frozen=True)
class TermLoggerType:
    SHORT = 0
    LONG = 1
    ALL = [SHORT, LONG]
