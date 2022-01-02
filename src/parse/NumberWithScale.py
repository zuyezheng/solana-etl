from __future__ import annotations

from dataclasses import dataclass


@dataclass
class NumberWithScale:
    """
    Numbers with decimal part stored as part of the int with a given scale.

    @author zuyezheng
    """

    v: int
    scale: int

    def __abs__(self) -> NumberWithScale:
        return NumberWithScale(abs(self.v), self.scale)

    def __add__(self, other: NumberWithScale) -> NumberWithScale:
        if isinstance(other, NumberWithScale) and self.scale == other.scale:
            return NumberWithScale(self.v + other.v, self.scale)

        return NotImplemented

    def __sub__(self, other: NumberWithScale) -> NumberWithScale:
        if isinstance(other, NumberWithScale) and self.scale == other.scale:
            return NumberWithScale(self.v - other.v, self.scale)

        return NotImplemented

    def __lt__(self, other: NumberWithScale) -> bool:
        if isinstance(other, NumberWithScale) and self.scale == other.scale:
            return self.v < other.v

        return NotImplemented

    def __le__(self, other: NumberWithScale) -> bool:
        if isinstance(other, NumberWithScale) and self.scale == other.scale:
            return self.v <= other.v

        return NotImplemented

    def __gt__(self, other: NumberWithScale) -> bool:
        if isinstance(other, NumberWithScale) and self.scale == other.scale:
            return self.v > other.v

        return NotImplemented

    def __ge__(self, other: NumberWithScale) -> bool:
        if isinstance(other, NumberWithScale) and self.scale == other.scale:
            return self.v >= other.v

        return NotImplemented

    def zero(self):
        return NumberWithScale(0, self.scale)

    @property
    def float(self):
        return float(self.v) / 10**self.scale
