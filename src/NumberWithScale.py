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

    @property
    def float(self):
        return float(self.v) / 10**self.scale
