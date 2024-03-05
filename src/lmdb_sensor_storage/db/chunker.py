from datetime import datetime
from typing import TypeVar, Sequence

import numpy as np

_T = TypeVar('_T')


def timestamp_chunker_left(x: Sequence[datetime]) -> Sequence[datetime]:
    return [x[0], ]


def timestamp_chunker_right(x: Sequence[datetime]) -> Sequence[datetime]:
    return [x[-1], ]


def timestamp_chunker_center(x: Sequence[datetime]) -> Sequence[datetime]:
    if len(x) > 1:
        return [x[0] + (x[-1] - x[0]) / 2, ]
    else:
        return [x[0], ]


def value_chunker_min(x: Sequence[_T]) -> Sequence[_T]:
    return [min(*x), ]


def value_chunker_max(x: Sequence[_T]) -> Sequence[_T]:
    return [max(*x), ]


def value_chunker_median(x: Sequence[_T]) -> Sequence[_T]:
    return [np.median(x), ]


def value_chunker_mean(x: Sequence[_T]) -> Sequence[_T]:
    return [np.mean(x, axis=0).tolist(), ]


def value_chunker_minmeanmax(x: Sequence[_T]) -> Sequence[_T]:
    if len(x) > 1:
        return np.min(x, axis=0).tolist(), np.mean(x, axis=0).tolist(), np.max(x, axis=0).tolist()
    else:
        return x, x, x


def timestamp_chunker_minmeanmax(x: Sequence[_T]) -> Sequence[_T]:
    if len(x) > 1:
        return [x[0], x[0] + (x[-1] - x[0]) / 2, x[-1]]
    else:
        return x, x, x


def non_chunker(x: Sequence[_T]) -> Sequence[_T]:
    return x
