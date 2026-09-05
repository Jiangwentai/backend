from datetime import date

MONTH_CODES = "FGHJKMNQUVXZ"


def month_code_to_month(code: str) -> int:
    if len(code) != 1 or code.upper() not in MONTH_CODES:
        raise ValueError("INVALID_MONTH_CODE")
    return MONTH_CODES.index(code.upper()) + 1


def month_to_month_code(month: int) -> str:
    if type(month) is not int or not 1 <= month <= 12:
        raise ValueError("INVALID_MONTH")
    return MONTH_CODES[month - 1]


def expand_yymm(yymm: str, *, as_of: date | None = None) -> str:
    """Default explicit 2000..2099 identity epoch; historical callers supply as_of.

    With as_of choose the unique year within +/-49 years. The 50-year tie
    is unresolved. No wall-clock dependency in this pure helper.
    """
    if len(yymm) != 4 or not yymm.isascii() or not yymm.isdigit():
        raise ValueError("INVALID_YYMM")
    month = int(yymm[2:])
    if not 1 <= month <= 12:
        raise ValueError("INVALID_MONTH")
    year = 2000 + int(yymm[:2])
    if as_of is not None:
        candidates = [y for y in range(as_of.year - 49, as_of.year + 50)
                      if 1 <= y <= 9999 and y % 100 == int(yymm[:2])]
        if len(candidates) != 1:
            raise ValueError("AMBIGUOUS_CENTURY")
        year = candidates[0]
    return f"{year:04d}{month:02d}"


def expand_czce_ymm(ymm: str, *, as_of: date | None) -> str:
    if as_of is None:
        raise ValueError("CZCE_AS_OF_REQUIRED")
    if len(ymm) != 3 or not ymm.isascii() or not ymm.isdigit() or not 1 <= int(ymm[1:]) <= 12:
        raise ValueError("INVALID_CZCE_YMM")
    # Deliberately narrow recognition window: as_of month -12 through +36.
    # Outside this window require a full year or an explicit dated mapping.
    anchor = as_of.year * 12 + as_of.month - 1
    candidates = [y for y in range(max(1, as_of.year - 1), min(9999, as_of.year + 3) + 1)
                  if y % 10 == int(ymm[0]) and -12 <= y * 12 + int(ymm[1:]) - 1 - anchor <= 36]
    if len(candidates) != 1:
        raise ValueError("AMBIGUOUS_CZCE_DECADE")
    return f"{candidates[0]:04d}{int(ymm[1:]):02d}"
