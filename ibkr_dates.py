"""Gemeinsame Normalisierung fuer IBKR-Flex-Datumsfelder.

IBKR verwendet fuer neue Flex Queries standardmaessig ``yyyyMMdd``,
``HHmmss`` und ein Semikolon als Date/Time-Separator. Aeltere bzw. bewusst
anders konfigurierte Queries liefern haeufig ISO-Werte. Intern rechnen und
matchen wir ausschliesslich mit kanonischen ISO-Strings, damit Sortierung,
Jahresfilter und Same-Day-Matches unabhaengig von der Query-Konfiguration
bleiben.
"""

import re
from datetime import date, datetime, timedelta


DATE_FIELDS = frozenset({
    'date', 'reportDate', 'tradeDate', 'fromDate', 'toDate', 'expiry',
})

DATETIME_FIELDS = frozenset({
    'dateTime', 'openDateTime',
})

_DATE_TIME_RE = re.compile(
    r'^(?P<date>\d{4}-\d{2}-\d{2}|\d{8})'
    r'(?:(?:[;,T]|\s+)?(?P<time>\d{2}:\d{2}:\d{2}|\d{6}))?$'
)


def _parse_parts(value):
    """Return ``(date, time_or_none)`` for supported IBKR values."""
    if isinstance(value, datetime):
        return value.date(), value.time().replace(microsecond=0)
    if isinstance(value, date):
        return value, None
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None
    match = _DATE_TIME_RE.fullmatch(raw)
    if match is None:
        return None

    raw_date = match.group('date')
    raw_time = match.group('time')
    try:
        parsed_date = datetime.strptime(
            raw_date, '%Y-%m-%d' if '-' in raw_date else '%Y%m%d'
        ).date()
        if raw_time is None:
            return parsed_date, None
        parsed_time = datetime.strptime(
            raw_time, '%H:%M:%S' if ':' in raw_time else '%H%M%S'
        ).time()
        return parsed_date, parsed_time
    except ValueError:
        return None


def parse_ibkr_date(value):
    """Parse an IBKR date or timestamp, returning ``date`` or ``None``."""
    parsed = _parse_parts(value)
    return parsed[0] if parsed else None


def is_supported_ibkr_date(value):
    """Whether a non-empty value uses a supported IBKR date/time format."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return True
    return _parse_parts(value) is not None


def normalize_ibkr_date(value):
    """Normalize a date-like IBKR value to ``YYYY-MM-DD``.

    Unsupported non-empty values are preserved so the strict CSV loader can
    report them instead of silently discarding the affected booking.
    """
    parsed = _parse_parts(value)
    if parsed:
        return parsed[0].isoformat()
    return value.strip() if isinstance(value, str) else value


def normalize_ibkr_datetime(value):
    """Normalize an IBKR timestamp to ``YYYY-MM-DD HH:MM:SS``."""
    parsed = _parse_parts(value)
    if parsed:
        parsed_date, parsed_time = parsed
        if parsed_time is None:
            return parsed_date.isoformat()
        return f'{parsed_date.isoformat()} {parsed_time.strftime("%H:%M:%S")}'
    return value.strip() if isinstance(value, str) else value


def normalize_ibkr_row(row):
    """Return a copy with known IBKR date fields normalized."""
    normalized = dict(row)
    for field in DATE_FIELDS:
        if field in normalized:
            normalized[field] = normalize_ibkr_date(normalized[field])
    for field in DATETIME_FIELDS:
        if field in normalized:
            normalized[field] = normalize_ibkr_datetime(normalized[field])
    return normalized


def unsupported_date_fields(row):
    """Return non-empty known date fields with unsupported values."""
    return [
        field for field in DATE_FIELDS | DATETIME_FIELDS
        if row.get(field) not in (None, '')
        and not is_supported_ibkr_date(row.get(field))
    ]


def _no_trading_day_between(after, before):
    """Whether only weekends and 1 January lie strictly between two dates."""
    day = after + timedelta(days=1)
    while day < before:
        if day.weekday() < 5 and (day.month, day.day) != (1, 1):
            return False
        day += timedelta(days=1)
    return True


def coverage_start(periods):
    """Start of the gapless coverage that ends with the latest period.

    ``periods`` are ``(fromDate, toDate)`` pairs of the statements of one
    account. A gap of only weekends and 1 January does not interrupt the
    coverage: IBKR statements often start on the first trading day.
    Returns ``''`` without a complete period.
    """
    parsed = sorted(
        (start, end) for start, end in (
            (parse_ibkr_date(f), parse_ibkr_date(t)) for f, t in periods)
        if start and end
    )
    if not parsed:
        return ''
    blocks = [list(parsed[0])]
    for start, end in parsed[1:]:
        last = blocks[-1]
        if start <= last[1] or _no_trading_day_between(last[1], start):
            last[1] = max(last[1], end)
        else:
            blocks.append([start, end])
    return max(blocks, key=lambda block: block[1])[0].isoformat()


def covers_year_start(coverage_from, year):
    """Whether a coverage starting at ``coverage_from`` includes every
    trading day of ``year`` from 1 January on. Unknown start: False."""
    start = parse_ibkr_date(coverage_from)
    if start is None:
        return False
    return (start <= date(year, 1, 1)
            or _no_trading_day_between(date(year - 1, 12, 31), start))
