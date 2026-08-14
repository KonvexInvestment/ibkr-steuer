"""Gemeinsame Normalisierung fuer IBKR-Flex-Datumsfelder.

IBKR verwendet fuer neue Flex Queries standardmaessig ``yyyyMMdd``,
``HHmmss`` und ein Semikolon als Date/Time-Separator. Aeltere bzw. bewusst
anders konfigurierte Queries liefern haeufig ISO-Werte. Intern rechnen und
matchen wir ausschliesslich mit kanonischen ISO-Strings, damit Sortierung,
Jahresfilter und Same-Day-Matches unabhaengig von der Query-Konfiguration
bleiben.
"""

import re
from datetime import date, datetime


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
