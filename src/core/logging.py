import logging
import os
import re

_ASSIGN_RE = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|token|secret|signature|password|key)=([^&\s\"'<>]+)"
)
_BEARER_RE = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-]+")

_SECRET_NAME_RE = re.compile(
    r"(?i)(secret|token|password|passwd|api[_-]?key|access[_-]?key|private[_-]?key|credential|auth)"
)
_NON_SECRET_SUFFIXES = (
    "_FILE", "_PATH", "_URL", "_URI", "_ID", "_USER", "_USERNAME",
    "_NAME", "_HOST", "_PORT", "_PUBLIC", "_ENABLED", "_REGION",
)
_MIN_SECRET_LEN = 8  # short values recur as ordinary text

_REDACTED = "***"
_FILTERED_FLAG = "_secret_masked"
_EXC_FORMATTER = logging.Formatter()


def _discover_secret_values() -> tuple[str, ...]:
    values: list[str] = []
    for name, value in os.environ.items():
        if not value or len(value) < _MIN_SECRET_LEN:
            continue
        upper = name.upper()
        if any(upper.endswith(suffix) for suffix in _NON_SECRET_SUFFIXES):
            continue
        if _SECRET_NAME_RE.search(name):
            values.append(value)
    return tuple(values)


class SecretMaskingFilter(logging.Filter):
    def __init__(self, secret_values: tuple[str, ...] = ()) -> None:
        super().__init__()
        secrets = sorted({s for s in secret_values if len(s) >= _MIN_SECRET_LEN}, key=len, reverse=True)
        self._replacer = re.compile("|".join(re.escape(s) for s in secrets)) if secrets else None

    def redact(self, text: str) -> str:
        text = _ASSIGN_RE.sub(rf"\1={_REDACTED}", text)
        text = _BEARER_RE.sub(rf"\1{_REDACTED}", text)
        if self._replacer is not None:
            text = self._replacer.sub(_REDACTED, text)
        return text

    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, _FILTERED_FLAG, False):
            return True

        original = record.getMessage()
        redacted = self.redact(original)
        if redacted != original:
            record.msg = redacted
            record.args = ()

        if record.exc_info:
            record.exc_text = self.redact(_EXC_FORMATTER.formatException(record.exc_info))
            record.exc_info = None
        elif record.exc_text:
            record.exc_text = self.redact(record.exc_text)

        if record.stack_info:
            record.stack_info = self.redact(record.stack_info)

        setattr(record, _FILTERED_FLAG, True)
        return True


def install_secret_masking() -> None:
    """Attach masking to root handlers and the httpx logger. Idempotent."""
    targets = [*logging.getLogger().handlers, logging.getLogger("httpx")]
    for target in targets:
        if not any(isinstance(f, SecretMaskingFilter) for f in target.filters):
            target.addFilter(SecretMaskingFilter(_discover_secret_values()))


def get_logger(name: str) -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    install_secret_masking()
    return logging.getLogger(name)
