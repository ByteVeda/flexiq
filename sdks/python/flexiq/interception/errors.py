"""Structured error messages for argument interception."""

from __future__ import annotations

from flexiq.exceptions import SerializationError


class InterceptionError(SerializationError):
    """Raised when argument interception rejects one or more arguments."""

    def __init__(self, failures: list[ArgumentFailure]) -> None:
        self.failures = failures
        msg = self._format(failures)
        super().__init__(msg)

    @staticmethod
    def _format(failures: list[ArgumentFailure]) -> str:
        lines = [f"Cannot serialize {len(failures)} argument(s):\n"]
        for f in failures:
            lines.append(f"  {f.path} (type: {f.type_name})")
            lines.append(f"    {f.reason}")
            if f.suggestions:
                lines.append("    Suggestions:")
                for i, s in enumerate(f.suggestions, 1):
                    lines.append(f"      {i}. {s}")
            lines.append("")
        return "\n".join(lines)


class ArgumentFailure:
    """Details about a single rejected argument."""

    __slots__ = ("path", "reason", "suggestions", "type_name")

    def __init__(
        self,
        path: str,
        type_name: str,
        reason: str,
        suggestions: list[str] | None = None,
    ) -> None:
        self.path = path
        self.type_name = type_name
        self.reason = reason
        self.suggestions = suggestions or []
