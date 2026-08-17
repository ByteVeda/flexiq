"""FileHandler — proxy for open file handles."""

from __future__ import annotations

import io
import os
import pathlib
import sys
from typing import Any, ClassVar

from flexiq.exceptions import ProxyReconstructionError
from flexiq.proxies.schema import FieldSpec


class FileHandler:
    """Deconstructs/reconstructs open file handles."""

    name = "file"
    version = 1
    handled_types: tuple[type, ...] = (
        io.TextIOWrapper,
        io.BufferedReader,
        io.BufferedWriter,
        io.FileIO,
    )
    schema: ClassVar[dict[str, FieldSpec]] = {
        "path": FieldSpec(str),
        "mode": FieldSpec(str),
        "encoding": FieldSpec((str, type(None)), required=False),
        "position": FieldSpec(int, required=False),
    }

    _STDIO_FILES: ClassVar[set[int]] = {id(sys.stdin), id(sys.stdout), id(sys.stderr)}

    def __init__(self, path_allowlist: list[str] | None = None) -> None:
        self._path_allowlist = [str(pathlib.Path(p).resolve()) for p in (path_allowlist or [])]

    def detect(self, obj: Any) -> bool:
        if not isinstance(obj, self.handled_types):
            return False
        if getattr(obj, "closed", True):
            return False
        if id(obj) in self._STDIO_FILES:
            return False
        name = getattr(obj, "name", None)
        if not isinstance(name, str):
            return False
        return name not in ("<stdin>", "<stdout>", "<stderr>")

    def deconstruct(self, obj: Any) -> dict[str, Any]:
        return {
            "path": obj.name,
            "mode": obj.mode,
            "encoding": getattr(obj, "encoding", None),
            "position": obj.tell(),
        }

    def reconstruct(self, recipe: dict[str, Any], version: int) -> Any:
        path = recipe["path"]

        # Path allowlist enforcement. Resolve once, check containment by path
        # component (not string prefix — ``/var/data`` must not allow
        # ``/var/data_exfil``), then open the *resolved* path so a symlink
        # swapped in after the check can't redirect us outside the allowlist.
        open_path = path
        if self._path_allowlist:
            resolved = str(pathlib.Path(path).resolve())
            if not self._is_allowed(resolved):
                raise ProxyReconstructionError(f"File path '{path}' is not in the allowed paths")
            open_path = resolved

        kwargs: dict[str, Any] = {}
        if recipe.get("encoding") is not None:
            kwargs["encoding"] = recipe["encoding"]
        f = open(open_path, recipe["mode"], **kwargs)  # noqa: SIM115
        position = recipe.get("position", 0)
        if position and position > 0:
            f.seek(position)
        return f

    def _is_allowed(self, resolved: str) -> bool:
        """Whether a resolved path lies within an allowlisted directory.

        Containment is by path component via ``os.path.commonpath`` so
        ``/var/data`` does not admit ``/var/data_exfil``. Incomparable paths
        (e.g. different drives) fail closed.
        """
        for prefix in self._path_allowlist:
            if resolved == prefix:
                return True
            try:
                if os.path.commonpath([resolved, prefix]) == prefix:
                    return True
            except ValueError:
                continue
        return False

    def cleanup(self, obj: Any) -> None:
        if not obj.closed:
            obj.close()
