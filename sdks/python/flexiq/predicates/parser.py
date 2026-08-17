"""Tiny recursive-descent parser for the predicate string DSL.

Grammar::

    expr     := or_expr
    or_expr  := and_expr ("|" and_expr)*
    and_expr := unary ("&" unary)*
    unary    := "!" unary | atom
    atom     := IDENT "(" kwargs? ")" | "(" expr ")"
    kwargs   := kwarg ("," kwarg)*
    kwarg    := IDENT "=" literal
    literal  := STRING | NUMBER | "true" | "false" | "null" | "[" list_items? "]"

Examples::

    is_business_hours(tz="US/Pacific") & !queue_paused()
    feature_flag(name="billing") | payload_matches(path="kwargs.tenant", expected="acme")

The parser builds the same :class:`~flexiq.predicates.core.Predicate`
AST that :meth:`Predicate.from_dict` produces, so it round-trips with
both JSON and Python-operator forms.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from flexiq.predicates.core import Predicate
from flexiq.predicates.registry import PredicateValidationError, default_registry

# -- Token model ---------------------------------------------------------


class _TokType:
    IDENT = "IDENT"
    STRING = "STRING"
    NUMBER = "NUMBER"
    BOOL = "BOOL"
    NULL = "NULL"
    LPAREN = "LPAREN"
    RPAREN = "RPAREN"
    LBRACKET = "LBRACKET"
    RBRACKET = "RBRACKET"
    EQ = "EQ"
    COMMA = "COMMA"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    EOF = "EOF"


@dataclass(frozen=True)
class _Token:
    kind: str
    value: Any
    pos: int


# -- Lexer ---------------------------------------------------------------


_KEYWORDS = {"true": True, "false": False, "null": None}


def _tokenize(source: str) -> list[_Token]:
    tokens: list[_Token] = []
    i = 0
    n = len(source)
    while i < n:
        ch = source[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "(":
            tokens.append(_Token(_TokType.LPAREN, ch, i))
            i += 1
            continue
        if ch == ")":
            tokens.append(_Token(_TokType.RPAREN, ch, i))
            i += 1
            continue
        if ch == "[":
            tokens.append(_Token(_TokType.LBRACKET, ch, i))
            i += 1
            continue
        if ch == "]":
            tokens.append(_Token(_TokType.RBRACKET, ch, i))
            i += 1
            continue
        if ch == ",":
            tokens.append(_Token(_TokType.COMMA, ch, i))
            i += 1
            continue
        if ch == "=":
            tokens.append(_Token(_TokType.EQ, ch, i))
            i += 1
            continue
        if ch == "&":
            tokens.append(_Token(_TokType.AND, ch, i))
            i += 1
            continue
        if ch == "|":
            tokens.append(_Token(_TokType.OR, ch, i))
            i += 1
            continue
        if ch == "!":
            tokens.append(_Token(_TokType.NOT, ch, i))
            i += 1
            continue
        if ch == '"' or ch == "'":
            string_value, j = _lex_string(source, i)
            tokens.append(_Token(_TokType.STRING, string_value, i))
            i = j
            continue
        if ch.isdigit() or (ch == "-" and i + 1 < n and source[i + 1].isdigit()):
            number_value, j = _lex_number(source, i)
            tokens.append(_Token(_TokType.NUMBER, number_value, i))
            i = j
            continue
        if ch.isalpha() or ch == "_":
            value, j = _lex_ident(source, i)
            if value in _KEYWORDS:
                kind = _TokType.BOOL if isinstance(_KEYWORDS[value], bool) else _TokType.NULL
                tokens.append(_Token(kind, _KEYWORDS[value], i))
            elif value == "and":
                tokens.append(_Token(_TokType.AND, "and", i))
            elif value == "or":
                tokens.append(_Token(_TokType.OR, "or", i))
            elif value == "not":
                tokens.append(_Token(_TokType.NOT, "not", i))
            else:
                tokens.append(_Token(_TokType.IDENT, value, i))
            i = j
            continue
        raise PredicateValidationError(f"unexpected character {ch!r} at position {i}")
    tokens.append(_Token(_TokType.EOF, None, n))
    return tokens


def _lex_string(source: str, start: int) -> tuple[str, int]:
    quote = source[start]
    out: list[str] = []
    i = start + 1
    n = len(source)
    while i < n:
        ch = source[i]
        if ch == "\\" and i + 1 < n:
            nxt = source[i + 1]
            escapes = {"n": "\n", "t": "\t", "r": "\r", "\\": "\\", '"': '"', "'": "'"}
            out.append(escapes.get(nxt, nxt))
            i += 2
            continue
        if ch == quote:
            return "".join(out), i + 1
        out.append(ch)
        i += 1
    raise PredicateValidationError(f"unterminated string starting at position {start}")


def _lex_number(source: str, start: int) -> tuple[int | float, int]:
    i = start
    n = len(source)
    if source[i] == "-":
        i += 1
    saw_dot = False
    saw_exp = False
    while i < n:
        ch = source[i]
        if ch.isdigit():
            i += 1
        elif ch == "." and not saw_dot and not saw_exp:
            saw_dot = True
            i += 1
        elif ch in ("e", "E") and not saw_exp:
            saw_exp = True
            saw_dot = True  # forbid more dots after exp
            i += 1
            if i < n and source[i] in ("+", "-"):
                i += 1
        else:
            break
    text = source[start:i]
    if saw_dot or saw_exp:
        return float(text), i
    return int(text), i


def _lex_ident(source: str, start: int) -> tuple[str, int]:
    i = start
    n = len(source)
    while i < n and (source[i].isalnum() or source[i] == "_"):
        i += 1
    return source[start:i], i


# -- Parser --------------------------------------------------------------


class _Parser:
    __slots__ = ("_pos", "_tokens")

    def __init__(self, tokens: list[_Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    def parse(self) -> Predicate:
        node = self._or()
        if self._peek().kind != _TokType.EOF:
            tok = self._peek()
            raise PredicateValidationError(f"unexpected token {tok.value!r} at position {tok.pos}")
        return node

    def _peek(self) -> _Token:
        return self._tokens[self._pos]

    def _eat(self, kind: str) -> _Token:
        tok = self._peek()
        if tok.kind != kind:
            raise PredicateValidationError(
                f"expected {kind}, got {tok.kind} ({tok.value!r}) at position {tok.pos}"
            )
        self._pos += 1
        return tok

    def _consume(self, kind: str) -> bool:
        if self._peek().kind == kind:
            self._pos += 1
            return True
        return False

    def _or(self) -> Predicate:
        left = self._and()
        while self._consume(_TokType.OR):
            right = self._and()
            left = left | right
        return left

    def _and(self) -> Predicate:
        left = self._unary()
        while self._consume(_TokType.AND):
            right = self._unary()
            left = left & right
        return left

    def _unary(self) -> Predicate:
        if self._consume(_TokType.NOT):
            return ~self._unary()
        return self._atom()

    def _atom(self) -> Predicate:
        tok = self._peek()
        if tok.kind == _TokType.LPAREN:
            self._eat(_TokType.LPAREN)
            inner = self._or()
            self._eat(_TokType.RPAREN)
            return inner
        if tok.kind == _TokType.IDENT:
            return self._call()
        raise PredicateValidationError(
            f"expected predicate atom, got {tok.kind} ({tok.value!r}) at position {tok.pos}"
        )

    def _call(self) -> Predicate:
        ident = self._eat(_TokType.IDENT)
        self._eat(_TokType.LPAREN)
        kwargs: dict[str, Any] = {}
        if not self._consume(_TokType.RPAREN):
            while True:
                name = self._eat(_TokType.IDENT).value
                self._eat(_TokType.EQ)
                kwargs[name] = self._literal()
                if not self._consume(_TokType.COMMA):
                    break
            self._eat(_TokType.RPAREN)
        op_name = ident.value
        target = default_registry().lookup(op_name)
        try:
            return target._from_kwargs(kwargs)
        except (TypeError, ValueError, PredicateValidationError) as exc:
            raise PredicateValidationError(
                f"cannot construct {op_name!r} from {kwargs!r}: {exc}"
            ) from exc

    def _literal(self) -> Any:
        tok = self._peek()
        if tok.kind in (_TokType.STRING, _TokType.NUMBER, _TokType.BOOL, _TokType.NULL):
            self._pos += 1
            return tok.value
        if tok.kind == _TokType.LBRACKET:
            self._eat(_TokType.LBRACKET)
            items: list[Any] = []
            if not self._consume(_TokType.RBRACKET):
                while True:
                    items.append(self._literal())
                    if not self._consume(_TokType.COMMA):
                        break
                self._eat(_TokType.RBRACKET)
            return items
        raise PredicateValidationError(
            f"expected literal, got {tok.kind} ({tok.value!r}) at position {tok.pos}"
        )


# -- Public API ----------------------------------------------------------


def parse(source: str) -> Predicate:
    """Parse a predicate string into a :class:`Predicate` AST.

    Raises :class:`~flexiq.predicates.PredicateValidationError` on
    malformed input or unknown op names.
    """
    if not isinstance(source, str):
        raise PredicateValidationError(f"parse() expected str, got {type(source).__name__}")
    tokens = _tokenize(source)
    return _Parser(tokens).parse()


def format_predicate(node: Predicate) -> str:
    """Render a :class:`Predicate` in the string DSL.

    Convenience wrapper around :meth:`Predicate.format`. The output is
    stable and ``parse(format_predicate(p))`` rebuilds an equivalent AST.
    """
    return node.format()
