from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

ESTIMATOR_NAME = "offline_estimator"


@dataclass(frozen=True)
class TokenCount:
    count: int
    tokenizer: str
    tokenizer_available: bool
    token_count_source: str
    warnings: tuple[dict[str, str], ...] = ()

    def metadata(self) -> dict[str, Any]:
        return {
            "tokenizer": self.tokenizer,
            "tokenizer_available": self.tokenizer_available,
            "token_count_source": self.token_count_source,
            "warnings": list(self.warnings),
        }


class TokenCounter:
    def __init__(self, mode: str = "estimate", target_tokenizer: str = ""):
        normalized_mode = (mode or "estimate").strip().lower()
        if normalized_mode not in {"estimate", "target"}:
            normalized_mode = "estimate"
        self.mode = normalized_mode
        self.target_tokenizer = (target_tokenizer or "cl100k_base").strip()
        self._target_encoding: Any | None = None
        self._target_warning: dict[str, str] | None = None
        if self.mode == "target":
            self._load_target_tokenizer()

    def count(self, text_or_value: Any) -> TokenCount:
        text = _to_text(text_or_value)
        if self._target_encoding is not None:
            try:
                token_count = max(1, len(self._target_encoding.encode(text)))
            except Exception as exc:
                return TokenCount(
                    count=_estimate(text),
                    tokenizer=self.target_tokenizer,
                    tokenizer_available=True,
                    token_count_source="estimate",
                    warnings=(
                        {
                            "code": "target_tokenizer_encode_failed",
                            "message": f"target tokenizer encode failed: {type(exc).__name__}",
                        },
                    ),
                )
            return TokenCount(
                count=token_count,
                tokenizer=self.target_tokenizer,
                tokenizer_available=True,
                token_count_source="target",
            )
        warnings = ()
        if self.mode == "target":
            warnings = (
                self._target_warning
                or {
                    "code": "target_tokenizer_unavailable",
                    "message": "target tokenizer unavailable; using offline estimate",
                },
            )
        return TokenCount(
            count=_estimate(text),
            tokenizer=self.target_tokenizer if self.mode == "target" else ESTIMATOR_NAME,
            tokenizer_available=self.mode != "target",
            token_count_source="estimate",
            warnings=warnings,
        )

    def metadata(self) -> dict[str, Any]:
        return self.count("").metadata()

    def cache_key_metadata(self) -> dict[str, Any]:
        metadata = self.metadata()
        return {
            "tokenizer": metadata["tokenizer"],
            "tokenizer_available": metadata["tokenizer_available"],
            "token_count_source": metadata["token_count_source"],
        }

    def _load_target_tokenizer(self) -> None:
        try:
            import tiktoken  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover - depends on optional package
            self._target_warning = {
                "code": "target_tokenizer_unavailable",
                "message": f"target tokenizer unavailable: {type(exc).__name__}",
            }
            return
        try:
            self._target_encoding = tiktoken.get_encoding(self.target_tokenizer)
        except Exception as exc:  # pragma: no cover - depends on optional package
            self._target_warning = {
                "code": "target_tokenizer_unavailable",
                "message": f"target tokenizer unavailable: {type(exc).__name__}",
            }


def estimate_tokens(text_or_value: Any) -> int:
    return TokenCounter().count(text_or_value).count


def _to_text(text_or_value: Any) -> str:
    if isinstance(text_or_value, str):
        return text_or_value
    return json.dumps(text_or_value, ensure_ascii=False, sort_keys=True)


def _estimate(text: str) -> int:
    return max(1, (len(text) + 3) // 4)
