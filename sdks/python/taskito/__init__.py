"""taskito — Rust-powered task queue for Python. No broker required."""

from taskito.app import Queue
from taskito.batching import (
    BatchItemResult,
    BatchPartialFailureError,
    BatchResultTypeError,
)
from taskito.canvas import Signature, chain, chord, chunks, group, starmap
from taskito.codecs import (
    AesGcmCodec,
    CodecSerializer,
    GzipCodec,
    HmacCodec,
    PayloadCodec,
)
from taskito.context import LogLevel, current_job
from taskito.enums import OnExcess, StorageBackend
from taskito.events import EventType
from taskito.exceptions import (
    CircuitBreakerOpenError,
    CircularDependencyError,
    CryptoError,
    JobNotFoundError,
    MaxRetriesExceededError,
    NotesValidationError,
    PredicateRejectedError,
    ProxyCleanupError,
    ProxyReconstructionError,
    QueueError,
    QueueFullError,
    RateLimitExceededError,
    ResourceError,
    ResourceInitError,
    ResourceNotFoundError,
    ResourceUnavailableError,
    SerializationError,
    SoftTimeoutError,
    TaskCancelledError,
    TaskFailedError,
    TaskitoError,
    TaskTimeoutError,
)
from taskito.inject import Inject
from taskito.interception import InterceptionError, InterceptionMode, InterceptionReport
from taskito.log_config import configure as configure_logging
from taskito.mesh import MeshWorker
from taskito.middleware import TaskMiddleware
from taskito.mixins.periodic import PeriodicInfo
from taskito.mixins.pubsub import ConsumerErrorAction, TopicMessage
from taskito.notes import MAX_NOTE_FIELDS
from taskito.predicates.outcomes import PredicateAction
from taskito.proxies.built_in import BuiltInProxy
from taskito.proxies.no_proxy import NoProxy
from taskito.result import JobResult
from taskito.retention import EffectiveRetention, Retention, RetentionPreview
from taskito.serializers import (
    CborSerializer,
    CloudpickleSerializer,
    EncryptedSerializer,
    JsonSerializer,
    MsgPackSerializer,
    Serializer,
    SignedSerializer,
    SmartSerializer,
)
from taskito.task import TaskWrapper
from taskito.testing import MockResource, TestMode, TestResult, TestResults

__all__ = [
    "MAX_NOTE_FIELDS",
    "AesGcmCodec",
    "BatchItemResult",
    "BatchPartialFailureError",
    "BatchResultTypeError",
    "BuiltInProxy",
    "CborSerializer",
    "CircuitBreakerOpenError",
    "CircularDependencyError",
    "CloudpickleSerializer",
    "CodecSerializer",
    "ConsumerErrorAction",
    "CryptoError",
    "EffectiveRetention",
    "EncryptedSerializer",
    "EventType",
    "GzipCodec",
    "HmacCodec",
    "Inject",
    "InterceptionError",
    "InterceptionMode",
    "InterceptionReport",
    "JobNotFoundError",
    "JobResult",
    "JsonSerializer",
    "LogLevel",
    "MaxRetriesExceededError",
    "MeshWorker",
    "MockResource",
    "MsgPackSerializer",
    "NoProxy",
    "NotesValidationError",
    "OnExcess",
    "PayloadCodec",
    "PeriodicInfo",
    "PredicateAction",
    "PredicateRejectedError",
    "ProxyCleanupError",
    "ProxyReconstructionError",
    "Queue",
    "QueueError",
    "QueueFullError",
    "RateLimitExceededError",
    "ResourceError",
    "ResourceInitError",
    "ResourceNotFoundError",
    "ResourceUnavailableError",
    "Retention",
    "RetentionPreview",
    "SerializationError",
    "Serializer",
    "Signature",
    "SignedSerializer",
    "SmartSerializer",
    "SoftTimeoutError",
    "StorageBackend",
    "TaskCancelledError",
    "TaskFailedError",
    "TaskMiddleware",
    "TaskTimeoutError",
    "TaskWrapper",
    "TaskitoError",
    "TestMode",
    "TestResult",
    "TestResults",
    "TopicMessage",
    "chain",
    "chord",
    "chunks",
    "configure_logging",
    "current_job",
    "group",
    "starmap",
]
# PyResultSender is only available when built with --features native-async.
# Expose it with a clean error instead of a confusing AttributeError.
try:
    from taskito._taskito import PyResultSender  # noqa: F401

    __all__.append("PyResultSender")
except (ImportError, AttributeError):
    pass

try:
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _get_version

    __version__ = _get_version("taskito")
except PackageNotFoundError:
    # Running from a source tree with no installed distribution. Kept in sync
    # with the root Cargo.toml by scripts/version.mjs — do not hand-edit.
    __version__ = "0.23.0"
