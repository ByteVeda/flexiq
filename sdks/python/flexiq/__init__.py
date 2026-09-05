"""flexiq — Rust-powered task queue for Python. No broker required."""

from flexiq.app import Queue
from flexiq.batching import (
    BatchItemResult,
    BatchPartialFailureError,
    BatchResultTypeError,
)
from flexiq.canvas import Signature, chain, chord, chunks, group, starmap
from flexiq.codecs import (
    AesGcmCodec,
    CodecSerializer,
    GzipCodec,
    HmacCodec,
    PayloadCodec,
)
from flexiq.context import LogLevel, current_job
from flexiq.enums import OnExcess, StorageBackend
from flexiq.events import EventType
from flexiq.exceptions import (
    CircuitBreakerOpenError,
    CircularDependencyError,
    CryptoError,
    DuplicateTaskError,
    FlexiQError,
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
    TaskNotBoundError,
    TaskTimeoutError,
)
from flexiq.inject import Inject
from flexiq.interception import InterceptionError, InterceptionMode, InterceptionReport
from flexiq.log_config import configure as configure_logging
from flexiq.mesh import MeshWorker
from flexiq.middleware import TaskMiddleware
from flexiq.mixins.periodic import PeriodicInfo
from flexiq.mixins.pubsub import ConsumerErrorAction, TopicMessage
from flexiq.notes import MAX_NOTE_FIELDS
from flexiq.predicates.outcomes import PredicateAction
from flexiq.proxies.built_in import BuiltInProxy
from flexiq.proxies.no_proxy import NoProxy

# Deliberate shadowing: ``flexiq.task`` is also a submodule, and this rebinds
# the name in the package namespace to the decorator, which is the public
# spelling the deferred decorator has to own.
#
# Breaking for one form: ``import flexiq.task`` followed by attribute access
# (``flexiq.task.TaskWrapper``) now reaches the decorator and raises
# AttributeError. ``from flexiq.task import TaskWrapper`` resolves through
# ``sys.modules`` and is unaffected — it is the spelling used everywhere in
# this package and in the docs. Pinned by
# ``test_the_task_name_wins_over_the_submodule_but_the_submodule_still_imports``.
from flexiq.registry import task
from flexiq.result import JobResult
from flexiq.retention import EffectiveRetention, Retention, RetentionPreview
from flexiq.serializers import (
    CborSerializer,
    CloudpickleSerializer,
    EncryptedSerializer,
    JsonSerializer,
    MsgPackSerializer,
    Serializer,
    SignedSerializer,
    SmartSerializer,
)
from flexiq.task import DeferredTask, TaskWrapper
from flexiq.testing import MockResource, TestMode, TestResult, TestResults

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
    "DeferredTask",
    "DuplicateTaskError",
    "EffectiveRetention",
    "EncryptedSerializer",
    "EventType",
    "FlexiQError",
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
    "TaskNotBoundError",
    "TaskTimeoutError",
    "TaskWrapper",
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
    "task",
]
# PyResultSender is only available when built with --features native-async.
# Expose it with a clean error instead of a confusing AttributeError.
try:
    from flexiq._flexiq import PyResultSender  # noqa: F401

    __all__.append("PyResultSender")
except (ImportError, AttributeError):
    pass

try:
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _get_version

    __version__ = _get_version("flexiq")
except PackageNotFoundError:
    # Running from a source tree with no installed distribution. Kept in sync
    # with the root Cargo.toml by scripts/version.mjs — do not hand-edit.
    __version__ = "2.0.0"
