class SSLConfigurationError(Exception):
    """Raised when SSL/TLS certificate bundle configuration fails."""
    pass


class EmptyStreamError(RuntimeError):
    """Raised when a provider closes a stream without yielding a response."""

    pass


class MoAPresetNotFoundError(ValueError):
    """Raised when a persisted MoA preset no longer exists in config."""


class ProjectExecutionControlSignal(Exception):
    """Terminal project-turn signal that must bypass conversation retries."""


class ProjectToolExecutionDenied(
    PermissionError,
    ProjectExecutionControlSignal,
):
    """Fail-closed project firewall denial."""
