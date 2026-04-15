"""Runtime abstractions and local adapters for Delphi 5.0."""

from .auth_composition import AuthComposer, LocalAuthComposer
from .host_infrastructure import HostInfrastructure, HostInfrastructureAssembler, LocalHostInfrastructureAssembler
from .provider_composition import LocalProviderComposer, ProviderComposer
from .launch import LaunchBehavior, WebBrowserLaunchBehavior
from .lifecycle import LocalRuntimeLifecycleCoordinator, RuntimeComponent, RuntimeLifecycleCoordinator
from .notifications import NotificationDelivery, PushoverNotificationDelivery
from .private_access import (
    AuthenticationRequiredError,
    LocalPrivateAccessGate,
    LocalRequestIdentityResolver,
    NoopSessionInvalidator,
    PrivateAccessDeniedError,
    PrivateAccessGate,
    RequestIdentity,
    RequestIdentityResolver,
    SessionInvalidator,
)
from .hosted_auth import (
    HostedAuthConfig,
    HostedBrowserSession,
    HostedSessionAuthenticator,
    NoopHostedSessionAuthenticator,
    SupabaseEmailPasswordAuthenticator,
    SupabaseHostedIdentityResolver,
    SupabasePrivateAccessGate,
    SupabaseSessionInvalidator,
)
from .profile import RuntimeProfile, select_runtime_profile
from .scheduler import RuntimeJobHandle, RuntimeScheduler, ThreadingTimerScheduler
from .settings_binding import LocalEnvironmentSettingsBinding, RuntimeSettings, SettingsBinding
from .storage_infrastructure import LocalFileSystemStorageComposer, StorageInfrastructure, StorageInfrastructureComposer
from .supabase_integration import SupabaseConfig, SupabaseConnectivityResult, SupabaseProjectIntegration, SupabaseRuntimeContext
from .workflow_state import FlaskSessionWorkflowState, WorkflowStateStore

__all__ = [
    "AuthComposer",
    "AuthenticationRequiredError",
    "FlaskSessionWorkflowState",
    "HostInfrastructure",
    "HostInfrastructureAssembler",
    "HostedAuthConfig",
    "HostedBrowserSession",
    "HostedSessionAuthenticator",
    "LaunchBehavior",
    "LocalAuthComposer",
    "LocalEnvironmentSettingsBinding",
    "LocalFileSystemStorageComposer",
    "LocalHostInfrastructureAssembler",
    "LocalPrivateAccessGate",
    "LocalProviderComposer",
    "LocalRequestIdentityResolver",
    "LocalRuntimeLifecycleCoordinator",
    "NoopSessionInvalidator",
    "NoopHostedSessionAuthenticator",
    "NotificationDelivery",
    "PushoverNotificationDelivery",
    "PrivateAccessDeniedError",
    "PrivateAccessGate",
    "ProviderComposer",
    "RequestIdentity",
    "RequestIdentityResolver",
    "RuntimeSettings",
    "RuntimeComponent",
    "RuntimeJobHandle",
    "RuntimeLifecycleCoordinator",
    "RuntimeProfile",
    "RuntimeScheduler",
    "SettingsBinding",
    "StorageInfrastructure",
    "StorageInfrastructureComposer",
    "SupabaseConfig",
    "SupabaseConnectivityResult",
    "SupabaseEmailPasswordAuthenticator",
    "SupabaseHostedIdentityResolver",
    "SupabasePrivateAccessGate",
    "SupabaseProjectIntegration",
    "SupabaseRuntimeContext",
    "SupabaseSessionInvalidator",
    "ThreadingTimerScheduler",
    "WebBrowserLaunchBehavior",
    "WorkflowStateStore",
    "select_runtime_profile",
]