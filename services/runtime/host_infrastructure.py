"""Host-specific infrastructure assembly for local and hosted Delphi runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from flask import Flask

from config import AppConfig

from .settings_binding import (
    HostedEnvironmentSettingsBindingSkeleton,
    LocalEnvironmentSettingsBinding,
    RuntimeSettings,
    SettingsBinding,
)
from .storage_infrastructure import (
    HostedFileSystemStorageComposerSkeleton,
    LocalFileSystemStorageComposer,
    StorageInfrastructure,
    StorageInfrastructureComposer,
)
from .supabase_integration import SupabaseProjectIntegration, SupabaseRuntimeContext, SupabaseConfig


@dataclass(frozen=True)
class HostInfrastructure:
    """Resolved host infrastructure bundle for the active runtime target."""

    host_kind: str
    settings: RuntimeSettings
    storage: StorageInfrastructure
    supabase_integration: SupabaseProjectIntegration | None = None
    supabase_context: SupabaseRuntimeContext | None = None


@runtime_checkable
class HostInfrastructureAssembler(Protocol):
    """Assemble settings and storage for a runtime host target."""

    def assemble(self, app: Flask) -> HostInfrastructure:
        ...


class LocalHostInfrastructureAssembler:
    """Local host assembler that keeps current local config and filesystem behavior active."""

    def __init__(
        self,
        config: AppConfig,
        *,
        settings_binding: SettingsBinding | None = None,
        storage_composer: StorageInfrastructureComposer | None = None,
    ) -> None:
        self.config = config
        self.settings_binding = settings_binding or LocalEnvironmentSettingsBinding(config)
        self.storage_composer = storage_composer or LocalFileSystemStorageComposer(config)

    def assemble(self, app: Flask) -> HostInfrastructure:
        settings = self.settings_binding.resolve(app)
        settings.apply_to_app(app)
        storage = self.storage_composer.compose(app, settings)
        storage.apply_to_app(app)
        return HostInfrastructure(host_kind="local", settings=settings, storage=storage)


class HostedHostInfrastructureAssemblerSkeleton(LocalHostInfrastructureAssembler):
    """Hosted host assembler skeleton that currently reuses the local filesystem adapters."""

    def __init__(
        self,
        config: AppConfig,
        *,
        settings_binding: SettingsBinding | None = None,
        storage_composer: StorageInfrastructureComposer | None = None,
    ) -> None:
        super().__init__(
            config,
            settings_binding=settings_binding or HostedEnvironmentSettingsBindingSkeleton(config),
            storage_composer=storage_composer or HostedFileSystemStorageComposerSkeleton(config),
        )

    def assemble(self, app: Flask) -> HostInfrastructure:
        infrastructure = super().assemble(app)
        supabase_integration = SupabaseProjectIntegration(SupabaseConfig.resolve(app, self.config))
        supabase_context = supabase_integration.initialize_context()
        if not supabase_context.configured:
            raise RuntimeError("Hosted Delphi 6.0 requires SUPABASE_URL and a Supabase API key. Local hosted fallback is disabled.")
        return HostInfrastructure(
            host_kind="hosted",
            settings=infrastructure.settings,
            storage=infrastructure.storage,
            supabase_integration=supabase_integration,
            supabase_context=supabase_context,
        )


def select_host_infrastructure_assembler(app: Flask, config: AppConfig) -> HostInfrastructureAssembler:
    """Select the host infrastructure assembler for the active runtime target."""

    runtime_target = str(app.config.get("RUNTIME_TARGET") or config.runtime_target or "local").strip().lower() or "local"
    if runtime_target == "hosted":
        return HostedHostInfrastructureAssemblerSkeleton(config)
    return LocalHostInfrastructureAssembler(config)