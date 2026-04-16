create table if not exists public.hosted_runtime_state (
    object_type text not null,
    object_key text not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    expires_at timestamptz null,
    primary key (object_type, object_key)
);

create index if not exists idx_hosted_runtime_state_type on public.hosted_runtime_state (object_type, updated_at desc);
create index if not exists idx_hosted_runtime_state_expires_at on public.hosted_runtime_state (expires_at);