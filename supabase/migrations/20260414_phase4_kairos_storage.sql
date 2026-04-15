create table if not exists public.kairos_snapshots (
    snapshot_key text primary key,
    payload jsonb not null,
    saved_at timestamptz not null default now()
);

create table if not exists public.kairos_simulation_tapes (
    scenario_key text primary key,
    session_date date,
    label text not null,
    source text not null default '',
    source_family_tag text not null default '',
    source_type text not null default '',
    session_status text not null default 'unknown',
    bar_count integer not null default 0,
    bundle_payload jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_kairos_simulation_tapes_session_date on public.kairos_simulation_tapes (session_date desc);
create index if not exists idx_kairos_simulation_tapes_source_family on public.kairos_simulation_tapes (source_family_tag, source_type);
