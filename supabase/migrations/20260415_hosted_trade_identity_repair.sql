create or replace function public.sync_journal_trade_identity_sequence()
returns bigint
language plpgsql
security definer
as $$
declare
    seq_name text;
    max_id bigint;
begin
    seq_name := pg_get_serial_sequence('public.journal_trades', 'id');
    if seq_name is null then
        raise exception 'journal_trades id sequence could not be resolved';
    end if;

    select coalesce(max(id), 0) into max_id from public.journal_trades;

    if max_id <= 0 then
        perform setval(seq_name, 1, false);
        return 1;
    end if;

    perform setval(seq_name, max_id, true);
    return max_id + 1;
end;
$$;

create or replace function public.sync_journal_trade_close_event_identity_sequence()
returns bigint
language plpgsql
security definer
as $$
declare
    seq_name text;
    max_id bigint;
begin
    seq_name := pg_get_serial_sequence('public.journal_trade_close_events', 'id');
    if seq_name is null then
        raise exception 'journal_trade_close_events id sequence could not be resolved';
    end if;

    select coalesce(max(id), 0) into max_id from public.journal_trade_close_events;

    if max_id <= 0 then
        perform setval(seq_name, 1, false);
        return 1;
    end if;

    perform setval(seq_name, max_id, true);
    return max_id + 1;
end;
$$;
