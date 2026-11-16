-- models: what clients ask for ("chat"); versions: what workers serve ("qwen-0.5b-v2")
create table models (
    name        text primary key check (name ~ '^[a-z0-9][a-z0-9._-]{0,62}$'),
    description text not null default '',
    created_at  timestamptz not null default now()
);

create table versions (
    name       text primary key check (name ~ '^[a-z0-9][a-z0-9._-]{0,62}$'),
    model      text not null references models(name) on delete cascade,
    backend    text not null default 'fake',
    config     jsonb not null default '{}',
    weight     int not null default 0 check (weight between 0 and 100),
    created_at timestamptz not null default now()
);
create index versions_model on versions(model);

create table api_keys (
    id         text primary key,
    name       text not null,
    key_hash   text not null unique,
    rpm        int not null default 0 check (rpm >= 0),
    tpm        int not null default 0 check (tpm >= 0),
    models     text[] not null default '{}',
    created_at timestamptz not null default now(),
    revoked_at timestamptz
);

-- every change bumps the revision; gateways skip unchanged snapshots
create table revisions (
    id   bigserial primary key,
    at   timestamptz not null default now(),
    what text not null
);
insert into revisions (what) values ('initial schema');
