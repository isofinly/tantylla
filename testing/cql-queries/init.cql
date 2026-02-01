-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS ecommerce
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1} AND TABLETS = {'enabled': false};

USE ecommerce;

-- Create User Defined Types (UDT)
CREATE TYPE IF NOT EXISTS address (
    street          text,
    city            text,
    zip_code        text,
    country         text
);

CREATE TYPE IF NOT EXISTS contact_info (
    email                 text,
    phone                 text,
    preferred_time        set<text>
);

-- Create table with SET, MAP, UDT, TTL, and CDC
CREATE TABLE IF NOT EXISTS users (
    user_id            uuid PRIMARY KEY,
    username           text,
    contact            frozen<contact_info>,
    addresses          map<text, frozen<address>>,
    tags               set<text>,
    preferences        map<text, text>,
    last_login         timestamp,
    created_at         timestamp
) WITH cdc = {'enabled': true}
AND default_time_to_live = 15;

-- Create orders table with CDC and column-level TTL
CREATE TABLE IF NOT EXISTS orders (
    order_id            uuid,
    user_id             uuid,
    order_date          timestamp,
    items               map<text, int>,
    status              text,
    tags                set<text>,
    total_amount        decimal,

    PRIMARY KEY (user_id, order_id)
) WITH cdc = {'enabled': true, 'preimage': false, 'postimage': false}
AND CLUSTERING ORDER BY (order_id DESC);

-- Create session tracking table with short TTL
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id          uuid PRIMARY KEY,
    user_id             uuid,
    ip_address          text,
    user_agent          text,
    activities          set<text>,
    session_data        map<text, text>,
    created_at          timestamp
) WITH cdc = {'enabled': true}
AND default_time_to_live = 20;
