-- Insert users with UDT, SET, and MAP
INSERT INTO users (
    user_id,
    username,
    contact,
    addresses,
    tags,
    preferences,
    last_login,
    created_at
)
VALUES (
    uuid(),
    'alice_wonder',
    {
        email: 'alice@example.com',
        phone: '+1-555-0101',
        preferred_time: {
            'morning',
            'evening'
        }
    },
    {
        'home': {
            street: '123 Main St',
            city: 'San Francisco',
            zip_code: '94102',
            country: 'USA'
        },
        'work': {
            street: '456 Market St',
            city: 'San Francisco',
            zip_code: '94105',
            country: 'USA'
        }
    },
    {
        'premium',
        'verified',
        'early-adopter'
    },
    {
        'theme': 'dark',
        'language': 'en',
        'notifications': 'enabled'
    },
    toTimestamp(now()),
    toTimestamp(now())
);

INSERT INTO users (
    user_id,
    username,
    contact,
    addresses,
    tags,
    preferences,
    last_login,
    created_at
)
VALUES (
    uuid(),
    'alice_wonder',
    {
        email: 'alice@example.com',
        phone: '+1-555-0101',
        preferred_time: {
            'morning',
            'evening'
        }
    },
    {
        'home': {
            street: '123 Main St',
            city: 'San Francisco',
            zip_code: '94102',
            country: 'USA'
        },
        'work': {
            street: '456 Market St',
            city: 'San Francisco',
            zip_code: '94105',
            country: 'USA'
        }
    },
    {
        'premium',
        'verified',
        'early-adopter'
    },
    {
        'theme': 'dark',
        'language': 'en',
        'notifications': 'enabled'
    },
    toTimestamp(now()),
    toTimestamp(now())
);

INSERT INTO users (
    user_id,
    username,
    contact,
    addresses,
    tags,
    preferences,
    last_login,
    created_at
)
VALUES (
    uuid(),
    'bob_builder',
    {
        email: 'bob@example.com',
        phone: '+1-555-0202',
        preferred_time: {
            'afternoon'
        }
    },
    {
        'home': {
            street: '789 Oak Ave',
            city: 'Seattle',
            zip_code: '98101',
            country: 'USA'
        }
    },
    {
        'standard',
        'verified'
    },
    {
        'theme': 'light',
        'language': 'en'
    },
    toTimestamp(now()),
    toTimestamp(now())
);

-- Insert with custom TTL (overrides default)
INSERT INTO users (
    user_id,
    username,
    contact,
    addresses,
    tags,
    preferences,
    last_login,
    created_at
)
VALUES (
    uuid(),
    'charlie_temp',
    {
        email: 'charlie@example.com',
        phone: '+1-555-0303',
        preferred_time: {
            'morning'
        }
    },
    {
        'temp': {
            street: '321 Temp Rd',
            city: 'Austin',
            zip_code: '78701',
            country: 'USA'
        }
    },
    {
        'trial',
        'new'
    },
    {
        'theme': 'light'
    },
    toTimestamp(now()),
    toTimestamp(now())
) USING TTL 86400; -- 24 hours

-- Insert orders with MAP and SET
INSERT INTO orders (
    order_id,
    user_id,
    order_date,
    items,
    status,
    tags,
    total_amount
)
VALUES (
    uuid(),
    uuid(),
    toTimestamp(now()),
    {
        'laptop': 1,
        'mouse': 2,
        'keyboard': 1
    },
    'completed',
    {
        'electronics',
        'high-value',
        'express-shipping'
    },
    1299.99
);

INSERT INTO orders (
    order_id,
    user_id,
    order_date,
    items,
    status,
    tags,
    total_amount
)
VALUES (
    uuid(),
    uuid(),
    toTimestamp(now()),
    {
        'book': 3,
        'pen': 5
    },
    'pending',
    {
        'books',
        'standard-shipping'
    },
    45.50
) USING TTL 2592000; -- 30 days

-- Insert user sessions with short TTL
INSERT INTO user_sessions (
    session_id,
    user_id,
    ip_address,
    user_agent,
    activities,
    session_data,
    created_at
)
VALUES (
    uuid(),
    uuid(),
    '192.168.1.100',
    'Mozilla/5.0',
    {
        'login',
        'browse',
        'add_to_cart'
    },
    {
        'last_page': '/products',
        'cart_items': '3',
        'device': 'desktop'
    },
    toTimestamp(now())
);

INSERT INTO user_sessions (
    session_id,
    user_id,
    ip_address,
    user_agent,
    activities,
    session_data,
    created_at
)
VALUES (
    uuid(),
    uuid(),
    '192.168.1.101',
    'Chrome/120.0',
    {
        'login',
        'checkout',
        'payment'
    },
    {
        'last_page': '/checkout',
        'payment_method': 'credit_card'
    },
    toTimestamp(now())
) USING TTL 1800; -- 30 minutes

-- Update examples with TTL
UPDATE users USING TTL 604800
SET tags = tags + {'vip'}, preferences['notifications'] = 'priority'
WHERE user_id = uuid();

UPDATE orders
SET status = 'shipped', tags = tags + {'shipped'}
WHERE user_id = uuid() AND order_id = uuid();