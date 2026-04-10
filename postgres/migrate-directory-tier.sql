-- Migration: Add 'directory' plan tier
-- Run inside nostrbtc-postgres container before deploying new backend

-- 1. Payments: drop old CHECK, add new one with 'directory'
ALTER TABLE payments DROP CONSTRAINT IF EXISTS payments_plan_check;
ALTER TABLE payments ADD CONSTRAINT payments_plan_check CHECK (plan IN ('monthly', 'annual', 'directory', 'email', 'email_renew'));

-- 2. Subscriptions: drop old CHECK, add new one with 'directory'
ALTER TABLE subscriptions DROP CONSTRAINT IF EXISTS subscriptions_plan_check;
ALTER TABLE subscriptions ADD CONSTRAINT subscriptions_plan_check CHECK (plan IN ('monthly', 'annual', 'directory', 'email', 'email_renew'));

-- 3. Payments: add nip05_name column for directory plan
ALTER TABLE payments ADD COLUMN IF NOT EXISTS nip05_name TEXT;

-- 4. Subscriptions: add nip05_expires_at column
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS nip05_expires_at TIMESTAMPTZ;

-- 5. Backfill: set nip05_expires_at = expires_at for existing subscribers with NIP-05
UPDATE subscriptions SET nip05_expires_at = expires_at
WHERE nip05_name IS NOT NULL AND nip05_expires_at IS NULL;

-- 6. Add activation log event types
ALTER TABLE activation_log DROP CONSTRAINT IF EXISTS activation_log_event_type_check;
ALTER TABLE activation_log ADD CONSTRAINT activation_log_event_type_check CHECK (event_type IN (
    'payment_completed', 'subscription_activated', 'subscription_renewed',
    'welcome_dm_sent', 'push_notification_sent', 'backfill_started',
    'backfill_completed', 'directory_indexed', 'subscription_expired',
    'expiry_warning_sent', 'directory_listing_activated',
    'nip05_expired', 'nip05_expiry_warning'
));
