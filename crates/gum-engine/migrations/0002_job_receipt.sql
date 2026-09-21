-- The full transaction receipt, exactly as the node returned it (logs, contractAddress, chain-specific
-- fields such as l1Fee or gasUsedForL1). Delivered in webhooks and the status route.
-- Expand-only: safe to apply while an older leader is still running.
ALTER TABLE jobs ADD COLUMN receipt JSONB;
