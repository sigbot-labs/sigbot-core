-- A routing quarantine belongs to a particular consuming instance. Its reason
-- is metadata, never part of the canonical message or a new correlation key.
ALTER TABLE messager_inbox ADD COLUMN quarantine_reason TEXT
    CHECK(quarantine_reason IS NULL OR octet_length(quarantine_reason)<=1024);
