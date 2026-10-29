-- Corrections are new venue facts, never edits to a previously observed fill.
CREATE FUNCTION order_reject_venue_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'venue observations are immutable; append an authoritative correction';
END;
$$;
CREATE TRIGGER order_venue_observation_immutable BEFORE UPDATE OR DELETE ON order_venue_observation
    FOR EACH ROW EXECUTE FUNCTION order_reject_venue_mutation();
