-- Move embedding privacy/delete propagation from AMQP hooks into PostgreSQL triggers.

CREATE FUNCTION memory.embedding_sync_calender_event_privacy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE memory.embedding
    SET privacy = NEW.privacy
    WHERE calender_event = NEW.id;
    RETURN NEW;
END;
$$;

CREATE FUNCTION memory.embedding_sync_conversation_privacy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE memory.embedding
    SET privacy = NEW.privacy
    WHERE conversation = NEW.id;
    RETURN NEW;
END;
$$;

CREATE FUNCTION memory.embedding_sync_contact_identity_privacy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE memory.embedding
    SET privacy = NEW.privacy
    WHERE contact = NEW.id;
    RETURN NEW;
END;
$$;

CREATE FUNCTION memory.embedding_sync_diary_privacy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE memory.embedding
    SET privacy = NEW.privacy
    WHERE diary = NEW.id;
    RETURN NEW;
END;
$$;

CREATE FUNCTION memory.embedding_remove_by_calender_event()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM memory.embedding
    WHERE calender_event = OLD.id;
    RETURN OLD;
END;
$$;

CREATE FUNCTION memory.embedding_remove_by_conversation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM memory.embedding
    WHERE conversation = OLD.id;
    RETURN OLD;
END;
$$;

CREATE FUNCTION memory.embedding_remove_by_contact_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM memory.embedding
    WHERE contact = OLD.id;
    RETURN OLD;
END;
$$;

CREATE FUNCTION memory.embedding_remove_by_diary()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM memory.embedding
    WHERE diary = OLD.id;
    RETURN OLD;
END;
$$;

CREATE TRIGGER embedding_sync_calender_event_privacy
AFTER UPDATE OF privacy ON memory.calender_event
FOR EACH ROW
WHEN (OLD.privacy IS DISTINCT FROM NEW.privacy)
EXECUTE FUNCTION memory.embedding_sync_calender_event_privacy();

CREATE TRIGGER embedding_sync_conversation_privacy
AFTER UPDATE OF privacy ON memory.conversation
FOR EACH ROW
WHEN (OLD.privacy IS DISTINCT FROM NEW.privacy)
EXECUTE FUNCTION memory.embedding_sync_conversation_privacy();

CREATE TRIGGER embedding_sync_contact_identity_privacy
AFTER UPDATE OF privacy ON memory.contact_identity
FOR EACH ROW
WHEN (OLD.privacy IS DISTINCT FROM NEW.privacy)
EXECUTE FUNCTION memory.embedding_sync_contact_identity_privacy();

CREATE TRIGGER embedding_sync_diary_privacy
AFTER UPDATE OF privacy ON memory.diary
FOR EACH ROW
WHEN (OLD.privacy IS DISTINCT FROM NEW.privacy)
EXECUTE FUNCTION memory.embedding_sync_diary_privacy();

CREATE TRIGGER embedding_remove_by_calender_event
AFTER DELETE ON memory.calender_event
FOR EACH ROW
EXECUTE FUNCTION memory.embedding_remove_by_calender_event();

CREATE TRIGGER embedding_remove_by_conversation
AFTER DELETE ON memory.conversation
FOR EACH ROW
EXECUTE FUNCTION memory.embedding_remove_by_conversation();

CREATE TRIGGER embedding_remove_by_contact_identity
AFTER DELETE ON memory.contact_identity
FOR EACH ROW
EXECUTE FUNCTION memory.embedding_remove_by_contact_identity();

CREATE TRIGGER embedding_remove_by_diary
AFTER DELETE ON memory.diary
FOR EACH ROW
EXECUTE FUNCTION memory.embedding_remove_by_diary();
