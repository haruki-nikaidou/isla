DROP TRIGGER IF EXISTS embedding_remove_by_diary ON memory.diary;
DROP TRIGGER IF EXISTS embedding_remove_by_contact_identity ON memory.contact_identity;
DROP TRIGGER IF EXISTS embedding_remove_by_conversation ON memory.conversation;
DROP TRIGGER IF EXISTS embedding_remove_by_calender_event ON memory.calender_event;

DROP TRIGGER IF EXISTS embedding_sync_diary_privacy ON memory.diary;
DROP TRIGGER IF EXISTS embedding_sync_contact_identity_privacy ON memory.contact_identity;
DROP TRIGGER IF EXISTS embedding_sync_conversation_privacy ON memory.conversation;
DROP TRIGGER IF EXISTS embedding_sync_calender_event_privacy ON memory.calender_event;

DROP FUNCTION IF EXISTS memory.embedding_remove_by_diary();
DROP FUNCTION IF EXISTS memory.embedding_remove_by_contact_identity();
DROP FUNCTION IF EXISTS memory.embedding_remove_by_conversation();
DROP FUNCTION IF EXISTS memory.embedding_remove_by_calender_event();

DROP FUNCTION IF EXISTS memory.embedding_sync_diary_privacy();
DROP FUNCTION IF EXISTS memory.embedding_sync_contact_identity_privacy();
DROP FUNCTION IF EXISTS memory.embedding_sync_conversation_privacy();
DROP FUNCTION IF EXISTS memory.embedding_sync_calender_event_privacy();
