use uuid::Uuid;
use memory_repository::entities::db::PrivacyControlFlag;

pub struct ChatRoomEntity {
    pub id: Uuid,

    /// This is to handle something like channels in a discord server. It's a pointer to the parent chat room.
    pub is_thread_of: Option<Uuid>,

    pub name: String,

    pub description: String,

    /// The most private content that can be seen in this chat room.
    pub privacy: PrivacyControlFlag,

    /// Not null of it's PM. Inherits privacy from the contact.
    pub inherits_privacy_from: Option<i64>
}