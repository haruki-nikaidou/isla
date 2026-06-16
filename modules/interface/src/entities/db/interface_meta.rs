
pub struct InterfaceMetaEntity {
    pub unique_name: String,
    pub allowed_message_types: Vec<AllowedMessageType>,
    pub supported_trigger_conditions: Vec<ConversationTriggerCondition>,
    pub enabled_trigger_conditions: Vec<ConversationTriggerCondition>,
    pub platform_describe: String
}

pub enum AllowedMessageType {
    /// Text message
    Text,

    /// Image message in several extensions
    Image,

    /// Video message in several extensions
    Video,

    /// A piece of sound
    Voice,

    /// Send an arbitrary file. However, video, image and audio files that will be processed by the
    /// platform are not included here.
    ///
    /// For example, if the platform can send image with or without processing (like telegram), both
    /// `Image` and `File` should be included.
    File,

    /// Send a file that is a document (like txt, md, pdf, docx, etc.)
    Document
}

pub enum ConversationTriggerCondition {
    /// Trigger conversation on any message received.
    ///
    /// For example, a private chatting webui is this kind.
    Always,

    /// In a group chat, trigger conversation only when the AI user is mentioned.
    OnMention,

    /// Because of schedule, plugin wake, or other reason, send a message proactively
    ///
    /// This is generally available for IM platforms like discord.
    /// However, private chatting webui is not of this kind.
    Motivated,

    /// In a group chat, trigger conversation when members are talking about the topic
    /// that AI is told to care about.
    Topic
}