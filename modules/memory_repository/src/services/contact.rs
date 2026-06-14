//! Contact service — identities, platform contacts, and relationship stories.

use dynamic_message_extension::dynamic_context::{ContextRef, RedisPool};
use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::amqp::AmqpPool;
use wakuwaku::{Error, sqlx::DatabaseProcessor};

use crate::entities::db::embedding::EntryRef;
use crate::entities::db::{
    PrivacyControlFlag,
    contact::{
        ContactEntity, CreateContact, DeleteContact, FindContactById, FindContactByPlatformUser,
        ListContactsByIdentity, UpdateContact,
    },
    contact_identity::{
        ContactIdentityEntity, CreateContactIdentity, DeleteContactIdentity,
        FindContactIdentityById, ListContactIdentities, Relationship, UpdateContactIdentity,
        UpdateContactIdentityPrivacy, UpdateContactIdentityRelationship,
    },
    contact_stories::{
        ContactStoryEntity, CreateContactStory, DeleteContactStory, FindContactStoryById,
        ListContactStoriesByIdentity, StoryType, UpdateContactStory, UpdateContactStoryPrivacy,
    },
};
use crate::events::publish::{EntryPrivacyChanged, EntryRemoved, EntryUpserted};

#[derive(Clone)]
pub struct ContactService {
    pub database: DatabaseProcessor,
    pub mq: AmqpPool,
    pub redis: RedisPool,
}

impl ContactService {
    /// Publish an [`EntryUpserted`] for a contact identity so the RAG index
    /// (re)embeds it.
    async fn publish_identity_upsert(&self, id: Uuid) -> Result<(), Error> {
        if let Some(i) = self
            .database
            .process(FindContactIdentityById { id })
            .await?
        {
            ContextRef::publish(
                &self.redis,
                &self.mq,
                EntryUpserted {
                    reference: EntryRef::contact(id),
                    privacy: i.privacy,
                    content: format!("{}\n{}", i.identify_name, i.description),
                },
            )
            .await?;
        }
        Ok(())
    }
}

// ─── Identity CRUD ────────────────────────────────────────────────────────────

/// Create a new cross-platform identity. A fresh UUID is generated automatically.
#[derive(Debug, Clone)]
pub struct CreateContactIdentityRequest {
    pub identify_name: String,
    pub description: String,
    pub relationship: Relationship,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateContactIdentityRequest> for ContactService {
    type Output = Uuid;
    type Error = Error;

    #[instrument(skip_all, name = "CreateContactIdentityRequest", err)]
    async fn process(&self, input: CreateContactIdentityRequest) -> Result<Uuid, Error> {
        if input.identify_name.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        let id = Uuid::new_v4();
        self.database
            .process(CreateContactIdentity {
                id,
                identify_name: input.identify_name,
                description: input.description,
                relationship: input.relationship,
                privacy: input.privacy,
            })
            .await?;
        self.publish_identity_upsert(id).await?;
        Ok(id)
    }
}

/// Retrieve a contact identity by its UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindContactIdentityRequest {
    pub id: Uuid,
}

impl Processor<FindContactIdentityRequest> for ContactService {
    type Output = Option<ContactIdentityEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindContactIdentityRequest", err, fields(id = %input.id))]
    async fn process(
        &self,
        input: FindContactIdentityRequest,
    ) -> Result<Option<ContactIdentityEntity>, Error> {
        Ok(self
            .database
            .process(FindContactIdentityById { id: input.id })
            .await?)
    }
}

/// Update the name and description of an identity.
#[derive(Debug, Clone)]
pub struct UpdateContactIdentityRequest {
    pub id: Uuid,
    pub identify_name: String,
    pub description: String,
}

impl Processor<UpdateContactIdentityRequest> for ContactService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateContactIdentityRequest", err, fields(id = %input.id))]
    async fn process(&self, input: UpdateContactIdentityRequest) -> Result<bool, Error> {
        if input.identify_name.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        let updated = self
            .database
            .process(UpdateContactIdentity {
                id: input.id,
                identify_name: input.identify_name,
                description: input.description,
            })
            .await?;
        if updated {
            self.publish_identity_upsert(input.id).await?;
        }
        Ok(updated)
    }
}

/// Transition the relationship status of an identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateRelationshipRequest {
    pub id: Uuid,
    pub relationship: Relationship,
}

impl Processor<UpdateRelationshipRequest> for ContactService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateRelationshipRequest", err, fields(id = %input.id))]
    async fn process(&self, input: UpdateRelationshipRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(UpdateContactIdentityRelationship {
                id: input.id,
                relationship: input.relationship,
            })
            .await?)
    }
}

/// Reassign the [`PrivacyControlFlag`] of an identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateContactIdentityPrivacyRequest {
    pub id: Uuid,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateContactIdentityPrivacyRequest> for ContactService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateContactIdentityPrivacyRequest", err, fields(id = %input.id))]
    async fn process(&self, input: UpdateContactIdentityPrivacyRequest) -> Result<bool, Error> {
        let updated = self
            .database
            .process(UpdateContactIdentityPrivacy {
                id: input.id,
                privacy: input.privacy,
            })
            .await?;
        if updated {
            ContextRef::publish(
                &self.redis,
                &self.mq,
                EntryPrivacyChanged {
                    reference: EntryRef::contact(input.id),
                    privacy: input.privacy,
                },
            )
            .await?;
        }
        Ok(updated)
    }
}

/// Delete a contact identity (cascades to platform contacts and stories).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteContactIdentityRequest {
    pub id: Uuid,
}

impl Processor<DeleteContactIdentityRequest> for ContactService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteContactIdentityRequest", err, fields(id = %input.id))]
    async fn process(&self, input: DeleteContactIdentityRequest) -> Result<bool, Error> {
        let deleted = self
            .database
            .process(DeleteContactIdentity { id: input.id })
            .await?;
        if deleted {
            ContextRef::publish(
                &self.redis,
                &self.mq,
                EntryRemoved {
                    reference: EntryRef::contact(input.id),
                },
            )
            .await?;
        }
        Ok(deleted)
    }
}

/// Paginated list of all identities ordered alphabetically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListContactIdentitiesRequest {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListContactIdentitiesRequest> for ContactService {
    type Output = Vec<ContactIdentityEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListContactIdentitiesRequest", err)]
    async fn process(
        &self,
        input: ListContactIdentitiesRequest,
    ) -> Result<Vec<ContactIdentityEntity>, Error> {
        Ok(self
            .database
            .process(ListContactIdentities {
                limit: input.limit,
                offset: input.offset,
            })
            .await?)
    }
}

// ─── Platform contact CRUD ────────────────────────────────────────────────────

/// Create or update a platform contact.
///
/// If a contact with the same `(platform, user_id)` already exists, its
/// `display_name` and `identity` are updated. Otherwise a new record is inserted.
/// Returns the contact's primary key.
#[derive(Debug, Clone)]
pub struct UpsertContactRequest {
    pub display_name: String,
    pub user_id: String,
    pub platform: String,
    pub identity: Uuid,
}

impl Processor<UpsertContactRequest> for ContactService {
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "UpsertContactRequest", err,
        fields(platform = %input.platform, user_id = %input.user_id))]
    async fn process(&self, input: UpsertContactRequest) -> Result<i64, Error> {
        if input.display_name.trim().is_empty() || input.user_id.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        let existing = self
            .database
            .process(FindContactByPlatformUser {
                platform: input.platform.clone(),
                user_id: input.user_id.clone(),
            })
            .await?;

        if let Some(contact) = existing {
            self.database
                .process(UpdateContact {
                    id: contact.id,
                    display_name: input.display_name,
                    identity: input.identity,
                })
                .await?;
            Ok(contact.id)
        } else {
            Ok(self
                .database
                .process(CreateContact {
                    display_name: input.display_name,
                    user_id: input.user_id,
                    platform: input.platform,
                    identity: input.identity,
                })
                .await?)
        }
    }
}

/// Retrieve a platform contact by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindContactRequest {
    pub id: i64,
}

impl Processor<FindContactRequest> for ContactService {
    type Output = Option<ContactEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindContactRequest", err, fields(id = input.id))]
    async fn process(&self, input: FindContactRequest) -> Result<Option<ContactEntity>, Error> {
        Ok(self
            .database
            .process(FindContactById { id: input.id })
            .await?)
    }
}

/// Delete a platform contact by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteContactRequest {
    pub id: i64,
}

impl Processor<DeleteContactRequest> for ContactService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteContactRequest", err, fields(id = input.id))]
    async fn process(&self, input: DeleteContactRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(DeleteContact { id: input.id })
            .await?)
    }
}

/// All platform contacts linked to a given identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListContactsByIdentityRequest {
    pub identity: Uuid,
}

impl Processor<ListContactsByIdentityRequest> for ContactService {
    type Output = Vec<ContactEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListContactsByIdentityRequest", err,
        fields(identity = %input.identity))]
    async fn process(
        &self,
        input: ListContactsByIdentityRequest,
    ) -> Result<Vec<ContactEntity>, Error> {
        Ok(self
            .database
            .process(ListContactsByIdentity {
                identity: input.identity,
            })
            .await?)
    }
}

// ─── Story CRUD ───────────────────────────────────────────────────────────────

/// Record a new story for a contact identity.
#[derive(Debug, Clone)]
pub struct CreateStoryRequest {
    pub identity: Uuid,
    pub story_type: StoryType,
    pub story_name: String,
    pub story_summary: String,
    pub story_text: String,
    pub happened_at: PrimitiveDateTime,
    pub related_conversation: Option<i64>,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateStoryRequest> for ContactService {
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "CreateStoryRequest", err, fields(identity = %input.identity))]
    async fn process(&self, input: CreateStoryRequest) -> Result<i64, Error> {
        if input.story_name.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        Ok(self
            .database
            .process(CreateContactStory {
                identity: input.identity,
                story_type: input.story_type,
                story_name: input.story_name,
                story_summary: input.story_summary,
                story_text: input.story_text,
                happened_at: input.happened_at,
                related_conversation: input.related_conversation,
                privacy: input.privacy,
            })
            .await?)
    }
}

/// Retrieve a story by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindStoryRequest {
    pub id: i64,
}

impl Processor<FindStoryRequest> for ContactService {
    type Output = Option<ContactStoryEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindStoryRequest", err, fields(id = input.id))]
    async fn process(&self, input: FindStoryRequest) -> Result<Option<ContactStoryEntity>, Error> {
        Ok(self
            .database
            .process(FindContactStoryById { id: input.id })
            .await?)
    }
}

/// Update the narrative content of an existing story.
#[derive(Debug, Clone)]
pub struct UpdateStoryRequest {
    pub id: i64,
    pub story_name: String,
    pub story_summary: String,
    pub story_text: String,
}

impl Processor<UpdateStoryRequest> for ContactService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateStoryRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateStoryRequest) -> Result<bool, Error> {
        if input.story_name.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        Ok(self
            .database
            .process(UpdateContactStory {
                id: input.id,
                story_name: input.story_name,
                story_summary: input.story_summary,
                story_text: input.story_text,
            })
            .await?)
    }
}

/// Reassign the [`PrivacyControlFlag`] of a story.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateStoryPrivacyRequest {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateStoryPrivacyRequest> for ContactService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateStoryPrivacyRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateStoryPrivacyRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(UpdateContactStoryPrivacy {
                id: input.id,
                privacy: input.privacy,
            })
            .await?)
    }
}

/// Delete a story by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteStoryRequest {
    pub id: i64,
}

impl Processor<DeleteStoryRequest> for ContactService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteStoryRequest", err, fields(id = input.id))]
    async fn process(&self, input: DeleteStoryRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(DeleteContactStory { id: input.id })
            .await?)
    }
}

/// Paginated list of stories for an identity ordered by occurrence time (newest first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListStoriesByIdentityRequest {
    pub identity: Uuid,
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListStoriesByIdentityRequest> for ContactService {
    type Output = Vec<ContactStoryEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListStoriesByIdentityRequest", err,
        fields(identity = %input.identity))]
    async fn process(
        &self,
        input: ListStoriesByIdentityRequest,
    ) -> Result<Vec<ContactStoryEntity>, Error> {
        Ok(self
            .database
            .process(ListContactStoriesByIdentity {
                identity: input.identity,
                limit: input.limit,
                offset: input.offset,
            })
            .await?)
    }
}
