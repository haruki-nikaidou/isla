//! Personality service — selects which character facets apply to a peer.
//!
//! The trigger logic ("a person behaves differently in private vs. public")
//! lives here: [`select_facets`] is a pure function over already-loaded facets,
//! so the audience rules can be unit-tested without a database.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::{Error, sqlx::DatabaseProcessor};

use crate::entities::db::contact_identity::Relationship;
use crate::entities::db::personality::{ListPersonalityFacets, PersonalityFacetEntity};

#[derive(Debug, Clone)]
pub struct PersonalityService {
    pub database: DatabaseProcessor,
}

/// Keep only the facets that should be surfaced to a peer in `relationship`,
/// preserving the input ordering (callers pass facets pre-sorted by priority).
///
/// A facet is kept when it is part of the invariant character base
/// (`is_core`) or when its [`PrivacyControlFlag`](crate::entities::db::PrivacyControlFlag)
/// permits the peer's relationship.
fn select_facets(
    facets: Vec<PersonalityFacetEntity>,
    relationship: Relationship,
) -> Vec<PersonalityFacetEntity> {
    facets
        .into_iter()
        .filter(|facet| facet.is_core || facet.privacy.allows(relationship))
        .collect()
}

/// List the personality facets that apply to a conversation peer, ordered by
/// descending priority and filtered by the peer's relationship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListApplicableFacets {
    /// Relationship of the peer the agent is currently talking to.
    pub relationship: Relationship,
}

impl Processor<ListApplicableFacets> for PersonalityService {
    type Output = Vec<PersonalityFacetEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListApplicableFacets", err)]
    async fn process(
        &self,
        input: ListApplicableFacets,
    ) -> Result<Vec<PersonalityFacetEntity>, Error> {
        let facets = self.database.process(ListPersonalityFacets).await?;
        Ok(select_facets(facets, input.relationship))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::db::PrivacyControlFlag;

    fn facet(name: &str, is_core: bool, privacy: PrivacyControlFlag) -> PersonalityFacetEntity {
        PersonalityFacetEntity {
            id: 0,
            name: name.into(),
            content: name.into(),
            priority: 0,
            is_core,
            privacy,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn sample() -> Vec<PersonalityFacetEntity> {
        vec![
            facet("base", true, PrivacyControlFlag::Private),
            facet("intimate", false, PrivacyControlFlag::Private),
            facet("friendly", false, PrivacyControlFlag::Protected),
            facet("polite", false, PrivacyControlFlag::Public),
        ]
    }

    fn names(facets: &[PersonalityFacetEntity]) -> Vec<&str> {
        facets.iter().map(|f| f.name.as_str()).collect()
    }

    #[test]
    fn master_sees_every_facet() {
        let kept = select_facets(sample(), Relationship::Master);
        assert_eq!(names(&kept), ["base", "intimate", "friendly", "polite"]);
    }

    #[test]
    fn stranger_sees_only_core_and_public() {
        let kept = select_facets(sample(), Relationship::Stranger);
        assert_eq!(names(&kept), ["base", "polite"]);
    }

    #[test]
    fn acquaintance_sees_core_protected_and_public_but_not_private() {
        let kept = select_facets(sample(), Relationship::Acquaintance);
        assert_eq!(names(&kept), ["base", "friendly", "polite"]);
    }
}
