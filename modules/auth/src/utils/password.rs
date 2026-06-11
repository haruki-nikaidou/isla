//! Password hashing and verification utilities backed by Argon2.
//!
//! Use [`hash_password`] to derive a PHC-formatted hash string suitable for
//! persistence, and [`verify_password`] to check a plaintext password against a
//! previously stored hash.

use argon2::password_hash::SaltString;
use argon2::password_hash::rand_core::OsRng;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};

/// Hashes the given plaintext password, returning a PHC-formatted hash string.
pub fn hash_password(plaintext: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default().hash_password(plaintext.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

/// Verifies a plaintext password against a previously stored PHC hash string.
///
/// Returns `true` when the password matches the hash, `false` otherwise
/// (including when the stored hash cannot be parsed).
pub fn verify_password(plaintext: &str, hash: &str) -> bool {
    let Ok(parsed_hash) = PasswordHash::new(hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(plaintext.as_bytes(), &parsed_hash)
        .is_ok()
}
