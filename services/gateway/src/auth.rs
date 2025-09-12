use jsonwebtoken::{DecodingKey, Validation, decode, errors::ErrorKind};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub org_id: u64,
    pub channel_id: Uuid,
    pub exp: usize,
}

#[derive(Error, Debug)]
pub enum TokenValidationError {
    #[error("Invalid RSA public key: {0}")]
    InvalidPublicKey(String),

    #[error("Token has expired")]
    TokenExpired,

    #[error("Token validation failed: {0}")]
    OtherValidationError(String),

    #[error("Organization ID mismatch: expected {expected}, got {actual}")]
    OrgIdMismatch { expected: u64, actual: u64 },

    #[error("Channel ID mismatch: expected {expected}, got {actual}")]
    ChannelIdMismatch { expected: Uuid, actual: Uuid },
}

pub fn validate_token(
    token: &str,
    org_id: u64,
    channel_id: Uuid,
    rsa_public_key: &str,
) -> Result<Claims, TokenValidationError> {
    let validation = Validation::new(jsonwebtoken::Algorithm::RS256);
    let key = DecodingKey::from_rsa_pem(rsa_public_key.as_bytes())
        .map_err(|e| TokenValidationError::InvalidPublicKey(e.to_string()))?;
    let token_data = decode::<Claims>(token, &key, &validation).map_err(|e| match e.kind() {
        ErrorKind::ExpiredSignature => TokenValidationError::TokenExpired,
        _ => TokenValidationError::OtherValidationError(e.to_string()),
    })?;

    let claims = token_data.claims;

    if claims.org_id != org_id {
        return Err(TokenValidationError::OrgIdMismatch {
            expected: org_id,
            actual: claims.org_id,
        });
    }

    if claims.channel_id != channel_id {
        return Err(TokenValidationError::ChannelIdMismatch {
            expected: channel_id,
            actual: claims.channel_id,
        });
    }

    Ok(claims)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use rsa::pkcs1::{EncodeRsaPrivateKey, LineEnding};
    use rsa::pkcs8::EncodePublicKey;
    use rsa::rand_core::OsRng;
    use rsa::{RsaPrivateKey, RsaPublicKey};
    use std::sync::LazyLock;

    static STANDARD_KEYS: LazyLock<(String, String)> = LazyLock::new(generate_test_keypair);

    fn generate_test_keypair() -> (String, String) {
        let mut rng = OsRng;
        let bits = 2048;
        let private_key = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate key");
        let public_key = RsaPublicKey::from(&private_key);

        let private_pem = private_key
            .to_pkcs1_pem(LineEnding::LF)
            .expect("failed to encode private key")
            .to_string();
        let public_pem = public_key
            .to_public_key_pem(LineEnding::LF)
            .expect("failed to encode public key");

        (private_pem, public_pem)
    }

    fn create_test_token(claims: &Claims, private_key: &str) -> String {
        let key = EncodingKey::from_rsa_pem(private_key.as_bytes()).unwrap();
        encode(&Header::new(jsonwebtoken::Algorithm::RS256), claims, &key).unwrap()
    }

    #[test]
    fn test_valid_token() {
        let (private_key, public_key) = &*STANDARD_KEYS;
        let org_id = 123;
        let channel_id = Uuid::new_v4();
        let claims = Claims {
            org_id,
            channel_id,
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key);

        let result = validate_token(&token, org_id, channel_id, public_key);
        assert!(result.is_ok());

        let validated_claims = result.unwrap();
        assert_eq!(validated_claims.org_id, org_id);
        assert_eq!(validated_claims.channel_id, channel_id);
    }

    #[test]
    fn test_expired_token() {
        let (private_key, public_key) = &*STANDARD_KEYS;
        let org_id = 123;
        let channel_id = Uuid::new_v4();
        let claims = Claims {
            org_id,
            channel_id,
            exp: (Utc::now().timestamp() - 3600) as usize, // 1 hour ago
        };
        let token = create_test_token(&claims, private_key);

        let result = validate_token(&token, org_id, channel_id, public_key);
        assert!(matches!(result, Err(TokenValidationError::TokenExpired)));
    }

    #[test]
    fn test_invalid_org_id() {
        let (private_key, public_key) = &*STANDARD_KEYS;
        let org_id = 123;
        let wrong_org_id = 456;
        let channel_id = Uuid::new_v4();
        let claims = Claims {
            org_id,
            channel_id,
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key);

        let result = validate_token(&token, wrong_org_id, channel_id, public_key);
        assert!(matches!(
            result,
            Err(TokenValidationError::OrgIdMismatch { expected, actual })
            if expected == wrong_org_id && actual == org_id
        ));
    }

    #[test]
    fn test_invalid_channel_id() {
        let (private_key, public_key) = &*STANDARD_KEYS;
        let org_id = 123;
        let channel_id = Uuid::new_v4();
        let claims = Claims {
            org_id,
            channel_id,
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key);

        let wrong_channel = Uuid::new_v4();
        let result = validate_token(&token, org_id, wrong_channel, public_key);
        assert!(matches!(
            result,
            Err(TokenValidationError::ChannelIdMismatch { expected, actual })
            if expected == wrong_channel && actual == channel_id
        ));
    }

    #[test]
    fn test_malformed_token() {
        let (_, public_key) = &*STANDARD_KEYS;

        let result = validate_token("not.a.valid.token", 123, Uuid::new_v4(), public_key);
        assert!(matches!(
            result,
            Err(TokenValidationError::OtherValidationError(_))
        ));
    }

    #[test]
    fn test_invalid_public_key() {
        let (private_key, _) = &*STANDARD_KEYS;
        let org_id = 123;
        let channel_id = Uuid::new_v4();
        let claims = Claims {
            org_id,
            channel_id,
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key);

        let result = validate_token(&token, org_id, channel_id, "invalid_key");
        assert!(matches!(
            result,
            Err(TokenValidationError::InvalidPublicKey(_))
        ));
    }

    #[test]
    fn test_wrong_public_key() {
        let (private_key1, _) = &*STANDARD_KEYS;
        let (_, public_key2) = generate_test_keypair();

        let org_id = 123;
        let channel_id = Uuid::new_v4();
        let claims = Claims {
            org_id,
            channel_id,
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key1);

        let result = validate_token(&token, org_id, channel_id, &public_key2);
        assert!(matches!(
            result,
            Err(TokenValidationError::OtherValidationError(_))
        ));
    }

    #[test]
    fn test_token_with_wrong_algorithm() {
        let (_, public_key) = &*STANDARD_KEYS;

        let org_id = 123;
        let channel_id = Uuid::new_v4();
        let claims = Claims {
            org_id,
            channel_id,
            exp: (Utc::now().timestamp() + 3600) as usize,
        };

        let key = EncodingKey::from_secret("secret".as_ref());
        let token = encode(&Header::new(jsonwebtoken::Algorithm::HS256), &claims, &key).unwrap();

        let result = validate_token(&token, org_id, channel_id, public_key);
        assert!(matches!(
            result,
            Err(TokenValidationError::OtherValidationError(_))
        ));
    }
}
