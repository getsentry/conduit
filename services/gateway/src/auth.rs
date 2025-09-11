use jsonwebtoken::{DecodingKey, Validation, decode, errors::ErrorKind};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub org_id: String,
    pub channel_id: String,
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
    OrgIdMismatch { expected: String, actual: String },

    #[error("Channel ID mismatch: expected {expected}, got {actual}")]
    ChannelIdMismatch { expected: String, actual: String },
}

pub fn validate_token(
    token: &str,
    org_id: &str,
    channel_id: &str,
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
            expected: org_id.to_string(),
            actual: claims.org_id.to_string(),
        });
    }

    if claims.channel_id != channel_id {
        return Err(TokenValidationError::ChannelIdMismatch {
            expected: channel_id.to_string(),
            actual: claims.channel_id.to_string(),
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
        let claims = Claims {
            org_id: "org123".to_string(),
            channel_id: "channel456".to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key);

        let result = validate_token(&token, "org123", "channel456", public_key);
        assert!(result.is_ok());

        let validated_claims = result.unwrap();
        assert_eq!(validated_claims.org_id, "org123");
        assert_eq!(validated_claims.channel_id, "channel456");
    }

    #[test]
    fn test_expired_token() {
        let (private_key, public_key) = &*STANDARD_KEYS;
        let claims = Claims {
            org_id: "org123".to_string(),
            channel_id: "channel456".to_string(),
            exp: (Utc::now().timestamp() - 3600) as usize, // 1 hour ago
        };
        let token = create_test_token(&claims, private_key);

        let result = validate_token(&token, "org123", "channel456", public_key);
        assert!(matches!(result, Err(TokenValidationError::TokenExpired)));
    }

    #[test]
    fn test_invalid_org_id() {
        let (private_key, public_key) = &*STANDARD_KEYS;
        let claims = Claims {
            org_id: "org123".to_string(),
            channel_id: "channel456".to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key);

        let result = validate_token(&token, "wrong_org", "channel456", public_key);
        assert!(matches!(
            result,
            Err(TokenValidationError::OrgIdMismatch { expected, actual })
            if expected == "wrong_org" && actual == "org123"
        ));
    }

    #[test]
    fn test_invalid_channel_id() {
        let (private_key, public_key) = &*STANDARD_KEYS;
        let claims = Claims {
            org_id: "org123".to_string(),
            channel_id: "channel456".to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key);

        let result = validate_token(&token, "org123", "wrong_channel", public_key);
        assert!(matches!(
            result,
            Err(TokenValidationError::ChannelIdMismatch { expected, actual })
            if expected == "wrong_channel" && actual == "channel456"
        ));
    }

    #[test]
    fn test_malformed_token() {
        let (_, public_key) = &*STANDARD_KEYS;

        let result = validate_token("not.a.valid.token", "org123", "channel456", public_key);
        assert!(matches!(
            result,
            Err(TokenValidationError::OtherValidationError(_))
        ));
    }

    #[test]
    fn test_invalid_public_key() {
        let (private_key, _) = &*STANDARD_KEYS;
        let claims = Claims {
            org_id: "org123".to_string(),
            channel_id: "channel456".to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key);

        let result = validate_token(&token, "org123", "channel456", "invalid_key");
        assert!(matches!(
            result,
            Err(TokenValidationError::InvalidPublicKey(_))
        ));
    }

    #[test]
    fn test_wrong_public_key() {
        let (private_key1, _) = &*STANDARD_KEYS;
        let (_, public_key2) = generate_test_keypair();

        let claims = Claims {
            org_id: "org123".to_string(),
            channel_id: "channel456".to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };
        let token = create_test_token(&claims, private_key1);

        let result = validate_token(&token, "org123", "channel456", &public_key2);
        assert!(matches!(
            result,
            Err(TokenValidationError::OtherValidationError(_))
        ));
    }

    #[test]
    fn test_token_with_wrong_algorithm() {
        let (_, public_key) = &*STANDARD_KEYS;

        let claims = Claims {
            org_id: "org123".to_string(),
            channel_id: "channel456".to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };

        let key = EncodingKey::from_secret("secret".as_ref());
        let token = encode(&Header::new(jsonwebtoken::Algorithm::HS256), &claims, &key).unwrap();

        let result = validate_token(&token, "org123", "channel456", public_key);
        assert!(matches!(
            result,
            Err(TokenValidationError::OtherValidationError(_))
        ));
    }
}
