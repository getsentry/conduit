use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, errors::ErrorKind};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub iss: String,
    pub aud: String,
    pub exp: usize,
}

#[derive(Error, Debug)]
pub enum JwtValidationError {
    #[error("Token has expired")]
    TokenExpired,

    #[error("Token validation failed: {0}")]
    ValidationFailed(String),
}

pub fn validate_token(
    token: &str,
    secret: &[u8],
    expected_issuer: &str,
    expected_audience: &str,
) -> Result<Claims, JwtValidationError> {
    let key = DecodingKey::from_secret(secret);
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[expected_issuer]);
    validation.set_audience(&[expected_audience]);

    let token_data = decode::<Claims>(token, &key, &validation).map_err(|e| match e.kind() {
        ErrorKind::ExpiredSignature => JwtValidationError::TokenExpired,
        _ => JwtValidationError::ValidationFailed(e.to_string()),
    })?;

    Ok(token_data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use jsonwebtoken::{EncodingKey, Header, encode};

    fn create_test_token(claims: &Claims, secret: &[u8]) -> String {
        let key = EncodingKey::from_secret(secret);
        encode(&Header::new(Algorithm::HS256), claims, &key).unwrap()
    }

    #[test]
    fn test_valid_token() {
        let secret = b"test_secret";
        let iss = "test-issuer";
        let aud = "test-audience";

        let claims = Claims {
            sub: "client-123".to_string(),
            iss: iss.to_string(),
            aud: aud.to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };

        let token = create_test_token(&claims, secret);
        let result = validate_token(&token, secret, iss, aud);

        assert!(result.is_ok());
        let validated = result.unwrap();
        assert_eq!(validated.sub, "client-123");
    }

    #[test]
    fn test_expired_token() {
        let secret = b"test_secret";
        let iss = "test-issuer";
        let aud = "test-audience";

        let claims = Claims {
            sub: "client-123".to_string(),
            iss: iss.to_string(),
            aud: aud.to_string(),
            exp: (Utc::now().timestamp() - 3600) as usize,
        };

        let token = create_test_token(&claims, secret);
        let result = validate_token(&token, secret, iss, aud);

        assert!(matches!(result, Err(JwtValidationError::TokenExpired)));
    }

    #[test]
    fn test_invalid_issuer() {
        let secret = b"test_secret";
        let iss = "test-issuer";
        let aud = "test-audience";

        let claims = Claims {
            sub: "client-123".to_string(),
            iss: iss.to_string(),
            aud: aud.to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };

        let token = create_test_token(&claims, secret);
        let result = validate_token(&token, secret, "wrong-issuer", aud);

        assert!(matches!(
            result,
            Err(JwtValidationError::ValidationFailed(_))
        ));
    }

    #[test]
    fn test_invalid_audience() {
        let secret = b"test_secret";
        let iss = "test-issuer";
        let aud = "test-audience";

        let claims = Claims {
            sub: "client-123".to_string(),
            iss: iss.to_string(),
            aud: aud.to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };

        let token = create_test_token(&claims, secret);
        let result = validate_token(&token, secret, iss, "wrong-audience");

        assert!(matches!(
            result,
            Err(JwtValidationError::ValidationFailed(_))
        ));
    }

    #[test]
    fn test_wrong_secret() {
        let secret = b"test_secret";
        let wrong_secret = b"wrong_secret";
        let iss = "test-issuer";
        let aud = "test-audience";

        let claims = Claims {
            sub: "client-123".to_string(),
            iss: iss.to_string(),
            aud: aud.to_string(),
            exp: (Utc::now().timestamp() + 3600) as usize,
        };

        let token = create_test_token(&claims, secret);
        let result = validate_token(&token, wrong_secret, iss, aud);

        assert!(matches!(
            result,
            Err(JwtValidationError::ValidationFailed(_))
        ));
    }

    #[test]
    fn test_malformed_token() {
        let secret = b"test_secret";
        let result = validate_token("not.a.valid.token", secret, "iss", "aud");

        assert!(matches!(
            result,
            Err(JwtValidationError::ValidationFailed(_))
        ));
    }
}
