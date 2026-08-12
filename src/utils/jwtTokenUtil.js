const jwt = require('jsonwebtoken');
const config = require('../config/env');
const logger = require('../utils/logger');

/**
 * Builds signed JWTs for the gateway message-routing demo, mirrors
 * com.antontech.itemkafka_poc.util.JwtTokenUtil. Supports either a real RSA
 * private key (PEM, via ITEM_JWT_PRIVATE_KEY) for RS256, or falls back to an
 * HS256 shared-secret signature (using the same env value) purely so the
 * demo endpoint works out of the box with no key configured.
 */
class JwtTokenUtil {
  /**
   * @param {object} claims - extra claims to embed in the token payload.
   * @returns {string} a signed JWT.
   */
  generateToken(claims = {}) {
    const now = Math.floor(Date.now() / 1000);
    const payload = {
      ...claims,
      iss: config.jwt.issuer,
      iat: now,
      exp: now + config.jwt.expiryMinutes * 60,
    };

    if (config.jwt.privateKey && config.jwt.privateKey.includes('BEGIN')) {
      try {
        return jwt.sign(payload, config.jwt.privateKey, { algorithm: 'RS256' });
      } catch (err) {
        logger.error(`RS256 signing failed, falling back to HS256: ${err.message}`);
      }
    }

    const secret = config.jwt.privateKey || 'dev-only-insecure-secret-change-me';
    return jwt.sign(payload, secret, { algorithm: 'HS256' });
  }
}

module.exports = new JwtTokenUtil();

