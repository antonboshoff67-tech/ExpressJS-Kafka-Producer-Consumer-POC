const cors = require('cors');
const config = require('./env');

/**
 * CORS middleware factory, mirrors CorsConfig.java. Allows the React front
 * end (typically Vite on http://localhost:5173) to call this API from the
 * browser. Allowed origins are externalised via ITEM_CORS_ALLOWED_ORIGINS.
 */
module.exports = cors({
  origin: config.cors.allowedOrigins,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
  allowedHeaders: '*',
  exposedHeaders: ['Authorization'],
  credentials: false,
});

