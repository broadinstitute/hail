require("dotenv").config();

const polka = require("polka");
const jwksRsa = require("jwks-rsa");
const jwt = require("jsonwebtoken");

const { AUTH0_DOMAIN, AUTH0_AUDIENCE } = process.env;
const AUTH0_ISSUER = `https://${AUTH0_DOMAIN}/`;

// Grabs public key, caches it, from Auth0's servers
// asymmetric signature used to ensure that not only the jwt was unmodified
// since signed, but that the issuer was Auth0 (since they hold the private key)
const jwksFn = jwksRsa({
  cache: true,
  rateLimit: true,
  jwksRequestsPerMinute: 5,
  jwksUri: `https://${AUTH0_DOMAIN}/.well-known/jwks.json`
});

// Ref: https://github.com/auth0/node-jwks-rsa/blob/master/src/integrations/express.js
function getSecretKey(header, cb) {
  jwksFn.getSigningKey(header.kid, (err, key) => {
    if (err) {
      cb(err, null);
      return;
    }

    cb(null, key.publicKey || key.rsaPublicKey);
  });
}

// Verifies signature, as well as the issuer and audience, to guarantee
// that the token was truly for our resource server
const verifyToken = token =>
  new Promise((resolve, reject) => {
    jwt.verify(
      token,
      getSecretKey,
      {
        issuer: AUTH0_ISSUER,
        audience: AUTH0_AUDIENCE,
        clockTolerance: 2, // seconds to deal with clock skew
        algorithms: ["RS256"]
      },
      (err, dToken) => {
        if (err) {
          console.error(err);
          return reject(err);
        }

        return resolve(dToken);
      }
    );
  });

const bearerPrefix = "Bearer ";
const bearerPrefixLen = bearerPrefix.length;

const cookiePrefix = "access_token=";
const cookieOffset = cookiePrefix.length;

const getAuthToken = req => {
  if (req.query.access_token) {
    return null;
  }

  const bearerIdx = req.headers.authorization
    ? req.headers.authorization.indexOf(bearerPrefix)
    : -1;

  const cookieIdx = req.headers.cookie
    ? req.headers.cookie.indexOf(cookiePrefix)
    : -1;

  if (bearerIdx === -1 && cookieIdx === -1) {
    return null;
  }

  if (bearerIdx > -1 && cookieIdx > -1) {
    console.warn("User specified > 1 access tokens", req.headers.origin);
    return null;
  }

  if (bearerIdx > -1) {
    return req.headers.authorization.substring(bearerPrefixLen);
  }

  const token = req.headers.cookie.substring(cookieIdx + cookieOffset);

  if (!token) {
    return null;
  }

  const spaceIdx = token.indexOf(" ");

  if (spaceIdx === -1) {
    return token;
  }

  // https://stackoverflow.com/questions/4843556/in-http-specification-what-is-the-string-that-separates-cookies
  if (token[spaceIdx - 1] !== ";" && token[spaceIdx - 1] !== ",") {
    return null;
  }

  return token.substring(0, spaceIdx - 1);
};

const PORT = 8000;
polka()
  .get("/verify", (req, res) => {
    const token = getAuthToken(req);

    if (!token) {
      unauthorized(res);
      return;
    }

    verifyToken(token)
      .then(user => {
        res.setHeader("User", user.sub);
        res.setHeader("Scope", user.scope);

        res.end();
      })
      .catch(e => {
        console.error(e);
        unauthorized(res);
      });
  })
  .listen(PORT, err => {
    if (err) {
      throw err;
    }

    console.info(`Auth server running on port ${PORT}`);
  });

function unauthorized(res) {
  res.statusCode = 401;
  res.end();
}
