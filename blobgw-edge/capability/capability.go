// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package capability implements blobgw-edge's stateless, asymmetrically-signed
// capability tokens.
//
// A capability is a short-lived, Ed25519-signed grant for ONE operation on ONE
// ref within ONE tenant, mintable and verifiable without any per-request
// callback to the auth proxy — required because the upload/download URL may be
// hit directly by a browser. Tokens are minted and verified at the edge;
// internal blobgw never sees them (it trusts the edge over the internal
// network). Ed25519 buys clean key rotation and lets a second verifier
// validate independently if ever needed.
//
// Wire format: “<base64url(claims_json)>.<base64url(signature)>“. The
// signature covers the exact claims bytes; the key id (“kid“) inside the
// claims selects the public key, so keys rotate without breaking outstanding
// tokens.
package capability

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Version is the current claim-set version. Verifiers reject other versions.
const Version = 1

// Op is the single operation a capability authorizes.
type Op string

const (
	OpPut    Op = "PUT"
	OpGet    Op = "GET"
	OpHead   Op = "HEAD"
	OpDelete Op = "DELETE"
	// OpStage and OpFinalize authorize the two halves of a presigned
	// stage-then-finalize upload. Unlike the four above they are NOT HTTP
	// methods: a Stage capability authorizes minting a presigned staging upload
	// for exactly (tenant, ref) (POST /staged/{ref}), and a Finalize capability
	// authorizes finalizing that staged upload for exactly (tenant, ref) (POST
	// /finalize/{ref}). The data path matches them against the route's REQUIRED
	// op, not r.Method, since both routes are POST.
	OpStage    Op = "STAGE"
	OpFinalize Op = "FINALIZE"
)

// Claims is the capability's grant. Field tags are short to keep tokens small.
type Claims struct {
	Ver         int    `json:"ver"`
	KeyID       string `json:"kid"`
	Tenant      string `json:"ten"`           // dedup + isolation domain
	Subject     string `json:"sub"`           // user/principal the proxy asserted
	Op          Op     `json:"op"`            // exactly one of PUT/GET/HEAD/DELETE
	Ref         string `json:"ref"`           // exact object ref (within the tenant)
	MaxSize     int64  `json:"max,omitempty"` // upload size ceiling (PUT); 0 = unset
	ContentType string `json:"ct,omitempty"`  // required content type, if set
	ContentHash string `json:"ch,omitempty"`  // expected whole-object hash, if set
	NotBefore   int64  `json:"nbf"`           // unix seconds
	Expiry      int64  `json:"exp"`           // unix seconds
	JTI         string `json:"jti"`           // unique id, for optional revocation
	Audience    string `json:"aud"`           // intended edge identity

	// RequireAuth binds this capability to its authenticated owner: when set, the
	// data path must ALSO verify the live asserted principal (identity headers
	// stamped by the auth proxy) matches Tenant/Subject per MatchMode. When false,
	// the URL is shareable — a pure bearer, principal-irrelevant, current behavior.
	// The minter ALWAYS stamps this explicitly, so a verifier never infers a
	// default from an absent field. JSON-additive (Ver stays 1): older verifiers
	// unmarshal-ignore it.
	RequireAuth bool `json:"ra,omitempty"`
	// MatchMode is how the live principal is matched when RequireAuth is set:
	// "exact" (tenant+subject) or "same-tenant" (tenant only). Empty/omitted is
	// treated as "exact". Only meaningful when RequireAuth; the minter forces it
	// empty for shareable tokens. JSON-additive; older verifiers ignore it.
	MatchMode string `json:"mm,omitempty"`
}

// Sentinel verification errors.
var (
	ErrMalformed    = errors.New("capability: malformed token")
	ErrVersion      = errors.New("capability: unsupported version")
	ErrUnknownKey   = errors.New("capability: unknown key id")
	ErrBadSignature = errors.New("capability: bad signature")
	ErrExpired      = errors.New("capability: token expired")
	ErrNotYetValid  = errors.New("capability: token not yet valid")
	ErrBadAudience  = errors.New("capability: wrong audience")
)

// Minter signs capabilities with one Ed25519 private key under a key id.
type Minter struct {
	kid      string
	priv     ed25519.PrivateKey
	audience string
	now      func() time.Time
}

// NewMinter constructs a Minter. audience is this edge's identity, stamped on
// every token and checked by the verifier.
func NewMinter(kid string, priv ed25519.PrivateKey, audience string) *Minter {
	return &Minter{kid: kid, priv: priv, audience: audience, now: time.Now}
}

// SetClock overrides the clock (tests).
func (m *Minter) SetClock(now func() time.Time) { m.now = now }

// Mint fills the protocol fields on c (version, key id, audience, not-before,
// expiry from ttl, and a random jti when absent), signs it, and returns the
// encoded token plus the finalized claims.
func (m *Minter) Mint(c Claims, ttl time.Duration) (string, Claims, error) {
	now := m.now().UTC()
	c.Ver = Version
	c.KeyID = m.kid
	c.Audience = m.audience
	if c.NotBefore == 0 {
		c.NotBefore = now.Add(-30 * time.Second).Unix() // small skew tolerance
	}
	c.Expiry = now.Add(ttl).Unix()
	if c.JTI == "" {
		c.JTI = newJTI()
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", Claims{}, fmt.Errorf("capability: marshal claims: %w", err)
	}
	sig := ed25519.Sign(m.priv, payload)
	token := b64(payload) + "." + b64(sig)
	return token, c, nil
}

// Verifier validates capability signatures (by key id) and temporal/audience
// claims. Operation/ref/tenant/size matching against the actual request is the
// caller's job (see the edge), keeping this package about authenticity only.
type Verifier struct {
	keys     map[string]ed25519.PublicKey
	audience string
	now      func() time.Time
}

// NewVerifier constructs a Verifier for the given audience (this edge's id).
func NewVerifier(audience string) *Verifier {
	return &Verifier{keys: make(map[string]ed25519.PublicKey), audience: audience, now: time.Now}
}

// AddKey registers a public key under a key id. Register multiple to verify
// tokens minted across a rotation.
func (v *Verifier) AddKey(kid string, pub ed25519.PublicKey) { v.keys[kid] = pub }

// SetClock overrides the clock (tests).
func (v *Verifier) SetClock(now func() time.Time) { v.now = now }

// Verify decodes and authenticates a token: signature (by kid), version,
// audience, and the not-before / expiry window. It returns the validated
// claims; a non-nil error means the token must be rejected outright.
func (v *Verifier) Verify(token string) (Claims, error) {
	dot := strings.IndexByte(token, '.')
	if dot <= 0 || dot == len(token)-1 {
		return Claims{}, ErrMalformed
	}
	payload, err := unb64(token[:dot])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	sig, err := unb64(token[dot+1:])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	var c Claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Claims{}, ErrMalformed
	}
	if c.Ver != Version {
		return Claims{}, ErrVersion
	}
	pub, ok := v.keys[c.KeyID]
	if !ok {
		return Claims{}, ErrUnknownKey
	}
	if !ed25519.Verify(pub, payload, sig) {
		return Claims{}, ErrBadSignature
	}
	if c.Audience != v.audience {
		return Claims{}, ErrBadAudience
	}
	now := v.now().UTC().Unix()
	if now < c.NotBefore {
		return Claims{}, ErrNotYetValid
	}
	if now >= c.Expiry {
		return Claims{}, ErrExpired
	}
	return c, nil
}

func b64(b []byte) string            { return base64.RawURLEncoding.EncodeToString(b) }
func unb64(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }

func newJTI() string {
	buf := make([]byte, 12)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

// GenerateKey is a convenience for dev/tests: a fresh Ed25519 keypair.
func GenerateKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}
