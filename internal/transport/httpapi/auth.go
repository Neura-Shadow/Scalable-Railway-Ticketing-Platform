package httpapi

import (
	"strings"

	"github.com/gin-gonic/gin"
)

const identityContextKey = "httpapi.identity"

func authenticate(parser BearerTokenParser) gin.HandlerFunc {
	return func(c *gin.Context) {
		fields := strings.Fields(c.GetHeader("Authorization"))
		if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") || fields[1] == "" {
			writeError(c, ErrUnauthenticated)
			return
		}
		if parser == nil {
			writeError(c, ErrUnavailable)
			return
		}
		identity, err := parser.ParseAccessToken(c.Request.Context(), fields[1])
		if err != nil || !identity.valid() {
			writeError(c, ErrUnauthenticated)
			return
		}
		c.Set(identityContextKey, identity)
		c.Next()
	}
}

func authorize(roles ...Role) gin.HandlerFunc {
	allowed := make(map[Role]struct{}, len(roles))
	for _, role := range roles {
		allowed[role] = struct{}{}
	}
	return func(c *gin.Context) {
		identity, ok := identityFromContext(c)
		if !ok {
			writeError(c, ErrUnauthenticated)
			return
		}
		if _, ok := allowed[identity.Role]; !ok {
			writeError(c, ErrForbidden)
			return
		}
		c.Next()
	}
}

func (i Identity) valid() bool {
	if strings.TrimSpace(i.Subject) == "" {
		return false
	}
	switch i.Role {
	case RoleCustomer, RoleAdmin, RoleOperator:
		return true
	default:
		return false
	}
}

func identityFromContext(c *gin.Context) (Identity, bool) {
	value, ok := c.Get(identityContextKey)
	if !ok {
		return Identity{}, false
	}
	identity, ok := value.(Identity)
	return identity, ok && identity.valid()
}
