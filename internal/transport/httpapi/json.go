package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
)

const defaultMaxRequestBodyBytes int64 = 1 << 20

func decodeJSON(c *gin.Context, limit int64, destination any) error {
	if limit <= 0 || limit > defaultMaxRequestBodyBytes {
		limit = defaultMaxRequestBodyBytes
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, limit)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return errRequestTooLarge
		}
		return ErrInvalidInput
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalidInput
	}
	return nil
}

var errRequestTooLarge = errors.New("request body too large")

func writeDecodeError(c *gin.Context, err error) {
	if errors.Is(err, errRequestTooLarge) {
		writePublicError(c, http.StatusRequestEntityTooLarge, "request_too_large", "request body is too large")
		return
	}
	writeError(c, err)
}
