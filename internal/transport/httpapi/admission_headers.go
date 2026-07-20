package httpapi

const maxAdmissionTokenHeaderBytes = 512

func validAdmissionTokenHeader(value string) bool {
	if len(value) == 0 || len(value) > maxAdmissionTokenHeaderBytes {
		return false
	}
	for _, character := range value {
		if character >= 'A' && character <= 'Z' ||
			character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}
