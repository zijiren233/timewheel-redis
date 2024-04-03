package timewheel

import "unsafe"

// The change of bytes will cause the change of string synchronously
func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// If string is readonly, modifying bytes will cause panic
func stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

func splitIntoBatches[T any](s []T, batchSize int) [][]T {
	var batches [][]T = make([][]T, 0, len(s)/batchSize+1)

	for batchSize < len(s) {
		batch, remainder := s[:batchSize], s[batchSize:]
		batches = append(batches, batch)
		s = remainder
	}

	// Add the last batch if it has any items
	if len(s) != 0 {
		batches = append(batches, s)
	}

	return batches
}
