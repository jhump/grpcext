package grpcext

import (
	"fmt"
	"sort"
	"strings"
)

// MD represents metadata that can be accompanied with a GRPC request (as headers)
// or with a response (as headers or trailers). It can be type-converted from
// the GRPC's metadata.MD type, and provides a much fuller API.
type MD map[string][]string

// New creates an MD from the given map of metadata. The given map has a single
// value per key. But if the given map contains multiple entries for the same
// metadata key (e.g. vary only in case), the values will be merged with those
// associated with lesser keys (e.g. collate earlier based on case) coming before
// values associated with greater keys.
func New(m map[string]string) MD {
	md := MD{}

	// sort keys for deterministic results when there are key collisions
	ks := make([]string, len(m))
	i := 0
	for k := range m {
		ks[i] = k
		i++
	}
	sort.Strings(ks)

	for _, k := range ks {
		nk := strings.ToLower(k)
		md[nk] = append(md[nk], m[k])
	}
	return md
}

// NewMulti creates an MD from the given map of metadata. The map may have multiple
// values per key. If the given map contains multiple entries for the same metadata
// key (e.g. vary only in case), the values will be merged with those associated with
// lesser keys (e.g. collate earlier based on case) coming before values associated
// with greater keys.
func NewMulti(m map[string][]string) MD {
	md := MD{}

	// sort keys for deterministic results when there are key collisions
	ks := make([]string, len(m))
	i := 0
	for k := range m {
		ks[i] = k
		i++
	}
	sort.Strings(ks)

	for _, k := range ks {
		nk := strings.ToLower(k)
		md[nk] = append(md[nk], m[k]...)
	}
	return md
}

// Create creates an MD with the given key + value pairs. The arguments must be even
// in number and alternating order of key then value, key then value, etc.
func Create(keysValues ...string) MD {
	if (len(keysValues) & 1) == 1 {
		panic(fmt.Sprintf("number of key+value arguments to Create must be even; got %d", len(keysValues)))
	}
	md := MD{}
	for i := 0; i < len(keysValues); i += 2 {
		k := strings.ToLower(keysValues[i])
		md[k] = append(md[k], keysValues[i+1])
	}
	return md
}

// Combine returns the result of combining this metadata with the given set of metadata.
// Neither of the given metadata objects are modified: a new metadata object is returned.
// If both objects contain a given key, the values for that key are combined with this
// metadata object's values appearing before the given metadata object's values.
func (md MD) Combine(other MD) MD {
	nmd := md.Clone()
	nmd.Merge(other)
	return nmd
}

// Merge adds the given metadata to this object. If the given object contains keys that
// already exist in this metadata, its values are appended to existing values. This is
// just like Combine except that it modifies the current metadata, in place, instead of
// returning a new object.
func (md MD) Merge(other MD) {
	for k, v := range other {
		md[k] = append(md[k], v...)
	}
}

// With returns the contents of the current metadata, but with the given key and value
// added to it. If the given key already exists in this metadata, the returned metadata
// will include them, with the given value appended to them.
func (md MD) With(key, value string) MD {
	nmd := md.Clone()
	nmd.Add(key, value)
	return nmd
}

// Add updates the current metadata to include the given key and value. If values already
// exist for the given key, the given value will be appended to them. This is just like
// With except that it modifies the current metadata, in place, instead of returning a
// new object
func (md MD) Add(key, value string) {
	k := strings.ToLower(key)
	md[k] = append(md[k], value)
}

// WithReplacement returns the contents of the current metadata, but with the given key
// and value set. If the given key already exists in this metadata, the returned metadata
// will have only the given replacement value, ignoring any existing values.
func (md MD) WithReplacement(key, value string) MD {
	nmd := md.Clone()
	nmd.Set(key, value)
	return nmd
}

// Set updates the current metadata to include the given key and value. If values already
// exist for the given key, they will be replaced with the given single value. This is just
// like WithReplacement except that it modifies the current metadata, in place, instead of
// returning a new object.
func (md MD) Set(key, value string) {
	k := strings.ToLower(key)
	md[k] = []string{value}
}

// Clone returns a new copy of this metadata. The returned metadata will be a deep copy
// that shares no storage with this object.
func (md MD) Clone() MD {
	nmd := MD{}
	for k, v := range md {
		vs := make([]string, len(v))
		copy(vs, v)
		nmd[k] = vs
	}
	return nmd
}

// Get returns the value associated with the given key. If there are multiple values then
// only the last value is returned. If there are no values associated with the given key
// then the empty string is returned.
func (md MD) Get(key string) string {
	k := strings.ToLower(key)
	v := md[k]
	if len(v) == 0 {
		return ""
	}
	return v[len(v) - 1]
}

// GetFirst returns the first value associated with the given key. If there are no values
// associated with the given key then the empty string is returned.
func (md MD) GetFirst(key string) string {
	k := strings.ToLower(key)
	v := md[k]
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

// GetAll returns all values associated with the given key.
func (md MD) GetAll(key string) []string {
	k := strings.ToLower(key)
	return md[k]
}

// Remove deletes the given key from this metadata, returning true if it was present and
// removed or false if the key did not exist.
func (md MD) Remove(key string) bool {
	k := strings.ToLower(key)
	_, ok := md[k]
	if ok {
		delete(md, k)
	}
	return ok
}

// RemoveValue deletes the given value from the list of values for the given key from
// this metadata. It returns true if the given key and value were found and removed, or
// false if either the key or value were not found.
func (md MD) RemoveValue(key, val string) bool {
	k := strings.ToLower(key)
	v := md[k]
	found := 0
	for _, vv := range v {
		if vv == val {
			found++
		}
	}
	if found > 0 {
		if found == len(v) {
			delete(md, k)
		} else {
			newv := make([]string, len(v) - found)
			for i, j := 0, 0; i < len(v); i++ {
				vv := v[i]
				if vv != val {
					newv[j] = vv
					j++
				}
			}
			md[k] = newv
		}
	}
	return found > 0
}
