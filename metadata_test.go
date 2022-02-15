package grpcext

import (
	"testing"
	"reflect"
	"google.golang.org/grpc/metadata"
)

func TestNew(t *testing.T) {
	m := map[string]string{
		"KEY": "value1",
		"Key": "value2",
		"kEy": "value3",
		"keY": "value4",
		"key": "value5",
		"Foo": "bar",
		"foo": "baz",
		"snafu": "fubar",
	}
	md := New(m)

	exp := MD{
		"key":   {"value1", "value2", "value3", "value4", "value5"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
}

func TestNewMulti(t *testing.T) {
	m := map[string][]string{
		"KEY": {"value1", "value1b"},
		"Key": {"value2", "value2b"},
		"kEy": {"value3", "value3b"},
		"keY": {"value4", "value4b"},
		"key": {"value5", "value5b"},
		"Foo": {"bar"},
		"foo": {"baz"},
		"snafu": {"fubar"},
	}
	md := NewMulti(m)

	exp := MD{
		"key":   {"value1", "value1b", "value2", "value2b", "value3", "value3b", "value4", "value4b", "value5", "value5b"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
}

func TestCreate(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")

	exp := MD{
		"key":   {"value1", "value2"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
}

func TestGet(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")

	for _, k := range []string{"KEY", "Key", "kEy", "keY", "key"} {
		eq(t, "value2", md.Get(k), "Wrong value")
	}

	for _, k := range []string{"FOO", "foo"} {
		eq(t, "baz", md.Get(k), "Wrong value")
	}

	eq(t, "fubar", md.Get("snafu"), "Wrong value")

	eq(t, "", md.Get("not here"), "Wrong value")
}

func TestGetFirst(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")

	for _, k := range []string{"KEY", "Key", "kEy", "keY", "key"} {
		eq(t, "value1", md.GetFirst(k), "Wrong value")
	}

	for _, k := range []string{"FOO", "foo"} {
		eq(t, "bar", md.GetFirst(k), "Wrong value")
	}

	eq(t, "fubar", md.GetFirst("snafu"), "Wrong value")

	eq(t, "", md.GetFirst("not here"), "Wrong value")
}

func TestGetAll(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")

	exp := []string{"value1", "value2"}
	for _, k := range []string{"KEY", "Key", "kEy", "keY", "key"} {
		assert(t, reflect.DeepEqual(exp, md.GetAll(k)), "Expecting %s but got %s", exp, md.GetAll(k))
	}

	exp = []string{"bar", "baz"}
	for _, k := range []string{"FOO", "foo"} {
		assert(t, reflect.DeepEqual(exp, md.GetAll(k)), "Expecting %s but got %s", exp, md.GetAll(k))
	}

	exp = []string{"fubar"}
	assert(t, reflect.DeepEqual(exp, md.GetAll("snafu")), "Expecting %s but got %s", exp, md.GetAll("snafu"))

	eq(t, 0, len(md.GetAll("not here")), "Expecting length 0 for invalid key")
}

func TestRemove(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")
	r := md.Remove("KeY")
	assert(t, r, "Remove of present key should return true")
	r = md.Remove("not-here")
	assert(t, !r, "Remove of absent key should return false")

	exp := MD{
		"foo":   {"bar", "baz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
}

func TestRemoveValue(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")
	r := md.RemoveValue("KeY", "value1")
	assert(t, r, "RemoveValue of present key and value should return true")
	r = md.RemoveValue("not-here", "something")
	assert(t, !r, "RemoveValue of absent key and value should return false")
	r = md.RemoveValue("FOO", "BAR") // values are case-sensitive
	assert(t, !r, "RemoveValue of absent key and value should return false")

	exp := MD{
		"key":   {"value2"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)

	r = md.RemoveValue("Foo", "bar")
	assert(t, r, "RemoveValue of present key and value should return true")
	r = md.RemoveValue("foo", "baz")
	assert(t, r, "RemoveValue of present key and value should return true")

	exp = MD{
		"key":   {"value2"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
}

func TestAdd(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")
	md.Add("KEY", "value")
	md.Add("frob", "nitz")

	exp := MD{
		"key":   {"value1", "value2", "value"},
		"foo":   {"bar", "baz"},
		"frob":  {"nitz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
}

func TestSet(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")
	md.Set("KEY", "value")
	md.Set("frob", "nitz")

	exp := MD{
		"key":   {"value"},
		"foo":   {"bar", "baz"},
		"frob":  {"nitz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
}

func TestMerge(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")
	md2 := Create("K", "v", "X", "yz", "kEY", "value3", "SNAFu", "froober")
	md.AddAll(md2)

	exp := MD{
		"k":     {"v"},
		"key":   {"value1", "value2", "value3"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar", "froober"},
		"x":     {"yz"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)

	// md2 should not have been modified
	exp = MD{
		"k":     {"v"},
		"key":   {"value3"},
		"snafu": {"froober"},
		"x":     {"yz"},
	}
	assert(t, reflect.DeepEqual(exp, md2), "Expecting %s but got %s", exp, md2)
}

func TestClone(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")
	md2 := md.Clone()

	md.Remove("key")
	md["foo"][0] = "bro"

	// md2 not affected by mutations to md
	exp := MD{
		"key":   {"value1", "value2"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md2), "Expecting %s but got %s", exp, md2)
}

func TestWith(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")
	md2 := md.With("KEY", "value").With("frob", "nitz")

	exp := MD{
		"key":   {"value1", "value2", "value"},
		"foo":   {"bar", "baz"},
		"frob":  {"nitz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md2), "Expecting %s but got %s", exp, md2)

	// original not modified by calls to With
	exp = MD{
		"key":   {"value1", "value2"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
}

func TestWithReplacement(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")
	md2 := md.WithReplacement("KEY", "value").WithReplacement("frob", "nitz")

	exp := MD{
		"key":   {"value"},
		"foo":   {"bar", "baz"},
		"frob":  {"nitz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md2), "Expecting %s but got %s", exp, md2)

	// original not modified by calls to WithReplacement
	exp = MD{
		"key":   {"value1", "value2"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
}

func TestCombine(t *testing.T) {
	md := Create("key", "value1", "Key", "value2", "foo", "bar", "foo", "baz", "snafu", "fubar")
	md2 := Create("K", "v", "X", "yz", "kEY", "value3", "SNAFu", "froober")
	md3 := md.WithAll(md2)

	exp := MD{
		"k":     {"v"},
		"key":   {"value1", "value2", "value3"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar", "froober"},
		"x":     {"yz"},
	}
	assert(t, reflect.DeepEqual(exp, md3), "Expecting %s but got %s", exp, md3)

	// originals should not have been modified by Combine
	exp = MD{
		"key":   {"value1", "value2"},
		"foo":   {"bar", "baz"},
		"snafu": {"fubar"},
	}
	assert(t, reflect.DeepEqual(exp, md), "Expecting %s but got %s", exp, md)
	exp = MD{
		"k":     {"v"},
		"key":   {"value3"},
		"snafu": {"froober"},
		"x":     {"yz"},
	}
	assert(t, reflect.DeepEqual(exp, md2), "Expecting %s but got %s", exp, md2)
}
