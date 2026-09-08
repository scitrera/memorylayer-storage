// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fusebridge

import (
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/chunkstore"
)

func TestClassifyStoreClass(t *testing.T) {
	const (
		def = uint8(chunkstore.ClassDefault)
		unc = uint8(chunkstore.ClassUncompressed)
	)
	cases := []struct {
		name string
		dflt uint8
		want uint8
	}{
		// Recognized model/tensor types are always uncompressed, regardless of the
		// mount default.
		{"model.safetensors", def, unc},
		{"weights.PT", def, unc},        // case-insensitive
		{"pytorch_model.bin", def, unc}, // HF shard
		{"a.npy", def, unc},
		{"m.gguf", def, unc},
		{"shard.onnx", def, unc},
		// Generic files take the mount default.
		{"notes.txt", def, def},
		{"data.json", def, def},
		{"noext", def, def},
		// A model-store mount (default uncompressed) keeps unrecognized files
		// uncompressed too.
		{"notes.txt", unc, unc},
		{"archive.zip", unc, unc},
	}
	for _, c := range cases {
		if got := classifyStoreClass(c.name, c.dflt); got != c.want {
			t.Errorf("classifyStoreClass(%q, dflt=%d) = %d, want %d", c.name, c.dflt, got, c.want)
		}
	}
}
