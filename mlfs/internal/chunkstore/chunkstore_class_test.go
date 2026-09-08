// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package chunkstore

import (
	"testing"

	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

// TestMetaForClass pins the class→casstore-metadata mapping that makes the
// uncompressed guarantee a per-slice property: ClassUncompressed stamps the
// store_uncompressed tag (which casstore's policy honors first, forcing
// CompressNone regardless of the global codec), while ClassDefault carries no
// signal so the backing CompressionPolicy decides.
func TestMetaForClass(t *testing.T) {
	if got := metaForClass(ClassUncompressed); got.Tags[snapshot.TagStoreUncompressed] == "" {
		t.Errorf("ClassUncompressed must stamp %q; got tags %v", snapshot.TagStoreUncompressed, got.Tags)
	}
	if got := metaForClass(ClassDefault); len(got.Tags) != 0 {
		t.Errorf("ClassDefault must carry no tags (policy decides); got %v", got.Tags)
	}
}
