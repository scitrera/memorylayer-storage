// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

package fileio

import (
	"reflect"
	"testing"

	"github.com/scitrera/memorylayer-storage/mlfs/internal/meta"
)

func sr(pos uint32, id uint64, off, ln uint32) meta.SliceRef {
	return meta.SliceRef{Pos: pos, Slice: meta.Slice{Id: id, Size: ln, Off: off, Len: ln}}
}

func TestResolveSingleSlice(t *testing.T) {
	got := resolve([]meta.SliceRef{sr(0, 1, 0, 100)}, 0, 100)
	want := []seg{{0, 100, 1, 0}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestResolveNewerWins(t *testing.T) {
	// s1 covers [0,100); s2 (newer) overwrites [40,60).
	got := resolve([]meta.SliceRef{sr(0, 1, 0, 100), sr(40, 2, 0, 20)}, 0, 100)
	want := []seg{
		{0, 40, 1, 0},
		{40, 60, 2, 0},
		{60, 100, 1, 60}, // remaining s1 piece sources from its offset 60
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
}

func TestResolveHole(t *testing.T) {
	got := resolve([]meta.SliceRef{sr(0, 1, 0, 20)}, 0, 100)
	want := []seg{{0, 20, 1, 0}, {20, 100, 0, 0}} // tail is a hole
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestResolveWindow(t *testing.T) {
	// Reading a sub-window [30,70) of a full-chunk slice.
	got := resolve([]meta.SliceRef{sr(0, 1, 0, 100)}, 30, 70)
	want := []seg{{30, 70, 1, 30}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v want %+v", got, want)
	}
}

func TestResolveThreeLayered(t *testing.T) {
	// s1 [0,100), s2 [20,80), s3 [40,60): newest wins in the middle.
	got := resolve([]meta.SliceRef{sr(0, 1, 0, 100), sr(20, 2, 0, 60), sr(40, 3, 0, 20)}, 0, 100)
	want := []seg{
		{0, 20, 1, 0},
		{20, 40, 2, 0},
		{40, 60, 3, 0},
		{60, 80, 2, 40},
		{80, 100, 1, 80},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
}
