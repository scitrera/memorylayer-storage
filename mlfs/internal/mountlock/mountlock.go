// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Package mountlock provides a single-node "is a daemon live?" guard built on a
// PID lock file in the data directory. The mlfs daemon writes the file on mount
// and removes it on clean shutdown; offline tools (mlfs-admin fsck/gc) refuse to
// run while a matching live daemon holds it, because those tools assume a
// quiesced filesystem — run against a LIVE mount they race the daemon and can
// reap a legitimately open-but-unlinked inode (sustained only in the daemon's
// memory), losing data.
//
// Single-node scope: there is exactly one mount per data-dir, so one PID file is
// sufficient. Multi-node coordination is a later phase.
package mountlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// FileName is the lock file's name, placed at the top level of the data
// directory. The "mlfs." prefix and the data-dir top level keep it clear of
// casstore's own contents under data/.
const FileName = "mlfs.lock"

// Path returns the lock file path for a given data directory.
func Path(dataDir string) string { return filepath.Join(dataDir, FileName) }

// Write records the current process's PID in the data-dir lock file, creating
// the directory if needed. The daemon calls this once after a successful mount.
func Write(dataDir string) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	p := Path(dataDir)
	if err := os.WriteFile(p, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		return fmt.Errorf("write lock file %s: %w", p, err)
	}
	return nil
}

// Remove deletes the lock file. The daemon calls this on clean shutdown. A
// missing file is not an error (it may have already been removed or never
// written).
func Remove(dataDir string) error {
	err := os.Remove(Path(dataDir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// LiveError describes a refusal: a live daemon holds the data-dir lock.
type LiveError struct {
	Path string
	PID  int
}

func (e *LiveError) Error() string {
	return fmt.Sprintf("a live mlfs daemon (pid %d) holds %s; "+
		"stop the daemon before running this offline operation "+
		"(or pass -force-online if you are certain no mount is active — DANGEROUS, can cause data loss)",
		e.PID, e.Path)
}

// CheckQuiesced verifies no live daemon holds the data-dir lock and returns an
// error if one does. Behavior:
//
//   - No lock file: quiesced, returns nil (warn is not called).
//   - Lock file present, PID alive: returns *LiveError (refuse).
//   - Lock file present but unparseable, or PID dead: stale; warn is called with
//     a human-readable reason and nil is returned (the caller proceeds and may
//     rewrite/remove the file).
//
// warn may be nil. force, when true, downgrades a live holder to a warning and
// returns nil — for operators who know the daemon is not actually running.
func CheckQuiesced(dataDir string, force bool, warn func(string)) error {
	p := Path(dataDir)
	data, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil // no lock file → quiesced
	}
	if err != nil {
		return fmt.Errorf("read lock file %s: %w", p, err)
	}

	pid, perr := strconv.Atoi(strings.TrimSpace(string(data)))
	if perr != nil {
		if warn != nil {
			warn(fmt.Sprintf("ignoring unparseable lock file %s (%v); proceeding", p, perr))
		}
		return nil // can't trust it → treat as stale
	}

	if !processAlive(pid) {
		if warn != nil {
			warn(fmt.Sprintf("ignoring stale lock file %s (pid %d not running); proceeding", p, pid))
		}
		return nil
	}

	if force {
		if warn != nil {
			warn(fmt.Sprintf("-force-online: overriding live daemon (pid %d) holding %s; "+
				"this can cause data loss if a mount is active", pid, p))
		}
		return nil
	}
	return &LiveError{Path: p, PID: pid}
}

// processAlive reports whether a process with the given PID exists, using the
// POSIX kill(pid, 0) liveness probe: signal 0 performs error checking only. A
// nil error or EPERM (the process exists but we may not signal it) means alive;
// ESRCH means no such process.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	return errors.Is(err, syscall.EPERM)
}
