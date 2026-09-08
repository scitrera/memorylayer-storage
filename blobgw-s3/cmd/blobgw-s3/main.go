// Copyright 2026 Scitrera LLC
// SPDX-License-Identifier: AGPL-3.0-only

// Command blobgw-s3 runs the S3-compatible front end over the internal blobgw
// object gateway. Off-the-shelf S3 clients (aws-cli, aws-sdk-go-v2) get
// casstore's content-addressed dedup and transparent compression for free.
//
// The single binary serves local dev and single-node production: a
// local-filesystem casstore stack (manifests + chunks under -data-dir), an
// in-memory dedup/ref index, and a credential store loaded from a JSON file
// (-credentials) that maps each access key to its secret and dedup domain.
// Buckets are explicit: a client must CreateBucket before any object op, and a
// bucket is pinned at creation to the creating credential's dedup domain. This
// keeps the bucket→domain mapping stable and gives a clean NoSuchBucket for
// object ops against a never-created bucket.
//
// Production swaps the local backends for S3 chunk storage plus Postgres-backed
// dedup/ref/credential/bucket stores; the wiring here is the local-fs profile
// that the integration smoke and dev workflows exercise. See README.md.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/scitrera/memorylayer-storage/blobgw-s3/s3"
	"github.com/scitrera/memorylayer-storage/casstore/snapshot"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	addr := flag.String("listen", ":8095", "HTTP listen address")
	dataDir := flag.String("data-dir", "./blobgw-s3-data", "local casstore backend directory (manifests + chunks)")
	compression := flag.String("compression", "zstd", "pack-blob compression for compressible content types: zstd|gzip|none (none disables compression entirely — rollback)")
	packFraming := flag.String("pack-framing", "per-chunk", "framing for COMPRESSED packs: per-chunk (each chunk compressed independently behind an in-pack index, so compressed packs stay range-readable) | whole-pack (one codec stream per pack: better ratio, but any chunk read fetches+decompresses the whole pack — rollback). No effect on packs stored uncompressed (tensor/media classes)")
	packSize := flag.Int("pack-size", 0, "casstore pack target in bytes (0 = default ~16 MiB)")
	maxObjectSize := flag.Int64("max-object-size", 0, "PutObject body ceiling in bytes (0 = unlimited)")
	gcInterval := flag.Duration("gc-interval", 10*time.Minute, "chunked-GC interval (0 disables)")
	gcSafetyWindow := flag.Duration("gc-safety-window", time.Hour, "do not GC blobs younger than this (guards the GC↔in-flight-write race)")
	credentialsPath := flag.String("credentials", os.Getenv("BLOBGW_S3_CREDENTIALS"), "path to the JSON credentials file (access key → {secret, domain}); defaults to $BLOBGW_S3_CREDENTIALS")
	registryBackend := flag.String("registry-backend", "memory", "bucket/credential/MPU registry backend: memory (default, in-process) | jetstream (NATS JetStream KV, survives restart — ADR-001 #37)")
	natsURL := flag.String("nats-url", nats.DefaultURL, "NATS server URL (used when -registry-backend=jetstream)")
	bucketKVBucket := flag.String("bucket-kv", s3.DefaultBucketKVBucket, "JetStream KV bucket name for the bucket registry (jetstream backend)")
	credentialKVBucket := flag.String("credential-kv", s3.DefaultCredentialKVBucket, "JetStream KV bucket name for the credential registry (jetstream backend)")
	mpuKVBucket := flag.String("mpu-kv", s3.DefaultMPUKVBucket, "JetStream KV bucket name for the in-flight multipart-upload registry (jetstream backend)")
	flag.Parse()

	if *credentialsPath == "" {
		slog.Error("blobgw-s3: -credentials is required (or $BLOBGW_S3_CREDENTIALS): path to the JSON access-key→{secret,domain} file")
		os.Exit(1)
	}

	codec, err := snapshot.ParseCompressionAlgo(*compression)
	if err != nil {
		slog.Error("blobgw-s3: parse -compression", "err", err)
		os.Exit(1)
	}

	framing, err := snapshot.ParsePackCompressionMode(*packFraming)
	if err != nil {
		slog.Error("blobgw-s3: parse -pack-framing", "err", err)
		os.Exit(1)
	}

	router, err := newLocalRouter(*dataDir, *packSize, codec, framing)
	if err != nil {
		slog.Error("blobgw-s3: local stack init", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Select the registry backend. The bucket registry switches on the bucket
	// lifecycle ops (CreateBucket / HeadBucket / DeleteBucket / ListBuckets) and
	// bucket-existence enforcement: an object op against an unregistered bucket
	// yields NoSuchBucket, so clients must CreateBucket first. CreateBucket pins
	// the bucket to the calling credential's dedup domain.
	//
	//   - memory (default): in-process registries; state is lost on restart. This
	//     preserves the original single-binary dev/single-node behavior exactly.
	//   - jetstream: bucket/credential/MPU registries are backed by NATS
	//     JetStream KV so they survive a restart (ADR-001 #37). blobgw-s3 is OUT
	//     of the mlfs hot path (nodes go direct to S3), so this only matters for
	//     blobgw-s3's external-endpoint / backup role.
	cfg := s3.Config{Router: router, MaxObject: *maxObjectSize}
	switch *registryBackend {
	case "memory":
		creds, err := loadCredentials(*credentialsPath)
		if err != nil {
			slog.Error("blobgw-s3: load credentials", "path", *credentialsPath, "err", err)
			os.Exit(1)
		}
		cfg.Creds = creds
		cfg.Buckets = s3.NewMemoryBucketRegistry()
	case "jetstream":
		creds, err := parseCredentials(*credentialsPath)
		if err != nil {
			slog.Error("blobgw-s3: load credentials", "path", *credentialsPath, "err", err)
			os.Exit(1)
		}
		nc, err := nats.Connect(*natsURL)
		if err != nil {
			slog.Error("blobgw-s3: connect NATS", "url", *natsURL, "err", err)
			os.Exit(1)
		}
		defer nc.Close()
		js, err := jetstream.New(nc)
		if err != nil {
			slog.Error("blobgw-s3: jetstream", "err", err)
			os.Exit(1)
		}
		credStore, err := s3.NewJetStreamCredentialStore(ctx, js, *credentialKVBucket)
		if err != nil {
			slog.Error("blobgw-s3: open credential KV", "bucket", *credentialKVBucket, "err", err)
			os.Exit(1)
		}
		// Seed/refresh the durable credential registry from the file. Upsert is
		// idempotent, so this is safe to run on every boot.
		for _, c := range creds {
			if err := credStore.Put(ctx, c); err != nil {
				slog.Error("blobgw-s3: seed credential", "access_key_id", c.AccessKeyID, "err", err)
				os.Exit(1)
			}
		}
		bucketReg, err := s3.NewJetStreamBucketRegistry(ctx, js, *bucketKVBucket)
		if err != nil {
			slog.Error("blobgw-s3: open bucket KV", "bucket", *bucketKVBucket, "err", err)
			os.Exit(1)
		}
		mpuStore, err := s3.NewJetStreamMPUStore(ctx, js, *mpuKVBucket)
		if err != nil {
			slog.Error("blobgw-s3: open MPU KV", "bucket", *mpuKVBucket, "err", err)
			os.Exit(1)
		}
		cfg.Creds = credStore
		cfg.Buckets = bucketReg
		cfg.MPU = mpuStore
	default:
		slog.Error("blobgw-s3: unknown -registry-backend (want memory|jetstream)", "backend", *registryBackend)
		os.Exit(1)
	}

	handler := s3.New(cfg)

	gc := router.gc(*gcSafetyWindow)
	if *gcInterval > 0 {
		go gc.Loop(ctx, *gcInterval)
	}

	httpSrv := &http.Server{Addr: *addr, Handler: handler}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("blobgw-s3: shutdown", "err", err)
		}
	}()

	slog.Info("blobgw-s3: listening",
		"addr", *addr, "data_dir", *dataDir, "compression", codec, "pack_framing", framing,
		"credentials", *credentialsPath, "gc_interval", *gcInterval,
		"registry_backend", *registryBackend)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("blobgw-s3: serve", "err", err)
		os.Exit(1)
	}
	slog.Info("blobgw-s3: stopped")
}
