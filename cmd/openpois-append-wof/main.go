package main

// go run -tags pmtiles,sqlite,modernc cmd/openpois-append-wof/main.go -target-uri test2.parquet ./test.parquet

import (
	"context"
	"flag"
	_ "fmt"
	"log"
	"log/slog"

	_ "github.com/whosonfirst/go-whosonfirst/v4/spatial/pmtiles"

	"github.com/sfomuseum/go-parquet"
	"github.com/whosonfirst/go-openpois"
	"github.com/whosonfirst/go-openpois/whosonfirst"
	"github.com/whosonfirst/go-reader/v2"
	"github.com/whosonfirst/go-whosonfirst/v4/spatial/database"
)

func main() {

	var reader_uri string
	var spatial_database_uri string
	var target_uri string
	var refresh bool
	var verbose bool

	flag.StringVar(&reader_uri, "reader-uri", "https://data.whosonfirst.org", "A registered whosonfirst/go-reader/v2.Reader URI.")
	flag.StringVar(&spatial_database_uri, "spatial-database-uri", "pmtiles://?tiles=file:///usr/local/data/whosonfirst/whosonfirst-pmtiles&database=whosonfirst-point-in-polygon-z13-20250805&enable-cache=true&zoom=13&layer=whosonfirst", "A registered whosonfirst/go-whosonfirst/v4/spatial/database.SpatialDatabase URI.")
	flag.StringVar(&target_uri, "target-uri", "-", "The URI where data should be written to. Default is STDOUT (-).")
	flag.BoolVar(&refresh, "refresh", false, "Update records with existing Who's On First properties.")
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose (debug) logging.")

	flag.Parse()

	if verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Debug("Verbose logging enabled")
	}

	ctx := context.Background()
	uris := flag.Args()

	wof_r, err := reader.NewReader(ctx, reader_uri)

	if err != nil {
		log.Fatalf("Failed to create WOF reader, %v", err)
	}

	spatial_db, err := database.NewSpatialDatabase(ctx, spatial_database_uri)

	if err != nil {
		log.Fatalf("Failed to create spatial database, %v", err)
	}

	defer spatial_db.Close(ctx)

	append_opts := &whosonfirst.AppendWhosOnFirstPropertiesOptions{
		Database: spatial_db,
		Reader:   wof_r,
	}

	p_wr, err := parquet.NewWriter[*openpois.Record](ctx, target_uri)

	if err != nil {
		log.Fatalf("Failed to create parquet writer, %v", err)
	}

	for rec, err := range parquet.Iterate[openpois.Record](ctx, uris...) {

		if err != nil {
			log.Fatalf("Iterator yield an error, %v", err)
		}

		logger := slog.Default()
		logger = logger.With("id", rec.UnifiedID)
		logger = logger.With("name", rec.PrimaryName())

		if rec.WhosOnFirstParentId != 0 && !refresh {
			p_wr.WriteRow(rec)
			continue
		}

		err = whosonfirst.AppendWhosOnFirstProperties(ctx, rec, append_opts)

		if err != nil {
			logger.Warn("Failed to append Who's On First properties", "error", err)
		}

		p_wr.WriteRow(rec)
		logger.Info("Write row", "parent", rec.WhosOnFirstParentId, "country", rec.WhosOnFirstCountry)
	}

	slog.Info("Close parquet writer")
	p_wr.Close()
}
