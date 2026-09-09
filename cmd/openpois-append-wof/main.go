package main

// go run -tags pmtiles,sqlite,modernc cmd/openpois-append-wof/main.go -target-uri test2.parquet ./test.parquet

import (
	"context"
	"flag"
	_ "fmt"
	"log"
	"log/slog"
	"strconv"
	"sync"

	_ "github.com/whosonfirst/go-whosonfirst/v4/spatial/pmtiles"

	"github.com/paulmach/orb/planar"
	"github.com/sfomuseum/go-parquet"
	"github.com/whosonfirst/go-openpois"
	"github.com/whosonfirst/go-reader/v2"
	"github.com/whosonfirst/go-whosonfirst/v4/feature/properties"
	wof_reader "github.com/whosonfirst/go-whosonfirst/v4/reader"
	"github.com/whosonfirst/go-whosonfirst/v4/spatial/database"
	"github.com/whosonfirst/go-whosonfirst/v4/spatial/filter"
	"github.com/whosonfirst/go-whosonfirst/v4/spatial/hierarchy"
)

func main() {

	var reader_uri string
	var spatial_database_uri string
	var target_uri string
	var refresh bool

	flag.StringVar(&reader_uri, "reader-uri", "https://data.whosonfirst.org", "")
	flag.StringVar(&spatial_database_uri, "spatial-database-uri", "pmtiles://?tiles=file:///usr/local/data/whosonfirst/whosonfirst-pmtiles&database=whosonfirst-point-in-polygon-z13-20250805&enable-cache=true&zoom=13&layer=whosonfirst", "")
	flag.StringVar(&target_uri, "target-uri", "", "")
	flag.BoolVar(&refresh, "refresh", false, "")

	flag.Parse()

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

	resolver_opts := &hierarchy.PointInPolygonHierarchyResolverOptions{
		Database: spatial_db,
	}

	resolver, err := hierarchy.NewPointInPolygonHierarchyResolver(ctx, resolver_opts)

	if err != nil {
		log.Fatal(err)
	}

	inputs := &filter.SPRInputs{
		IsCurrent: []int64{
			1,
		},
	}

	parent_cache := new(sync.Map)

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

		if rec.WhosOnFirstParentId != -1 && !refresh {
			p_wr.WriteRow(rec)
			continue
		}

		// logger.Info("Fetch WOF data")

		geom, err := rec.ToOrbGeometry()

		if err != nil {
			logger.Warn("Failed to derive geometry", "error", err)
			p_wr.WriteRow(rec)
			continue
		}

		pt, _ := planar.CentroidArea(geom)

		logger = logger.With("centroid", pt)

		rsp, err := resolver.PointInPolygonWithPoint(ctx, inputs, &pt, "venue")

		if err != nil {
			logger.Error("Failed to point-in-polygon", "error", err)
			p_wr.WriteRow(rec)
			continue
		}

		switch len(rsp) {
		case 0:
			rec.WhosOnFirstParentId = -1
			rec.WhosOnFirstCountry = "XY"
		case 1:

			spr := rsp[0]

			parent_id, err := strconv.ParseInt(spr.Id(), 10, 64)

			if err != nil {
				logger.Error("Failed to parse parent ID", "parent id", spr.Id(), "error", err)
			} else {
				rec.WhosOnFirstParentId = parent_id
			}

			logger = logger.With("parent id", parent_id)

			v, ok := parent_cache.Load(parent_id)

			if ok {
				parent_body := v.([]byte)
				rec.WhosOnFirstHierarchies = properties.Hierarchies(parent_body)
				rec.WhosOnFirstCountry = properties.Country(parent_body)
			} else {

				parent_body, err := wof_reader.LoadBytes(ctx, wof_r, parent_id)

				if err != nil {
					logger.Error("Failed to retrieve parent body", "error", err)
				} else {
					parent_cache.Store(parent_id, parent_body)

					rec.WhosOnFirstHierarchies = properties.Hierarchies(parent_body)
					rec.WhosOnFirstCountry = properties.Country(parent_body)
				}
			}

			// slog.Info("Hiers", "h", rec.WhosOnFirstHierarchies)
		default:

			rec.WhosOnFirstParentId = -1
			rec.WhosOnFirstCountry = "XY"

			for i, r := range rsp {
				logger.Info("Multiple hierarchies", "i", i, "r", r.Id(), "n", r.Name(), "p", r.Placetype())
			}
		}

		p_wr.WriteRow(rec)
	}

	slog.Info("Close parquet writer")
	p_wr.Close()
}
