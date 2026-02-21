package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type agg struct {
	Total   int64
	Covered int64
}

type fileStat struct {
	File     string
	Total    int64
	Covered  int64
	CoverPct float64
}

func main() {
	var (
		coverFile = flag.String("coverprofile", "coverage.out", "path to coverprofile")
		topN      = flag.Int("top", 30, "how many files to print")
		minTotal  = flag.Int64("min-total", 1, "min statements to include")
	)
	flag.Parse()

	stats, err := readCoverProfile(*coverFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "coverrank: %v\n", err)
		os.Exit(1)
	}

	list := make([]fileStat, 0, len(stats))
	for f, st := range stats {
		if st.Total < *minTotal {
			continue
		}
		pct := 0.0
		if st.Total > 0 {
			pct = float64(st.Covered) * 100.0 / float64(st.Total)
		}
		list = append(list, fileStat{
			File:     f,
			Total:    st.Total,
			Covered:  st.Covered,
			CoverPct: pct,
		})
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].CoverPct == list[j].CoverPct {
			return list[i].Total > list[j].Total
		}
		return list[i].CoverPct < list[j].CoverPct
	})

	if *topN > len(list) {
		*topN = len(list)
	}

	fmt.Printf("=== Worst files by coverage (weighted) ===\n")
	for i := 0; i < *topN; i++ {
		s := list[i]
		fmt.Printf("%6.2f%%  %6d/%-6d  %s\n", s.CoverPct, s.Covered, s.Total, s.File)
	}

	var tot, cov int64
	for _, s := range list {
		tot += s.Total
		cov += s.Covered
	}
	overall := 0.0
	if tot > 0 {
		overall = float64(cov) * 100.0 / float64(tot)
	}
	fmt.Printf("\nOverall (files included): %.2f%%  %d/%d\n", overall, cov, tot)
}

// coverprofile line format:
// <file>:<startLine>.<startCol>,<endLine>.<endCol> <numStmts> <count>
func readCoverProfile(path string) (map[string]*agg, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open coverprofile: %w", err)
	}
	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	stats := map[string]*agg{}

	// First line: "mode: set"
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		return nil, fmt.Errorf("empty coverprofile")
	}

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid cover line: %q", line)
		}

		filePart := parts[0]
		numStmtsStr := parts[1]
		countStr := parts[2]

		idx := strings.Index(filePart, ":")
		if idx <= 0 {
			return nil, fmt.Errorf("invalid file/range: %q", filePart)
		}
		file := filePart[:idx]

		numStmts, err := strconv.ParseInt(numStmtsStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse numStmts %q: %w", numStmtsStr, err)
		}
		count, err := strconv.ParseInt(countStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse count %q: %w", countStr, err)
		}

		st := stats[file]
		if st == nil {
			st = &agg{}
			stats[file] = st
		}

		st.Total += numStmts
		if count > 0 {
			st.Covered += numStmts
		}
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}
	return stats, nil
}
