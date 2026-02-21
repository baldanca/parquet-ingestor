package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type agg struct {
	total   int64
	covered int64
}

type fileStat struct {
	file string
	pct  float64
	cov  int64
	tot  int64
}

func main() {

	profile := flag.String("coverprofile", "coverage.out", "")
	ignore := flag.String("ignore", ".covignore", "")
	top := flag.Int("top", 30, "")
	markdown := flag.Bool("markdown", false, "")
	jsonOut := flag.Bool("json", false, "")

	flag.Parse()

	rules := loadIgnore(*ignore)

	stats, err := readCoverage(*profile, rules)
	if err != nil {
		panic(err)
	}

	var list []fileStat

	var total int64
	var covered int64

	for file, s := range stats {

		pct := float64(s.covered) * 100 / float64(s.total)

		list = append(list, fileStat{
			file: file,
			pct:  pct,
			cov:  s.covered,
			tot:  s.total,
		})

		total += s.total
		covered += s.covered
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].pct < list[j].pct
	})

	if *markdown {

		fmt.Println("## Coverage Ranking")
		fmt.Println("")
		fmt.Println("| Coverage | Covered | File |")
		fmt.Println("|---|---|---|")

	} else {

		fmt.Println("=== Worst files by coverage ===")

	}

	limit := *top
	if limit > len(list) {
		limit = len(list)
	}

	for i := 0; i < limit; i++ {

		s := list[i]

		if *markdown {

			fmt.Printf(
				"| %.2f%% | %d/%d | %s |\n",
				s.pct,
				s.cov,
				s.tot,
				s.file,
			)

		} else {

			fmt.Printf(
				"%6.2f%% %6d/%-6d %s\n",
				s.pct,
				s.cov,
				s.tot,
				s.file,
			)

		}
	}

	overall := float64(covered) * 100 / float64(total)

	if *jsonOut {

		fmt.Printf(
			`{"coverage":%.2f}`,
			overall,
		)

		return
	}

	fmt.Printf(
		"\nOverall (filtered): %.2f%% %d/%d\n",
		overall,
		covered,
		total,
	)
}

func loadIgnore(path string) []string {

	var rules []string

	f, err := os.Open(path)
	if err != nil {
		return rules
	}

	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)

	for sc.Scan() {

		line := strings.TrimSpace(sc.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		rules = append(rules, line)
	}

	return rules
}

func ignored(file string, rules []string) bool {

	file = filepath.ToSlash(file)

	for _, r := range rules {

		// directory ignore
		if strings.HasSuffix(r, "/") {
			if strings.Contains(file, r) {
				return true
			}
		}

		ok, err := filepath.Match(r, filepath.Base(file))
		if err == nil && ok {
			return true
		}

		if strings.Contains(file, r) {
			return true
		}
	}

	return false
}

func readCoverage(path string, rules []string) (map[string]*agg, error) {

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)

	stats := map[string]*agg{}

	sc.Scan() // skip mode:

	for sc.Scan() {

		line := sc.Text()

		fields := strings.Fields(line)

		fileRange := fields[0]

		idx := strings.Index(fileRange, ":")

		file := fileRange[:idx]

		if ignored(file, rules) {
			continue
		}

		numStmt, _ := strconv.ParseInt(fields[1], 10, 64)
		count, _ := strconv.ParseInt(fields[2], 10, 64)

		s := stats[file]

		if s == nil {
			s = &agg{}
			stats[file] = s
		}

		s.total += numStmt

		if count > 0 {
			s.covered += numStmt
		}
	}

	return stats, nil
}
