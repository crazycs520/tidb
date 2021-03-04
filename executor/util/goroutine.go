package util

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Knetic/govaluate"
	"github.com/foize/go.sgr"
)

var (
	startLinePattern = regexp.MustCompile(`^goroutine\s+(\d+)\s+\[(.*)\]:$`)
)

func Load(fn string) (*GoroutineDump, error) {
	fn = strings.Trim(fn, "\"")
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dump := NewGoroutineDump()
	var goroutine *Goroutine

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if startLinePattern.MatchString(line) {
			goroutine, err = NewGoroutine(line)
			if err != nil {
				return nil, err
			}
			dump.Add(goroutine)
		} else if line == "" {
			// End of a goroutine section.
			if goroutine != nil {
				goroutine.Freeze()
			}
			goroutine = nil
		} else if goroutine != nil {
			goroutine.AddLine(line)
		}
	}

	if goroutine != nil {
		goroutine.Freeze()
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return dump, nil
}

type MetaType int

var (
	MetaState    MetaType = 0
	MetaDuration MetaType = 1

	durationPattern = regexp.MustCompile(`^\d+ minutes$`)

	functions = map[string]govaluate.ExpressionFunction{
		"contains": func(args ...interface{}) (interface{}, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("contains() accepts exactly two arguments")
			}
			idx := strings.Index(args[0].(string), args[1].(string))
			return bool(idx > -1), nil
		},
		"lower": func(args ...interface{}) (interface{}, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("lower() accepts exactly one arguments")
			}
			lowered := strings.ToLower(args[0].(string))
			return string(lowered), nil
		},
		"upper": func(args ...interface{}) (interface{}, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("upper() accepts exactly one arguments")
			}
			uppered := strings.ToUpper(args[0].(string))
			return string(uppered), nil
		},
	}
)

// Goroutine contains a goroutine info.
type Goroutine struct {
	ID       int
	Header   string
	Trace    string
	lines    int
	Duration int // In minutes.
	Metas    map[MetaType]string

	lineMd5    []string
	FullMd5    string
	fullHasher hash.Hash
	duplicates []int

	frozen bool
	buf    *bytes.Buffer
}

// AddLine appends a line to the goroutine info.
func (g *Goroutine) AddLine(l string) {
	if !g.frozen {
		g.lines++
		g.buf.WriteString(l)
		g.buf.WriteString("\n")

		if strings.HasPrefix(l, "\t") {
			parts := strings.Split(l, " ")
			if len(parts) != 2 {
				//fmt.Println("ignored one line for digest")
				return
			}

			fl := strings.TrimSpace(parts[0])

			h := md5.New()
			io.WriteString(h, fl)
			g.lineMd5 = append(g.lineMd5, string(h.Sum(nil)))

			io.WriteString(g.fullHasher, fl)
		}
	}
}

// Freeze freezes the goroutine info.
func (g *Goroutine) Freeze() {
	if !g.frozen {
		g.frozen = true
		g.Trace = g.buf.String()
		g.buf = nil

		g.FullMd5 = string(g.fullHasher.Sum(nil))
	}
}

// Print outputs the goroutine details to w.
func (g Goroutine) Print(w io.Writer) error {
	if _, err := fmt.Fprint(w, g.Header); err != nil {
		return err
	}
	if len(g.duplicates) > 0 {
		if _, err := fmt.Fprintf(w, " %d times: [[", len(g.duplicates)); err != nil {
			return err
		}
		for i, id := range g.duplicates {
			if i > 0 {
				if _, err := fmt.Fprint(w, ", "); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprint(w, id); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprint(w, "]"); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, g.Trace); err != nil {
		return err
	}
	return nil
}

// PrintWithColor outputs the goroutine details to stdout with color.
func (g Goroutine) PrintWithColor() {
	sgr.Printf("[fg-blue]%s[reset]", g.Header)
	if len(g.duplicates) > 0 {
		sgr.Printf(" [fg-red]%d[reset] times: [[", len(g.duplicates))
		for i, id := range g.duplicates {
			if i > 0 {
				sgr.Printf(", ")
			}
			sgr.Printf("[fg-green]%d[reset]", id)
		}
		sgr.Print("]")
	}
	sgr.Println()
	fmt.Println(g.Trace)
}

// NewGoroutine creates and returns a new Goroutine.
func NewGoroutine(metaline string) (*Goroutine, error) {
	idx := strings.Index(metaline, "[")
	parts := strings.Split(metaline[idx+1:len(metaline)-2], ",")
	metas := map[MetaType]string{
		MetaState: strings.TrimSpace(parts[0]),
	}

	duration := 0
	if len(parts) > 1 {
		value := strings.TrimSpace(parts[1])
		metas[MetaDuration] = value
		if durationPattern.MatchString(value) {
			if d, err := strconv.Atoi(value[:len(value)-8]); err == nil {
				duration = d
			}
		}
	}

	idstr := strings.TrimSpace(metaline[9:idx])
	id, err := strconv.Atoi(idstr)
	if err != nil {
		return nil, err
	}

	return &Goroutine{
		ID:         id,
		lines:      1,
		Header:     metaline,
		buf:        &bytes.Buffer{},
		Duration:   duration,
		Metas:      metas,
		fullHasher: md5.New(),
		duplicates: []int{},
	}, nil
}

// GoroutineDump defines a goroutine dump.
type GoroutineDump struct {
	Goroutines []*Goroutine
}

// Add appends a goroutine info to the list.
func (gd *GoroutineDump) Add(g *Goroutine) {
	gd.Goroutines = append(gd.Goroutines, g)
}

// Copy duplicates and returns the GoroutineDump.
func (gd GoroutineDump) Copy(cond string) *GoroutineDump {
	dump := GoroutineDump{
		Goroutines: []*Goroutine{},
	}
	if cond == "" {
		// Copy all.
		for _, d := range gd.Goroutines {
			dump.Goroutines = append(dump.Goroutines, d)
		}
	} else {
		goroutines, err := gd.withCondition(cond, func(i int, g *Goroutine, passed bool) *Goroutine {
			if passed {
				return g
			}
			return nil
		})
		if err != nil {
			fmt.Println(err)
			return nil
		}
		dump.Goroutines = goroutines
	}
	return &dump
}

// Dedup finds Goroutines with duplicated stack traces and keeps only one copy
// of them.
func (gd *GoroutineDump) Dedup() {
	m := map[string][]int{}
	for _, g := range gd.Goroutines {
		if _, ok := m[g.FullMd5]; ok {
			m[g.FullMd5] = append(m[g.FullMd5], g.ID)
		} else {
			m[g.FullMd5] = []int{g.ID}
		}
	}

	kept := make([]*Goroutine, 0, len(gd.Goroutines))

outter:
	for digest, ids := range m {
		for _, g := range gd.Goroutines {
			if g.FullMd5 == digest {
				g.duplicates = ids
				kept = append(kept, g)
				continue outter
			}
		}
	}

	if len(gd.Goroutines) != len(kept) {
		fmt.Printf("Dedupped %d, kept %d\n", len(gd.Goroutines), len(kept))
		gd.Goroutines = kept
	}
}

// Delete deletes by the condition.
func (gd *GoroutineDump) Delete(cond string) error {
	goroutines, err := gd.withCondition(cond, func(i int, g *Goroutine, passed bool) *Goroutine {
		if !passed {
			return g
		}
		return nil
	})
	if err != nil {
		return err
	}
	gd.Goroutines = goroutines
	return nil
}

// Diff shows the difference between two dumps.
func (gd *GoroutineDump) Diff(another *GoroutineDump) (*GoroutineDump, *GoroutineDump, *GoroutineDump) {
	lonly := map[int]*Goroutine{}
	ronly := map[int]*Goroutine{}
	common := map[int]*Goroutine{}

	for _, v := range gd.Goroutines {
		lonly[v.ID] = v
	}
	for _, v := range another.Goroutines {
		if _, ok := lonly[v.ID]; ok {
			delete(lonly, v.ID)
			common[v.ID] = v
		} else {
			ronly[v.ID] = v
		}
	}
	return NewGoroutineDumpFromMap(lonly), NewGoroutineDumpFromMap(common), NewGoroutineDumpFromMap(ronly)
}

// Keep keeps by the condition.
func (gd *GoroutineDump) Keep(cond string) error {
	goroutines, err := gd.withCondition(cond, func(i int, g *Goroutine, passed bool) *Goroutine {
		if passed {
			return g
		}
		return nil
	})
	if err != nil {
		return err
	}
	gd.Goroutines = goroutines
	return nil
}

// Save saves the goroutine dump to the given file.
func (gd GoroutineDump) Save(fn string) error {
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, g := range gd.Goroutines {
		if err := g.Print(f); err != nil {
			return err
		}
	}
	return nil
}

// Search displays the Goroutines with the offset and limit.
func (gd GoroutineDump) Search(cond string, offset, limit int) {
	sgr.Printf("[fg-green]Search with offset %d and limit %d.[reset]\n\n", offset, limit)

	count := 0
	_, err := gd.withCondition(cond, func(i int, g *Goroutine, passed bool) *Goroutine {
		if passed {
			if count >= offset && count < offset+limit {
				g.PrintWithColor()
			}
			count++
		}
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
}

// Show displays the Goroutines with the offset and limit.
func (gd GoroutineDump) Show(offset, limit int) {
	for i := offset; i < offset+limit && i < len(gd.Goroutines); i++ {
		gd.Goroutines[offset+i].PrintWithColor()
	}
}

// Sort sorts the goroutine entries.
func (gd *GoroutineDump) Sort() {
	fmt.Printf("# of Goroutines: %d\n", len(gd.Goroutines))
}

// Summary prints the summary of the goroutine dump.
func (gd GoroutineDump) Summary() {
	fmt.Printf("# of Goroutines: %d\n", len(gd.Goroutines))
	stats := map[string]int{}
	if len(gd.Goroutines) > 0 {
		for _, g := range gd.Goroutines {
			stats[g.Metas[MetaState]]++
		}
		fmt.Println()
	}
	if len(stats) > 0 {
		states := make([]string, 0, 10)
		for k := range stats {
			states = append(states, k)
		}
		sort.Sort(sort.StringSlice(states))

		for _, k := range states {
			fmt.Printf("%15s: %d\n", k, stats[k])
		}
		fmt.Println()
	}
}

// NewGoroutineDump creates and returns a new GoroutineDump.
func NewGoroutineDump() *GoroutineDump {
	return &GoroutineDump{
		Goroutines: []*Goroutine{},
	}
}

// NewGoroutineDumpFromMap creates and returns a new GoroutineDump from a map.
func NewGoroutineDumpFromMap(gs map[int]*Goroutine) *GoroutineDump {
	gd := &GoroutineDump{
		Goroutines: []*Goroutine{},
	}
	for _, v := range gs {
		gd.Goroutines = append(gd.Goroutines, v)
	}
	return gd
}

func (gd *GoroutineDump) withCondition(cond string, callback func(int, *Goroutine, bool) *Goroutine) ([]*Goroutine, error) {
	cond = strings.Trim(cond, "\"")
	expression, err := govaluate.NewEvaluableExpressionWithFunctions(cond, functions)
	if err != nil {
		return nil, err
	}

	goroutines := make([]*Goroutine, 0, len(gd.Goroutines))
	for i, g := range gd.Goroutines {
		params := map[string]interface{}{
			"ID":       g.ID,
			"dups":     len(g.duplicates),
			"Duration": g.Duration,
			"lines":    g.lines,
			"state":    g.Metas[MetaState],
			"Trace":    g.Trace,
		}
		res, err := expression.Evaluate(params)
		if err != nil {
			return nil, err
		}
		if val, ok := res.(bool); ok {
			if gor := callback(i, g, val); gor != nil {
				goroutines = append(goroutines, gor)
			}
		} else {
			return nil, errors.New("argument expression should return a boolean")
		}
	}
	fmt.Printf("Deleted %d Goroutines, kept %d.\n", len(gd.Goroutines)-len(goroutines), len(goroutines))
	return goroutines, nil
}
