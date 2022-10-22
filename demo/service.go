package main

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/pingcap/fn"
	"github.com/spf13/pflag"
)

var ErrServerInternal = errors.New("server internal error")

type options struct {
	Port int

	DB struct {
		Host    string
		Port    int
		User    string
		Pass    string
		Name    string
		Options string
	}

	Record    int
	BatchSize int
}

func (opt *options) addFlags(flags *pflag.FlagSet) {
	flags.IntVar(&opt.Port, "port", 8080, "TiFancy demo service port")

	// DB server configurations
	flags.StringVar(&opt.DB.Host, "db.host", "127.0.0.1", "Database server host name")
	flags.IntVar(&opt.DB.Port, "db.port", 4000, "Database server port")
	flags.StringVar(&opt.DB.User, "db.user", "root", "Database server user name")
	flags.StringVar(&opt.DB.Pass, "db.pass", "", "Database server password")
	flags.StringVar(&opt.DB.Name, "db.name", "tifancy-demo", "Database server database name")
	flags.StringVar(&opt.DB.Options, "db.options", "charset=utf8mb4", "Database server connection options")

	flags.IntVar(&opt.Record, "record", 10000, "Initial user rate records")
	flags.IntVar(&opt.BatchSize, "batch-size", 1000, "Batch size of initialing user rate records")

}

// DSN returns the data source name for the given database.
func (opt *options) DSN() string {
	db := opt.DB
	return db.User + ":" + db.Pass + "@tcp(" + db.Host + ":" + strconv.Itoa(db.Port) + ")/" + db.Name + "?" + db.Options
}

type service struct {
	opt *options
	db  *sql.DB
}

func newService(opt *options) *service {
	return &service{
		opt: opt,
	}
}

func (s *service) serve() error {
	db, err := sql.Open("mysql", s.opt.DSN())
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}
	s.db = db

	fmt.Println("Connected to TiDB successfully")

	if err := s.initData(s.opt.Record, s.opt.BatchSize); err != nil {
		return err
	}

	router := mux.NewRouter()
	router.Handle("/", s.homepageEmbed())
	router.Handle("/api/v1/rate", fn.Wrap(s.Rate)).Methods(http.MethodPost)
	router.Handle("/api/v1/stats", fn.Wrap(s.Stats)).Methods(http.MethodGet)

	addr := fmt.Sprintf(":%d", s.opt.Port)
	fmt.Println("Serve HTTP:", addr)

	return http.ListenAndServe(addr, router)
}

func (s *service) initData(record, batchSize int) error {
	// CREATE TABLE
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS rate_records(
		    id BIGINT NOT NULL AUTO_RANDOM PRIMARY KEY,
		    team_name VARCHAR(64),
		    score BIGINT DEFAULT 0,
		    created_at DATETIME DEFAULT NOW()
		);
`)
	if err != nil {
		return err
	}

	var count int64
	err = s.db.QueryRow(`SELECT COUNT(*) FROM rate_records LIMIT 1`).Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		return nil
	}

	var batchRecords []string
	batches := int(math.Ceil(float64(record) / float64(batchSize)))
	for i := 0; i < batches; i++ {
		count := batchSize
		if i == batches-1 {
			count = record - (batches-1)*batchSize
		}
		batchRecords = batchRecords[:0]
		for j := 0; j < count; j++ {
			batchRecords = append(batchRecords, fmt.Sprintf(`("%s"", 1)`, s.randomTeam()))
		}
		_, err := s.db.Exec("INSERT INTO rate_records(team_name, score) VALUES " + strings.Join(batchRecords, ","))
		if err != nil {
			return err
		}
	}

	fmt.Println("Initialize data successfully")

	return nil
}

func (s *service) homepage() http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		http.ServeFile(writer, request, "app.html")
	}
}

func (s *service) homepageEmbed() http.HandlerFunc {
	startTime := time.Now()
	return func(writer http.ResponseWriter, request *http.Request) {
		http.ServeContent(writer, request, "app.html", startTime, bytes.NewReader(appHtml))
	}
}

func (s *service) randomTeam() string {
	return teamNames[rand.Intn(len(teamNames))]
}

type (
	RateRequest struct {
		TeamName string `json:"team_name"`
	}

	RateResponse struct {
		TeamName string `json:"team_name"`
		Score    int64  `json:"score"`
	}
)

func (s *service) Rate(r *RateRequest) (*RateResponse, error) {
	i := sort.SearchStrings(teamNames, strings.TrimSpace(r.TeamName))
	if i < 0 {
		return nil, fmt.Errorf("illegal team name in request")
	}
	if r.TeamName == "" {
		r.TeamName = s.randomTeam()
	}
	score := int64(rand.Intn(100000))
	_, err := s.db.Exec("INSERT INTO rate_records(team_name, score) VALUES (?, ?)", r.TeamName, score)
	if err != nil {
		return nil, ErrServerInternal
	}
	res := &RateResponse{TeamName: r.TeamName, Score: score}
	return res, nil
}

type (
	StatsItem struct {
		TeamName   string `json:"team_name"`
		TotalScore int64  `json:"total_score"`
	}
	StatsResponse struct {
		Teams   []StatsItem `json:"teams"`
		Count   int64       `json:"count"`
		Latency int64       `json:"latency"`
	}
)

func (s *service) Stats() (*StatsResponse, error) {
	var response StatsResponse
	startTime := time.Now()
	err := s.db.QueryRow("SELECT COUNT(*) FROM rate_records").Scan(&response.Count)
	if err != nil {
		return nil, ErrServerInternal
	}
	response.Latency = time.Since(startTime).Milliseconds()

	r, err := s.db.Query("SELECT team_name, SUM(score) FROM rate_records GROUP BY team_name")
	if err != nil {
		return nil, ErrServerInternal
	}
	for r.Next() {
		var teamName string
		var totalScore int64
		if err := r.Scan(&teamName, &totalScore); err != nil {
			return nil, ErrServerInternal
		}
		response.Teams = append(response.Teams, StatsItem{
			TeamName:   teamName,
			TotalScore: totalScore,
		})
	}

	return &response, nil
}
