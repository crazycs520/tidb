//package dumpling
package main

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/interval/util"

	"github.com/pingcap/tidb/dumpling/export"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

func main() {
	//err := testDumpling("0.0.0.0", "4000", "/tmp/test", "", "select * from test.t0 partition (p0)")
	s3Path := "s3://" + util.GetTablePartitionBucketName("t0", 0)
	err := testDumpling("0.0.0.0", "4000", s3Path, "us-west-2", "select * from test.t0 partition (p0)")
	if err != nil {
		fmt.Printf("dumpling failed: %+v\n", err)
	}
}

var prometheusRegistry = prometheus.NewRegistry()

func init() {
	prometheusRegistry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	prometheusRegistry.MustRegister(prometheus.NewGoCollector())
	export.InitMetricsVector(nil)
	export.RegisterMetrics(prometheusRegistry)
	prometheus.DefaultGatherer = prometheusRegistry
}

func testDumpling(host, port, s3Path, s3Region, sql string) error {
	conf := export.DefaultConfig()
	conf.DefineFlags(pflag.CommandLine)

	flags := []struct {
		name  string
		value string
	}{
		{"host", host},
		{"port", port},
		{"user", "root"},
		{"output", s3Path},
		{"s3.region", s3Region},
		{"rows", "200000"},
		{"filetype", "csv"},
		{"sql", sql},
		{"csv-delimiter", ""},
		{"no-header", "true"},
	}
	for _, flag := range flags {
		err := pflag.Set(flag.name, flag.value)
		if err != nil {
			return err
		}
	}

	err := conf.ParseFromFlags(pflag.CommandLine)
	if err != nil {
		return err
	}
	dumper, err := export.NewDumper(context.Background(), conf)
	dumper.L().Info("show config", zap.String("cfg", conf.String()))
	if err != nil {
		return err
	}
	err = dumper.Dump()
	dumper.Close()
	return err
}
