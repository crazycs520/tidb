package dumpling

import (
	"context"
	"os"

	"github.com/pingcap/tidb/dumpling/export"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
)

var prometheusRegistry = prometheus.NewRegistry()

func init() {
	prometheusRegistry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	prometheusRegistry.MustRegister(prometheus.NewGoCollector())
	export.InitMetricsVector(nil)
	export.RegisterMetrics(prometheusRegistry)
	prometheus.DefaultGatherer = prometheusRegistry
}

func DumpDataToS3Bucket(host, port, bucketName, s3Region, sql string) error {
	s3Path := "s3://" + bucketName
	return dumpData(host, port, s3Path, s3Region, sql)
}

func dumpData(host, port, s3Path, s3Region, sql string) error {
	conf := export.DefaultConfig()
	flagSet := pflag.NewFlagSet(os.Args[0], pflag.ExitOnError)
	conf.DefineFlags(flagSet)

	flags := []struct {
		name  string
		value string
	}{
		{"host", host},
		{"port", port},
		{"user", "root"},
		{"output", s3Path},
		{"s3.region", s3Region},
		{"filesize", "64MiB"},
		{"filetype", "parquet"},
		{"sql", sql},
		{"no-header", "true"},
	}
	for _, flag := range flags {
		err := flagSet.Set(flag.name, flag.value)
		if err != nil {
			return err
		}
	}

	err := conf.ParseFromFlags(flagSet)
	if err != nil {
		return err
	}
	dumper, err := export.NewDumper(context.Background(), conf)
	if err != nil {
		return err
	}
	err = dumper.Dump()
	dumper.Close()
	return err
}
