package model

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

var retry = [...]string{"linear", "exp"}

type Config struct {
	Content BlackHole `yaml:"blackhole"`
}

type BlackHole struct {
	Address string           `yaml:"address"`
	DB      []string         `yaml:"db"`
	Queues  map[string]Queue `yaml:"queue"`
}

type FillInterval struct {
	Interval int    `yaml:"interval"`
	Rate     string `yaml:"rate"`
}

type Retry struct {
	Delay    string `yaml:"delay"`
	Doubling int    `yaml:"doubling"`
	Limit    int    `yaml:"limit"`
}

type Queue struct {
	TRetry     Retry        `yaml:"retry"`
	MaxWorkers int          `yaml:"maxWorkers"`
	Capacity   int64        `yaml:"capacity"`
	Tokens     int64        `yaml:"tokens"`
	Rate       FillInterval `yaml:"fillInterval"`
}

type BlackHoleConfig struct {
	Address string
	DB      []string
	Opts    []*TaskOption
}

type TaskOption struct {
	TRetry     *Retry
	Name       string
	MaxWorkers int
	MaxQueued  int
	Capacity   int64
	Tokens     int64
	FillRate   *FillInterval
}

func DetermineInterval(ft *FillInterval) time.Duration {
	if ft.Interval <= 0 { //TODO: for now, this should be implemented beter!!!!
		return time.Second
	}

	tm := time.Duration(ft.Interval)
	switch ft.Rate {
	case "s", "second":
		return tm * time.Second
	case "ms", "millisecond":
		return tm * time.Millisecond
	case "h", "hour":
		return tm * time.Second
	default:
		return time.Second
	}
}

func checkRetry(strategy string) string {
	for _, value := range retry {
		if strategy == value {
			return strategy
		}
	}
	return "linear" //TODO: This should be checked nad return some error
}

func configToOption(bcf *Config) *BlackHoleConfig {
	opts := []*TaskOption{}
	for name, q := range bcf.Content.Queues {
		to := &TaskOption{
			TRetry:     &q.TRetry,
			Name:       name,
			MaxWorkers: q.MaxWorkers,
			MaxQueued:  q.MaxWorkers,
			Capacity:   q.Capacity,
			Tokens:     q.Tokens,
			FillRate:   &q.Rate,
		}
		opts = append(opts, to)
	}

	tretry := &Retry{
		Delay:    "linear",
		Doubling: 1,
		Limit:    5,
	}

	fr := &FillInterval{
		Interval: 1,
		Rate:     "s",
	}

	// add defaiult queue
	dtk := &TaskOption{
		TRetry:     tretry,
		Name:       "default",
		MaxWorkers: 5,
		MaxQueued:  5,
		Capacity:   5,
		Tokens:     0,
		FillRate:   fr,
	}
	opts = append(opts, dtk)
	return &BlackHoleConfig{
		Address: bcf.Content.Address,
		DB:      bcf.Content.DB,
		Opts:    opts,
	}
}

func LoadConfig(n ...string) (*BlackHoleConfig, error) {
	path := ""
	if len(n) > 0 {
		path = n[0]
	}

	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var f Config
	err = yaml.Unmarshal(yamlFile, &f)
	if err != nil {
		return nil, err
	}

	return configToOption(&f), nil
}
