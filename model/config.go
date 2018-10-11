package model

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

type Config struct {
	Content BlackHole `yaml:"blackhole"`
}

type BlackHole struct {
	Address string           `yaml:"address"`
	DB      string           `yaml:"db"`
	Queues  map[string]Queue `yaml:"queue"`
}

type FillInterval struct {
	Interval int    `yaml:"interval"`
	Rate     string `yaml:"rate"`
}

type Queue struct {
	MaxWorkers int          `yaml:"max_workers"`
	Capacity   int          `yaml:"capacity"`
	Tokens     int          `yaml:"tokens"`
	Rate       FillInterval `yaml:"fill_interval"`
}

type BlackHoleConfig struct {
	Address string
	DB      string
	Opts    []*TaskOption
}

type TaskOption struct {
	Name         string
	MaxWorkers   int
	MaxQueued    int
	Capacity     int
	Tokens       int
	FillInterval time.Duration
}

func determineInterval(ft *FillInterval) time.Duration {
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

func configToOption(bcf *Config) *BlackHoleConfig {
	opts := []*TaskOption{}
	for name, q := range bcf.Content.Queues {
		to := &TaskOption{
			Name:         name,
			MaxWorkers:   q.MaxWorkers,
			MaxQueued:    q.MaxWorkers,
			Capacity:     q.Capacity,
			Tokens:       q.Tokens,
			FillInterval: determineInterval(&q.Rate),
		}
		opts = append(opts, to)
	}

	// add defaiult queue
	dtk := &TaskOption{
		Name:         "default",
		MaxWorkers:   5,
		MaxQueued:    5,
		Capacity:     5,
		Tokens:       0,
		FillInterval: time.Second,
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
