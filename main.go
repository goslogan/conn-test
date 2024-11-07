/*
Copyright © 2024 Nic Gibson
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

 1. Redistributions of source code must retain the above copyright notice,
    this list of conditions and the following disclaimer.

 2. Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.

 3. Neither the name of the copyright holder nor the names of its contributors
    may be used to endorse or promote products derived from this software
    without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
)

var (
	ignoreCertValidity bool
	help               bool
	continuous         bool
	duration           time.Duration
	frequency          time.Duration
	clientFrequency    time.Duration
	url                string
	address            string
	secure             bool
	user               string
	password           string
	caPath             string
	clientCertPath     string
	clientKeyPath      string
	clientCert         []byte
	clientKey          []byte
	caCert             []byte
	log                zerolog.Logger
	count              int
	poolSize           int
	logfile            string
	counter            atomic.Uint64
)

var minFrequency = 10 * time.Millisecond

//go:embed redis_ca.pem
var redisCA []byte

func main() {

	var err error

	log = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	pflag.Parse()

	if help {
		fmt.Fprintf(os.Stderr, "\n%s: Usage\n", os.Args[0])
		pflag.PrintDefaults()
		os.Exit(0)
	}

	if logfile != "" {
		var fh *os.File

		if strings.ToLower(logfile) == "stderr" {
			fh = os.Stderr
		} else if strings.ToLower(logfile) == "stdout" {
			fh = os.Stdout
		} else {
			fh, err = os.OpenFile(logfile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o755)
		}

		if err != nil {
			log.Fatal().Err(err).Msg("unable to open log file")
		} else {
			log = zerolog.New(fh).With().Timestamp().Logger()
		}
	}

	checkFlags()
	options := buildOptions()

	client := redis.NewClient(options)

	err = client.Ping(context.Background()).Err()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to redis")
	}

	if continuous {
		runMonitor(context.Background(), client)
	} else {
		ctx, cancelFn := context.WithTimeout(context.Background(), duration)
		defer cancelFn()
		runMonitor(ctx, client)
	}

}

func runMonitor(ctx context.Context, client *redis.Client) {

	runTicker := time.NewTicker(frequency)
	clientTicker := time.NewTicker(clientFrequency)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	runCount := 0

	handleClients(ctx, client, 0)

	for {
		select {

		case <-ctx.Done():
			return
		case <-runTicker.C:
			runCount++
			go handleRun(ctx, client)
		case <-clientTicker.C:
			go handleClients(ctx, client, runCount)
		case <-sigchan:
			log.Info().Msg("terminated by user")
			return
		}
	}

}

func handleRun(ctx context.Context, client *redis.Client) {

	c := counter.Add(1)

	start := time.Now()

	id, err := client.ClientID(ctx).Result()
	if err != nil {
		log.Warn().Err(err).Msg("unable to get client id")
		return
	}

	log.Info().Uint64("run-no", c).Time("start", start).Str("state", "starting").Send()

	keybase := fmt.Sprintf("%d:%d", id, time.Now().Unix())

	for idx := 1; idx <= count; idx++ {

		curkey := fmt.Sprintf("%s:%d", keybase, idx)

		command := rand.Intn(9) + 1

		switch command {
		case 1:
			err = client.Set(ctx, curkey+":set", id, time.Minute*20).Err()
		case 2:
			err = client.Get(ctx, curkey+":set").Err()
		case 3:
			err = client.HSet(ctx, curkey+":hash", "id", id).Err()
			if err == nil {
				err = client.Expire(ctx, curkey+"hash", time.Minute*20).Err()
			}
		case 4:
			err = client.HGet(ctx, curkey+":hash", "id").Err()
		case 5:
			err = client.RPush(ctx, "testlist", keybase).Err()
		case 6:
			err = client.LPop(ctx, "testlist").Err()
		case 7:
			err = client.LLen(ctx, "testlist").Err()
		case 8:
			err = client.ZAdd(ctx, "testsorted", redis.Z{
				Score:  float64(time.Now().Unix()),
				Member: curkey,
			}).Err()
		case 9:
			err = client.ZRandMember(ctx, "testsorted", 100).Err()
		case 10:
			err = client.ZCount(ctx, "testsorted", "-inf", "+inf").Err()
		}

		if err != nil && err != redis.Nil {
			log.Warn().Err(err).Int("index", idx).Str("curkey", curkey).Msg("unable to execute command")
		}

	}

	log.Info().Uint64("run-no", c).Time("start", start).Time("end", time.Now()).Str("state", "finished").Send()

}

func handleClients(ctx context.Context, client *redis.Client, count int) {
	clientList, err := client.ClientList(ctx).Result()

	if err != nil {
		log.Warn().Err(err).Msg("unable to get client list")
		return
	}

	info := parseClientInfo(clientList)

	log.Info().Int("clients", len(info)).Int("runs", count).Msg("open clients")
	for _, i := range info {
		log.Info().Str("id", i["id"]).Str("age", i["age"]).Str("idle", i["idle"]).Str("cmd", i["cmd"]).Send()
	}

	stats := client.PoolStats()
	log.Info().Interface("stats", stats).Send()

}

func parseClientInfo(info string) []map[string]string {

	results := make([]map[string]string, 0)
	lines := strings.Split(info, "\n")

	for _, line := range lines {

		if line != "" {
			cInfo := make(map[string]string)
			pairs := strings.Split(line, " ")

			for _, pair := range pairs {
				item := strings.Split(pair, "=")
				if len(item) == 1 {
					cInfo[item[0]] = ""
				} else {
					cInfo[item[0]] = item[1]
				}

			}

			results = append(results, cInfo)
		}
	}

	return results
}

func buildOptions() *redis.Options {

	var options *redis.Options
	var err error

	if url != "" {
		options, err = redis.ParseURL(url)
		if err != nil {
			log.Fatal().Err(err).Msg("invalid redis URL")
		}
		if options.Password == "" {
			options.Password = password
		}
		if options.Username == "" {
			options.Username = user
		}
	} else {
		options = &redis.Options{}
		options.Addr = address
		options.Password = password
		options.Username = user
	}

	options.MaxActiveConns = 500
	options.ContextTimeoutEnabled = true
	options.PoolTimeout = time.Second * 10

	if poolSize != 0 {
		options.PoolSize = poolSize
	}

	if secure {
		var cert tls.Certificate
		var err error
		var pool *x509.CertPool = x509.NewCertPool()

		if !pool.AppendCertsFromPEM(redisCA) {
			log.Fatal().Msg("unable to add redis cloud CA cert to pool")
		}

		if caPath != "" && !pool.AppendCertsFromPEM(caCert) {
			log.Fatal().Str("ca", caPath).Msg("unable to add redis cloud CA cert to pool")
		}

		if clientCertPath != "" {

			if cert, err = tls.X509KeyPair(clientCert, clientKey); err != nil {
				log.Fatal().Err(err).Str("cert", clientCertPath).Str("key", clientKeyPath).Msg("unable to create key pair for client key & certificate")
			} else {
				options.TLSConfig = &tls.Config{
					Certificates: []tls.Certificate{cert},
					RootCAs:      pool,
					MinVersion:   tls.VersionTLS12,
				}
			}
		}

		options.TLSConfig.InsecureSkipVerify = ignoreCertValidity

	}

	return options
}

func checkFlags() {

	var errorMessage string
	var err error

	if secure && ignoreCertValidity {
		log.Warn().Msg("certificate validity will not be checked")
	}

	if clientCertPath != "" && clientKeyPath == "" {
		errorMessage = "if client certificates are used both certificate and key must be provided"
	}

	if clientCertPath == "" && clientKeyPath != "" {
		errorMessage = "if client certificates are used both certificate and key must be provided"
	}

	if errorMessage != "" {
		log.Error().Msg(errorMessage)
		pflag.PrintDefaults()
		os.Exit(1)
	}

	if caPath != "" {
		caCert, err = os.ReadFile(caPath)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to read CA certificate")
		}
	}

	if clientCertPath != "" {
		clientCert, err = os.ReadFile(clientCertPath)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to read client certificate")
		}
	}

	if clientKeyPath != "" {
		clientKey, err = os.ReadFile(clientKeyPath)
		if err != nil {
			log.Fatal().Err(err).Msg("unable to read client key")
		}
	}

	if frequency < minFrequency {
		log.Warn().Dur("setting", frequency).Msg("frequency too low")
		frequency = minFrequency
	}
}

func init() {
	pflag.BoolVarP(&help, "help", "h", false, "print the usage message")
	pflag.DurationVarP(&duration, "duration", "d", time.Duration(10*time.Minute), "duration of testing")
	pflag.BoolVarP(&continuous, "continuous", "c", false, "run forever (until killed) - runs without a timeout")
	pflag.StringVarP(&url, "url", "u", "", "redis connection URL")
	pflag.StringVarP(&address, "address", "a", "localhost:6379", "redis host name:port")
	pflag.BoolVarP(&ignoreCertValidity, "skip", "S", false, "do not verify certificates")
	pflag.BoolVarP(&secure, "secure", "s", false, "use TLS to connect to redis")

	pflag.StringVarP(&caPath, "capath", "A", "", "path to CA certificate (defaults to redis cloud cert if none provided)")
	pflag.StringVarP(&clientCertPath, "cert", "C", "", "path to client certificate")
	pflag.StringVarP(&clientKeyPath, "key", "K", "", "path to client key")

	pflag.StringVarP(&password, "password", "p", "", "redis password")
	pflag.StringVarP(&user, "user", "U", "", "redis username")

	pflag.DurationVarP(&frequency, "frequency", "f", minFrequency, "run commands this frequently`")
	pflag.DurationVarP(&clientFrequency, "client-frequency", "F", time.Second*10, "run client list this frequently")
	pflag.IntVarP(&count, "command-count", "n", 10, "number of commands to run each iteration")

	pflag.IntVarP(&poolSize, "pool-size", "P", 0, "pool size - zero sets the default")

	pflag.CommandLine.MarkHidden("skip")

	pflag.StringVarP(&logfile, "logfile", "l", "", "logfile (default console), set to 'stderr' or 'stdout' to log those")
}
