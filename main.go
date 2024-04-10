package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"

	"github.com/redis/rueidis"
	syslog "gopkg.in/mcuadros/go-syslog.v2"
	"gopkg.in/mcuadros/go-syslog.v2/format"
)

var (
	redisAddr     string
	cidrKeyPrefix string
	cidrWebPrefix string
	expiration    int
	redisDB       int
	httpPort      string
	ctx, cancel   = context.WithCancel(context.Background())
	rdb           rueidis.Client
	err           error
)

func init() {
	flag.StringVar(&redisAddr, "redis", "localhost:6379", "Redis server address")
	flag.StringVar(&cidrKeyPrefix, "prefix"+":", "cidrs:", "Prefix for keys to store CIDRs")
	flag.StringVar(&cidrWebPrefix, "/"+"prefix", "/cidrs", "Prefix for keys to get CIDRs")
	flag.IntVar(&expiration, "expiration", 86400, "Expiration time for individual CIDRs (in seconds)")
	flag.IntVar(&redisDB, "redisdb", 2, "Select Redis DB")
	flag.StringVar(&httpPort, "port", "8080", "HTTP server port")
	flag.Parse()

	// Initialize Redis client
	rdb, err = rueidis.NewClient(rueidis.ClientOption{
		InitAddress:      []string{redisAddr},
		DisableCache:     true,
		AlwaysPipelining: true,
		SelectDB:         redisDB,
	})
	if err != nil {
		log.Fatalf("Error while creating new connection error = %v", err)
	}
}

func main() {
	defer cancel()

	// Handle OS signals to trigger cancellation
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	go func() {
		<-sigCh
		fmt.Println("Received termination signal. Shutting down...")
		cancel()
	}()

	// Start syslog server
	go startSyslogServer()

	// Start HTTP server
	http.HandleFunc(cidrWebPrefix, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-ctx.Done():
			http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
			return
		default:
			// Retrieve all CIDRs from Redis
			cidrs, err := getAllCIDRs()
			if err != nil {
				http.Error(w, "Failed to retrieve CIDRs", http.StatusInternalServerError)
				return
			}

			// Generate plain text response
			w.Header().Set("Content-Type", "text/plain")
			for _, cidr := range cidrs {
				fmt.Fprintf(w, "%s\n", cidr)
			}
		}
	})

	// Start listening on the specified port
	fmt.Printf("HTTP Server listening on port %s\n", httpPort)
	server := http.Server{Addr: ":" + httpPort}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Println("HTTP server error:", err)
		}
	}()

	// Monitor context cancellation
	<-ctx.Done()

	// Shutdown HTTP server gracefully
	fmt.Println("Shutting down HTTP server...")
	if err := server.Shutdown(context.Background()); err != nil {
		fmt.Println("HTTP server shutdown error:", err)
	}
}

func startSyslogServer() {
	// Create a new syslog server
	channel := make(syslog.LogPartsChannel)
	handler := syslog.NewChannelHandler(channel)

	server := syslog.NewServer()
	server.SetFormat(syslog.RFC5424)
	server.SetHandler(handler)

	go func(channel syslog.LogPartsChannel) {
		for logParts := range channel {
			cidrs := extractCIDRsFromMessage(&logParts)
			if len(cidrs) == 0 {
				return
			}

			// Store CIDRs in Redis with expiration
			for _, cidr := range cidrs {
				key := cidrKeyPrefix + cidr
				resp := rdb.Do(ctx, rdb.B().Setex().Key(key).Seconds(int64(expiration)).Value("1").Build())
				if err := resp.Error(); err != nil {
					fmt.Println("Error inserting CIDR into Redis:", err)
				}
			}
		}
	}(channel)

	// Listen on UDP port 514 for syslog messages
	if err := server.ListenUDP("0.0.0.0:514"); err != nil {
		fmt.Println("Error starting syslog server:", err)
		return
	}

	// Start the syslog server
	if err := server.Boot(); err != nil {
		fmt.Println("Error starting syslog server:", err)
		return
	}

	fmt.Printf("Syslog Server listening on port %s\n", "0.0.0.0:514")

	server.Wait()

	<-ctx.Done()

	// Shutdown syslog server gracefully
	if err := server.Kill(); err != nil {
		fmt.Println("Syslog server shutdown error:", err)
	}
}

func extractCIDRsFromMessage(m *format.LogParts) []string {
	// Extract CIDRs from the message
	cidrRegex := regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}\b`)
	text := (*m)["message"]
	matches := cidrRegex.FindAllString(text.(string), -1)

	return matches
}

func getAllCIDRs() ([]string, error) {
	// Get all keys matching the prefix
	keys, err := rdb.Do(ctx, rdb.B().Keys().Pattern(cidrKeyPrefix+"*").Build()).AsStrSlice()
	if err != nil {
		return nil, err
	}

	// Extract CIDRs from keys
	var cidrs []string
	for _, key := range keys {
		// Remove the prefix from the key
		cidr := strings.TrimPrefix(key, cidrKeyPrefix)
		cidrs = append(cidrs, cidr)
	}
	return cidrs, nil
}
