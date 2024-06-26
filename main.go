package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"

	"github.com/redis/rueidis"
	"go4.org/netipx"
)

var (
	redisAddr     string
	cidrKeyPrefix string
	cidrWebPrefix string
	expiration    time.Duration
	redisDelay    time.Duration
	randomness    float64
	redisDB       int
	httpPort      string
)

func init() {
	flag.StringVar(&redisAddr, "redis", "localhost:6379", "Redis server address")
	flag.StringVar(&cidrKeyPrefix, "redisprefix", "cidrs:", "Prefix for keys to store CIDRs")
	flag.StringVar(&cidrWebPrefix, "webprefix", "/cidrs", "Prefix for HTTP CIDRs endpoint")
	flag.DurationVar(&expiration, "expiration", time.Hour*24, "Expiration time for individual CIDRs (in seconds)")
	flag.Float64Var(&randomness, "randomness", 1.5, "Expiration time randomness")
	flag.IntVar(&redisDB, "redisdb", 2, "Select Redis DB")
	flag.StringVar(&httpPort, "port", "8080", "HTTP server port")
	flag.DurationVar(&redisDelay, "redis-delay", time.Millisecond*500, "Delay Redis TS.ADDs duration")
	flag.Parse()
}

func main() {
	// Create a parent context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals to trigger cancellation
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	go func() {
		<-sigCh
		log.Println("Received termination signal. Shutting down...")
		cancel()
	}()

	// Initialize Redis client
	rdb, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:      []string{redisAddr},
		MaxFlushDelay:    redisDelay,
		DisableCache:     true,
		AlwaysPipelining: true,
		SelectDB:         redisDB,
	})
	if err != nil {
		log.Fatalf("Error while creating new connection error = %v", err)
	}

	resp := rdb.Do(ctx, rdb.B().Ping().Build())

	if err := resp.Error(); err != nil {
		log.Fatalf("Error while pinging redis server = %v", err)
	} else {
		log.Printf("Received PONG from Redis at address: %s, DB: %d...", redisAddr, redisDB)
	}

	// Start syslog server
	go startSyslogServer(ctx, rdb)

	// Start HTTP server
	http.HandleFunc(cidrWebPrefix, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-ctx.Done():
			http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
			return
		default:
			// Retrieve all CIDRs from Redis
			cidrs, err := getAllCIDRs(ctx, rdb)
			if err != nil {
				http.Error(w, "Failed to retrieve CIDRs", http.StatusInternalServerError)
				return
			}

			// Generate plain text response
			w.Header().Set("Content-Type", "text/plain")
			for _, cidr := range cidrs {
				fmt.Fprintf(w, "%s\n", cidr)
			}
			log.Printf("HTTP Web request for URL: %s, from: %s, header: %s\n", r.URL, r.RemoteAddr, r.Header)
		}
	})

	// Start listening on the specified port
	log.Printf("HTTP Server listening on port %s\n", httpPort)
	server := http.Server{Addr: ":" + httpPort}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Println("HTTP server error:", err)
		}
	}()

	// Monitor context cancellation
	<-ctx.Done()

	// Shutdown HTTP server gracefully
	log.Println("Shutting down HTTP server...")
	if err := server.Shutdown(context.Background()); err != nil {
		log.Println("HTTP server shutdown error:", err)
	}
}

func startSyslogServer(ctx context.Context, rdb rueidis.Client) {
	// Set up UDP listener on port 514
	udpAddr, err := net.ResolveUDPAddr("udp", ":514")
	if err != nil {
		log.Fatalf("Failed to resolve UDP address: %v", err)
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP: %v", err)
	}
	defer udpConn.Close()

	// Set up TCP listener on port 514
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":514")
	if err != nil {
		log.Fatalf("Failed to resolve TCP address: %v", err)
	}

	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatalf("Failed to listen on TCP: %v", err)
	}
	defer tcpListener.Close()

	log.Println("Rsyslog server is listening on port 514...")

	// Start a goroutine to handle UDP syslog messages
	go handleSyslogMessages(ctx, udpConn, rdb)

	// Start a goroutine to handle TCP syslog messages
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				tcpConn, err := tcpListener.Accept()
				if err != nil {
					log.Printf("Failed to accept TCP connection: %v", err)
					continue
				}
				go handleSyslogMessages(ctx, tcpConn, rdb)
			}
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	<-ctx.Done()

	log.Println("Shutting down syslog server...")
}

func handleSyslogMessages(ctx context.Context, conn net.Conn, rdb rueidis.Client) {
	defer conn.Close()
	reader := bufio.NewScanner(conn)
	reader.Split(bufio.ScanLines)
	for reader.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
			conn.SetReadDeadline(time.Now().Add(60 * time.Second)) // Set read deadline to avoid blocking indefinitely
			line := reader.Bytes()
			// Print the received syslog message
			log.Printf("Received syslog message: %s\n", line[:])
			insertCIDRsToRedis(ctx, rdb, extractCIDRsFromMessage(string(line[:])))
		}
	}
}

func extractCIDRsFromMessage(m string) map[string]string {
	// Extract CIDRs from the message
	cidrRegex := regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}\b`)
	matchesr := cidrRegex.FindAllString(m, -1)
	var matches = make(map[string]string)
	for _, ms := range matchesr {
		if c, err := netip.ParsePrefix(ms); err != nil {
			log.Println("Error parsing CIDR:", err)
			return matches
		} else if c.Bits() < 24 || ms != c.String() || invalidCIDR(c) {
			log.Printf("Error CIDR conversion - origin - %s - convert - %s\n", ms, c.String())
			return matches
		} else {
			matches[c.String()] = m
		}
	}
	return matches
}

func insertCIDRsToRedis(ctx context.Context, rdb rueidis.Client, c map[string]string) {
	if len(c) == 0 {
		return
	}

	// Store CIDRs in Redis with expiration
	for cidr, msg := range c {
		key := cidrKeyPrefix + cidr
		//log.Printf("Trying to insert %s key into Redis\n", key)
		r := rand.Intn(int(expiration.Seconds()*randomness)) + int(expiration.Seconds())
		resp := rdb.Do(ctx, rdb.B().Set().Key(key).Value(msg).Build())
		if err := resp.Error(); err != nil {
			log.Println("Error inserting CIDR into Redis:", err)
			return
		}
		resp = rdb.Do(ctx, rdb.B().Expire().Key(key).Seconds(int64(r)).Build())
		if err := resp.Error(); err != nil {
			log.Println("Error expiring CIDR into Redis:", err)
			return
		}
	}
}

func getAllCIDRs(ctx context.Context, rdb rueidis.Client) ([]string, error) {
	// Get all keys matching the prefix
	keys, err := rdb.Do(ctx, rdb.B().Keys().Pattern(cidrKeyPrefix+"*").Build()).AsStrSlice()
	if err != nil {
		return nil, err
	}
	// Extract CIDRs from keys
	var cidrs []string
	var sorted netipx.IPSetBuilder
	for _, key := range keys {
		// Remove the prefix from the key
		add := strings.TrimPrefix(key, cidrKeyPrefix)
		if c, err := netip.ParsePrefix(add); err != nil {
			log.Println("Error parsing CIDR:", err)
			continue
		} else if ones := c.Bits(); ones < 24 {
			log.Printf("Wrong bits CIDR: %#v, from address: %#v, from key: %#v\n", c.String(), add, key)
			continue
		} else {
			sorted.AddPrefix(c)
		}
	}
	merged, _ := sorted.IPSet()
	for _, c := range merged.Prefixes() {
		cidrs = append(cidrs, c.String())
	}
	return cidrs, nil
}

func invalidCIDR(c netip.Prefix) bool {
	var invalidCIDR netipx.IPSetBuilder
	invalidCIDR.AddPrefix(netip.MustParsePrefix("0.0.0.0/8"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("10.0.0.0/8"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("100.64.0.0/10"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("127.0.0.0/8"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("169.254.0.0/16"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("172.16.0.0/12"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("192.0.0.0/24"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("192.0.2.0/24"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("192.88.99.0/24"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("192.168.0.0/16"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("198.18.0.0/15"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("198.51.100.0/24"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("203.0.113.0/24"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("240.0.0.0/4"))
	invalidCIDR.AddPrefix(netip.MustParsePrefix("255.255.255.255/32"))

	r, _ := invalidCIDR.IPSet()
	return r.ContainsPrefix(c)
}
