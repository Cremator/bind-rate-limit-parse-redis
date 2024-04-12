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
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"

	"github.com/redis/rueidis"
	"github.com/thcyron/cidrmerge"
)

var (
	redisAddr     string
	cidrKeyPrefix string
	cidrWebPrefix string
	expiration    int
	redisDB       int
	httpPort      string
)

func init() {
	flag.StringVar(&redisAddr, "redis", "localhost:6379", "Redis server address")
	flag.StringVar(&cidrKeyPrefix, "prefix"+":", "cidrs:", "Prefix for keys to store CIDRs")
	flag.StringVar(&cidrWebPrefix, "/"+"prefix", "/cidrs", "Prefix for HTTP CIDRs endpoint")
	flag.IntVar(&expiration, "expiration", 86400, "Expiration time for individual CIDRs (in seconds)")
	flag.IntVar(&redisDB, "redisdb", 2, "Select Redis DB")
	flag.StringVar(&httpPort, "port", "8080", "HTTP server port")
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
			log.Printf("HTTP Web request for URL: %s, from: %s\n", r.URL, r.RemoteAddr)
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

func extractCIDRsFromMessage(m string) []string {
	// Extract CIDRs from the message
	cidrRegex := regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/\d{1,2}\b`)
	matchesr := cidrRegex.FindAllString(m, -1)
	var matches []string
	for _, ms := range matchesr {
		if _, c, err := net.ParseCIDR(ms); err != nil {
			log.Println("Error parsing CIDR:", err)
			return matches
		} else if ones, _ := c.Mask.Size(); ones < 24 || ms != c.String() || !validCIDR(c) {
			log.Printf("Error CIDR conversion - origin - %s - convert - %s\n", ms, c.String())
			return matches
		} else {
			matches = append(matches, c.String())
		}
	}
	return matches
}

func insertCIDRsToRedis(ctx context.Context, rdb rueidis.Client, c []string) {
	if len(c) == 0 {
		return
	}

	// Store CIDRs in Redis with expiration
	for _, cidr := range c {
		key := cidrKeyPrefix + cidr
		//log.Printf("Trying to insert %s key into Redis\n", key)
		r := rand.Intn(expiration) + expiration
		resp := rdb.Do(ctx, rdb.B().Set().Key(key).Value("1").Build())
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
	var sorted []*net.IPNet
	for _, key := range keys {
		// Remove the prefix from the key
		add := strings.TrimPrefix(key, cidrKeyPrefix)
		if _, c, err := net.ParseCIDR(add); err != nil {
			log.Println("Error parsing CIDR:", err)
			continue
		} else if ones, _ := c.Mask.Size(); ones < 24 {
			log.Printf("Wrong bits CIDR: %#v, from address: %#v, from key: %#v\n", c.String(), add, key)
			continue
		} else {
			sorted = append(sorted, c)
		}
	}
	merged := cidrmerge.Merge(sorted)
	for _, c := range merged {
		cidrs = append(cidrs, c.String())
	}
	return cidrs, nil
}

func validCIDR(c *net.IPNet) bool {
	invalidCIDR := []*net.IPNet{}
	invalidList := []string{
		"0.0.0.0/8",
		"10.0.0.0/8",
		"100.64.0.0/10",
		"127.0.0.0/8",
		"169.254.0.0/16",
		"172.16.0.0/12",
		"192.0.0.0/24",
		"192.0.2.0/24",
		"192.88.99.0/24",
		"192.168.0.0/16",
		"198.18.0.0/15",
		"198.51.100.0/24",
		"203.0.113.0/24",
		"240.0.0.0/4",
		"255.255.255.255/32",
	}

	for _, l := range invalidList {
		_, ll, _ := net.ParseCIDR(l)
		invalidCIDR = append(invalidCIDR, ll)
	}
	for _, r := range invalidCIDR {
		if r.Contains(c.IP) {
			return false
		}
	}
	return true
}
