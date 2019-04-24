package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/jackc/pgx"
	"github.com/mitchellh/go-homedir"
)

// Configuration file
type TomlConfig struct {
	Pg PGInfo
}
type PGInfo struct {
	Database       string
	NumConnections int `toml:"num_connections"`
	Port           int
	Password       string
	Server         string
	SSL            bool
	Username       string
}

var (
	// Application config
	Conf TomlConfig

	// Display debugging messages?
	debug = false

	// PostgreSQL Connection pool
	pg *pgx.ConnPool

	// The starting point in time for entries to be processed, and the length of time to cover
	startTime  = time.Date(2019, time.April, 1, 0, 0, 0, 0, time.UTC)
	timePeriod = time.Hour * 24 * 31
)

func main() {
	// Override config file location via environment variables
	var err error
	configFile := os.Getenv("CONFIG_FILE")
	if configFile == "" {
		userHome, err := homedir.Dir()
		if err != nil {
			log.Fatalf("User home directory couldn't be determined: %s", "\n")
		}
		configFile = filepath.Join(userHome, ".db4s", "status_updater.toml")
	}

	// Read our configuration settings
	if _, err = toml.DecodeFile(configFile, &Conf); err != nil {
		log.Fatal(err)
	}

	// Setup the PostgreSQL config
	pgConfig := new(pgx.ConnConfig)
	pgConfig.Host = Conf.Pg.Server
	pgConfig.Port = uint16(Conf.Pg.Port)
	pgConfig.User = Conf.Pg.Username
	pgConfig.Password = Conf.Pg.Password
	pgConfig.Database = Conf.Pg.Database
	clientTLSConfig := tls.Config{InsecureSkipVerify: true}
	if Conf.Pg.SSL {
		pgConfig.TLSConfig = &clientTLSConfig
	} else {
		pgConfig.TLSConfig = nil
	}

	// Connect to PG
	pgPoolConfig := pgx.ConnPoolConfig{*pgConfig, Conf.Pg.NumConnections, nil, 5 * time.Second}
	pg, err = pgx.NewConnPool(pgPoolConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer pg.Close()

	// Log successful connection
	if debug {
		fmt.Printf("Connected to PostgreSQL server: %v\n", Conf.Pg.Server)
	}

	// Process entries from the given starting point
	err = processRange(startTime)
	if err != nil {
		log.Print(err)
	}
}

// Returns the 3 letter country code associated with a given IPv4 address
func countryLookupIPv4(ipAddress string) (country string) {
	// Break the IPv4 address into octets
	var part1, part2, part3, part4 int
	ip := strings.Split(ipAddress, ".")
	if len(ip) != 4 {
		log.Fatalf("Unknown IPv4 address string format")
	}
	part1, err := strconv.Atoi(ip[0])
	if err != nil {
		log.Fatal(err)
	}
	part2, err = strconv.Atoi(ip[1])
	if err != nil {
		log.Fatal(err)
	}
	part3, err = strconv.Atoi(ip[2])
	if err != nil {
		log.Fatal(err)
	}
	part4, err = strconv.Atoi(ip[3])
	if err != nil {
		log.Fatal(err)
	}

	// Convert the IP address pieces to the correct lookup value
	ipVal := part4 + (part3 * 256) + (part2 * 256 * 256) + (part1 * 256 * 256 * 256)

	// Look up the country code for the IP address
	dbQuery := `
		SELECT cntry
		FROM country_code_lookups
		WHERE ipfrom < $1
			AND ipto > $2`
	err = pg.QueryRow(dbQuery, ipVal, ipVal).Scan(&country)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Looking up the country code for '%s' failed: %v\n", ipAddress, err)
	}
	return
}

// This function does the actual work of querying the PG database and updating rows with the country code
func processRange(startTime time.Time) (err error) {
	// Determine the end processing time
	endTime := startTime.Add(timePeriod)

	// Begin PostgreSQL transaction
	tx, err := pg.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Set up an automatic transaction roll back if the function exits without committing
	defer func() {
		err = tx.Rollback()
		if err != nil {
			log.Println(err)
		}
	}()

	// Select all download rows with a stored IPv4 address and a NULL country code field

	// Display the date range being processed
	fmt.Printf("Processing range '%v' - '%v'\n", startTime.UTC().Format(time.RFC822), endTime.UTC().Format(time.RFC822))

	var rows *pgx.Rows
	dbQuery := `
		SELECT download_id, request_time, client_ipv4
		FROM download_log
		WHERE client_ipv4 IS NOT NULL
			AND client_country IS NULL
			AND request_time > $1
			AND request_time < $2`
	rows, err = tx.Query(dbQuery, startTime, endTime)
	if err != nil {
		log.Printf("Retrieving unprocessed IPv4 addresses failed: %v\n", err)
		return // This will automatically call the transaction rollback code
	}
	var countryCode, ipAddress string
	var reqTime time.Time
	var downloadID int64
	for rows.Next() {
		err = rows.Scan(&downloadID, &reqTime, &ipAddress)
		if err != nil {
			log.Printf("Error retrieving unprocessed IPv4 address: %v\n", err)
			rows.Close()
			return // This will automatically call the transaction rollback code
		}

		// Do the country code lookup for the IPv4 address
		countryCode = countryLookupIPv4(ipAddress)
		if countryCode != "" {
			// Debugging info
			if debug {
				log.Printf("Processing request #%d dated '%v' : IPv4: '%s' : Country code: '%s'\n",
					downloadID, reqTime.UTC().Format(time.RFC822), ipAddress, countryCode)
			}

			// * Update the download row with the country code information *

			// Begin nested PostgreSQL transaction
			var tx2 *pgx.Tx
			tx2, err = pg.Begin()
			if err != nil {
				log.Fatal(err)
			}

			// Save the updated list for the user back to PG
			var commandTag pgx.CommandTag
			dbQuery = `
				UPDATE download_log
				SET client_country = $2
				WHERE download_id = $1`
			commandTag, err = tx2.Exec(dbQuery, downloadID, countryCode)
			if err != nil {
				log.Printf("Updating download ID '%d' with country code '%s' failed: %v", downloadID, countryCode,
					err)
				err2 := tx2.Rollback()
				if err2 != nil {
					log.Print(err2)
				}
				return // This will automatically call the outer transaction rollback code
			}
			if numRows := commandTag.RowsAffected(); numRows != 1 {
				log.Printf("Wrong number of rows affected (%v) when updating download ID '%d' with country code "+
					"'%s'", numRows, downloadID, countryCode)
				err = tx2.Rollback()
				if err != nil {
					log.Print(err)
				}
				return // This will automatically call the outer transaction rollback code
			}

			// Commit nested PostgreSQL transaction
			err = tx2.Commit()
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	// This seems to commit the outer transaction, so no need to do it explicitly
	rows.Close()

	// Display completion message
	fmt.Printf("Country codes updated for '%v' - '%v'\n", startTime.UTC().Format(time.RFC822),  endTime.UTC().Format(time.RFC822))
	return
}
