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
	sqlite "github.com/gwenn/gosqlite"
	"github.com/jackc/pgx"
	"github.com/mitchellh/go-homedir"
)

// Configuration file
type TomlConfig struct {
	Geo GeoInfo
	Pg  PGInfo
}
type GeoInfo struct {
	Path string // Path to the Geo-IP.sqlite file
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
	debug = true

	// PostgreSQL Connection pool
	pg *pgx.ConnPool

	// SQLite pieces
	sdb  *sqlite.Conn
	stmt *sqlite.Stmt
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
		configFile = filepath.Join(userHome, ".db4s", "downloader_config.toml")
	}

	// Read our configuration settings
	if _, err = toml.DecodeFile(configFile, &Conf); err != nil {
		log.Fatal(err)
	}

	// Open the Geo-IP database, for country lookups
	sdb, err = sqlite.Open(Conf.Geo.Path)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err = sdb.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	// Log successful connection
	if debug {
		fmt.Printf("Connected to Geo-IP database: %v\n", Conf.Geo.Path)
	}

	// Create the SQLite prepared statement for IP address to country code lookups
	prepQuery := `
		SELECT cntry
		FROM ipv4
		WHERE ipfrom < ?
			AND ipto > ?`
	stmt, err = sdb.Prepare(prepQuery)
	if err != nil {
		log.Fatalf("Error when preparing statement for database: %s\n", err)
	}
	defer func() {
		err = stmt.Finalize()
		if err != nil {
			log.Println(err)
		}
	}()

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
		fmt.Printf("Connected to PostgreSQL server: %v:%v\n", Conf.Pg.Server, uint16(Conf.Pg.Port))
	}

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

	// TODO: Don't use hard coded time range here
	startTime := time.Date(2019, time.April, 15, 0, 2, 0, 0, time.UTC)
	endTime := time.Date(2019, time.April, 15, 0, 12, 0, 0, time.UTC)

	// Debugging info
	if debug {
		fmt.Printf("Processing range '%v' - '%v'\n", startTime.Format(time.RFC822), endTime.Format(time.RFC822))
	}

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
		countryCode, err = countryLookupIPv4(ipAddress)
		if err != nil {
			log.Print(err)
			return // This will automatically call the transaction rollback code
		}

		// Debugging info
		if debug {
			log.Printf("Processing request #%d dated '%v' : IPv4: '%s' : Country code: '%s'\n",
				downloadID, reqTime.Format(time.RFC822), ipAddress, countryCode)
		}

		// * Update the download row with the country code information *

		// Begin nested PostgreSQL transaction
		tx2, err := pg.Begin()
		if err != nil {
			log.Fatal(err)
		}

		// Save the updated list for the user back to PG
		dbQuery = `
				UPDATE download_log
				SET client_country = $2
				WHERE download_id = $1`
		commandTag, err := tx2.Exec(dbQuery, downloadID, countryCode)
		if err != nil {
			log.Printf("Updating download ID '%d' with country code '%s' failed: %v", downloadID, countryCode,
				err)
			err = tx2.Rollback()
			if err != nil {
				log.Print(err)
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
	// This seems to commit the outer transaction, so no need to do it explicitly
	rows.Close()

	if debug {
		fmt.Println("Country codes updated")
	}
}

// Returns the 3 letter country code associated with a given IPv4 address
func countryLookupIPv4(ipAddress string) (country string, err error) {
	// Break the IPv4 address into octets
	var part1, part2, part3, part4 int
	ip := strings.Split(ipAddress, ".")
	if len(ip) != 4 {
		log.Fatalf("Unknown IPv4 address string format")
	}
	part1, err = strconv.Atoi(ip[0])
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
	err = stmt.Select(func(s *sqlite.Stmt) (innerErr error) {
		innerErr = s.Scan(&country)
		return
	}, ipVal, ipVal)
	return
}
