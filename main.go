package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pelletier/go-toml"
)

var Config struct {
	General struct {
		EventName string `toml:"event_name"`
		AuthHeader string `toml:"auth_header"`
	} `toml:"general"`
	Airtable struct {
		Enabled        bool   `toml:"enabled"`
		APIKey         string `toml:"api_key"`
		Program        string `toml:"program"`
		RefreshMinutes int    `toml:"refresh_minutes"`
	} `toml:"airtable"`
	Slack struct {
		Enabled  bool     `toml:"enabled"`
		BotToken string   `toml:"bot_token"`
		AppToken string   `toml:"app_token"`
		Channels []string `toml:"channels"`
	} `toml:"slack"`
	Plausible struct {
		Enabled   bool     `toml:"enabled"`
		APIKey    string   `toml:"api_key"`
		BaseURL   string   `toml:"base_url"`
		Sites     []string `toml:"sites"`
		Metrics   []string `toml:"metrics"`
		DateRange string   `toml:"date_range"`
	} `toml:"plausible"`
}

func readConfig(filename string) {
	wd, err := os.Getwd();
	if err != nil { log.Fatal("couldn't get current directory"); }
	path := filepath.Join(wd, "config.toml");
	file, err := os.Open(path);
	defer file.Close();
	if err != nil { log.Fatal("couldn't open TOML config file"); }
	configData, err := io.ReadAll(file);
	err = toml.Unmarshal(configData, &Config);
	if err != nil { log.Fatal("failed to parse config file as TOML"); }
}

func main() {
	// read our TOML config file real quick
	readConfig("config.toml")

	mux := http.NewServeMux()
	// what /yswsdashboard sends to the TRMNL plugin, keyed by source
	dashboard := map[string]func() any{}

	if Config.Airtable.Enabled {
		a := Config.Airtable
		if a.Program == "" { a.Program = Config.General.EventName; }
		airtableManager := NewAirtableManager(a.APIKey, a.Program, a.RefreshMinutes)
		go airtableManager.Run(context.Background())
		dashboard["airtable"] = func() any { return airtableManager.Stats() }
		mux.HandleFunc("GET /airtable", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(airtableManager.Stats())
		})
	}

	if Config.Slack.Enabled {
		slackManager :=NewSlackManager(Config.Slack.BotToken, Config.Slack.AppToken, Config.Slack.Channels)
		go func() {
			if err := slackManager.Run(context.Background()); err != nil { log.Fatal(err); }
		}()
		dashboard["slack"] = func() any { return slackManager.Stats() }
		mux.HandleFunc("GET /slack", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"channels": slackManager.Stats()})
		})
	}

	if Config.Plausible.Enabled {
		p := Config.Plausible
		plausibleManager := NewPlausibleManager(p.APIKey, p.BaseURL, p.Sites, p.Metrics, p.DateRange)
		go plausibleManager.Run(context.Background())
		dashboard["plausible"] = func() any { return plausibleManager.Stats() }
		mux.HandleFunc("GET /plausible", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"sites": plausibleManager.Stats()})
		})
	}

	mux.HandleFunc("GET /yswsdashboard", func(w http.ResponseWriter, r *http.Request) {
		if !(strings.Contains(r.Header.Get("Authorization"), Config.General.AuthHeader)) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized);
			return;
		}
		// TRMNL turns each top-level key into a variable for the plugin's markup
		data := map[string]any{"event_name": Config.General.EventName}
		for name, stats := range dashboard { data[name] = stats(); }
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	})

	server := http.Server{
		Addr: "127.0.0.1:8090",
		Handler: mux,
		ReadTimeout: time.Second * 10,
		ReadHeaderTimeout: time.Second * 5,
		WriteTimeout: time.Second * 30,
		IdleTimeout: time.Second * 120,
	}
	log.Fatal(server.ListenAndServe())
}