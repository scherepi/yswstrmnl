package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/mehanizm/airtable"
)

// IDs in the Unified YSWS Projects DB. IDs instead of names so renames in the
// base don't break anything.
const (
	unifiedBaseID          = "app3A5kJwYqxMLOgh"
	unifiedProgramsTable   = "tblrGi9RARJy1A0c5" // YSWS Programs
	weightedThisMonthField = "flddWQbhmy3oytww2" // Weighted Projects This Month
	weightedTotalField     = "fldgfs70478BwFFBj" // Weighted–Total
)

// Weighted–Total gets logged here once a day, so the plugin can show it day by day
const historyFile = "weighted_history.json"

type ProgramStats struct {
	Program                   string             `json:"program"`
	WeightedProjectsThisMonth float64            `json:"weighted_projects_this_month"`
	WeightedTotal             float64            `json:"weighted_total"`
	History                   map[string]float64 `json:"history"` // YYYY-MM-DD -> Weighted–Total at the end of that day
	UpdatedAt                 time.Time          `json:"updated_at"`
	Ready                     bool               `json:"ready"` // false until the first successful lookup
}

// AirtableManager periodically looks up the program's row in the Unified DB.
type AirtableManager struct {
	client   *airtable.Client
	interval time.Duration

	mu    sync.RWMutex
	stats ProgramStats
}

func NewAirtableManager(apiKey, program string, refreshMinutes int) *AirtableManager {
	if refreshMinutes <= 0 {
		refreshMinutes = 15
	}
	client := airtable.NewClient(apiKey)
	// the default client has no timeout
	client.SetCustomClient(&http.Client{Timeout: 30 * time.Second})
	a := &AirtableManager{
		client:   client,
		interval: time.Duration(refreshMinutes) * time.Minute,
		stats:    ProgramStats{Program: program, History: map[string]float64{}},
	}
	// no file just means no history yet, but don't start over on a broken one or we'd overwrite it
	if data, err := os.ReadFile(historyFile); err == nil {
		if err := json.Unmarshal(data, &a.stats.History); err != nil {
			log.Fatalf("airtable: couldn't parse %s: %v", historyFile, err)
		}
	}
	return a
}

// Run refreshes the stats every refresh interval until ctx is cancelled.
func (a *AirtableManager) Run(ctx context.Context) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		thisMonth, total, err := a.lookup(ctx)
		if err != nil {
			// keep serving the last good values
			log.Printf("airtable: couldn't look up %q: %v", a.stats.Program, err)
		} else {
			a.mu.Lock()
			a.stats.WeightedProjectsThisMonth = thisMonth
			a.stats.WeightedTotal = total
			// overwritten every refresh, so each day ends up with the last value seen that day
			a.stats.History[time.Now().Format(time.DateOnly)] = total
			a.stats.UpdatedAt = time.Now()
			a.stats.Ready = true
			history, _ := json.MarshalIndent(a.stats.History, "", "  ")
			a.mu.Unlock()
			if err := os.WriteFile(historyFile, history, 0o644); err != nil {
				log.Printf("airtable: couldn't save %s: %v", historyFile, err)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Stats returns a snapshot of the program's stats.
func (a *AirtableManager) Stats() ProgramStats {
	a.mu.RLock()
	defer a.mu.RUnlock()
	st := a.stats
	// copy the map so Run can't change it while it's being encoded
	st.History = maps.Clone(st.History)
	return st
}

// lookup finds the program's row in YSWS Programs by name and reads its
// Weighted Projects This Month and Weighted–Total rollups.
func (a *AirtableManager) lookup(ctx context.Context) (float64, float64, error) {
	records, err := a.client.GetTable(unifiedBaseID, unifiedProgramsTable).GetRecords().
		WithFilterFormula(fmt.Sprintf("{Name} = %q", a.stats.Program)).
		ReturnFields(weightedThisMonthField, weightedTotalField).
		WithFieldsById().
		MaxRecords(1).
		DoContext(ctx)
	if err != nil {
		return 0, 0, err
	}
	if len(records.Records) == 0 {
		return 0, 0, fmt.Errorf("no program with that name in YSWS Programs")
	}
	fields := records.Records[0].Fields
	thisMonth, ok1 := fields[weightedThisMonthField].(float64)
	total, ok2 := fields[weightedTotalField].(float64)
	// rollup errors come back as objects like {"specialValue": "NaN"}
	if !ok1 || !ok2 {
		return 0, 0, fmt.Errorf("unexpected field values: %v", fields)
	}
	// they're sums of decimals, so they come back as e.g. 16.960000000000004
	return math.Round(thisMonth*100) / 100, math.Round(total*100) / 100, nil
}

// subtypes that are things people (or bots) actually posted, as opposed to
// "x has joined the channel", topic changes, etc.
var countedSubtypes = map[string]bool{
	"":                 true,
	"bot_message":      true,
	"file_share":       true,
	"me_message":       true,
	"thread_broadcast": true,
}

type ChannelStats struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Members  int    `json:"members"`
	Messages int    `json:"messages"` // top-level messages
	Replies  int    `json:"replies"`  // thread replies (including ones also sent to the channel)
	Ready    bool   `json:"ready"`    // false until the history backfill finishes
}

// SlackManager counts messages by backfilling each channel's history once at
// startup, then keeping the count current with message events over Socket Mode.
type SlackManager struct {
	api      *slack.Client
	socket   *socketmode.Client
	channels []string

	mu    sync.RWMutex
	stats map[string]*ChannelStats
	// live events only count messages newer than this, the backfill covers everything up to it
	cutoff string
}

func NewSlackManager(botToken, appToken string, channels []string) *SlackManager {
	api := slack.New(botToken, slack.OptionAppLevelToken(appToken))
	s := &SlackManager{
		api:    api,
		socket: socketmode.New(api),
		stats:  map[string]*ChannelStats{},
	}
	for _, id := range channels {
		if _, dup := s.stats[id]; dup {
			continue
		}
		s.channels = append(s.channels, id)
		s.stats[id] = &ChannelStats{ID: id}
	}
	return s
}

// Run connects to Slack and keeps the stats up to date until ctx is cancelled.
func (s *SlackManager) Run(ctx context.Context) error {
	if _, err := s.api.AuthTestContext(ctx); err != nil {
		return fmt.Errorf("slack auth failed, check bot_token: %w", err)
	}

	// done before the cutoff below, so nothing posted while joining gets missed
	for _, id := range s.channels {
		s.join(ctx, id)
	}

	go s.pollInfo(ctx)

	connected := make(chan struct{})
	go s.handleEvents(ctx, connected)
	go s.keepConnected(ctx)

	select {
	case <-connected:
	case <-ctx.Done():
		return ctx.Err()
	}
	

	// anything posted after this point will come in over the socket
	cutoff := slackTimestamp(time.Now())
	s.mu.Lock()
	s.cutoff = cutoff
	s.mu.Unlock()

	for _, id := range s.channels {
		log.Printf("slack: backfilling %s", id)
		if err := s.backfill(ctx, id, cutoff); err != nil {
			log.Printf("slack: backfill of %s failed (is the bot in the channel?): %v", id, err)
		}
	}

	<-ctx.Done()
	return nil
}

// Stats returns a snapshot of every tracked channel, in config order.
func (s *SlackManager) Stats() []ChannelStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ChannelStats, 0, len(s.channels))
	for _, id := range s.channels {
		out = append(out, *s.stats[id])
	}
	return out
}

// join adds the bot to the channel if it isn't in it yet, since it only gets
// history (not_in_channel otherwise) and message events for channels it's in.
func (s *SlackManager) join(ctx context.Context, id string) {
	ch, err := s.api.GetConversationInfoContext(ctx, &slack.GetConversationInfoInput{ChannelID: id})
	if err != nil {
		// private channels the bot isn't in look like they don't exist
		log.Printf("slack: couldn't find %s, if it's private someone has to /invite the bot: %v", id, err)
		return
	}
	if ch.IsMember {
		return
	}
	if _, _, _, err := s.api.JoinConversationContext(ctx, id); err != nil {
		log.Printf("slack: couldn't join #%s (%s), does the bot have the channels:join scope? %v", ch.Name, id, err)
		return
	}
	log.Printf("slack: joined #%s (%s)", ch.Name, id)
}

// RunContext gives up if a reconnect fails, so keep restarting it.
func (s *SlackManager) keepConnected(ctx context.Context) {
	for {
		err := s.socket.RunContext(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Printf("slack: socket mode connection lost, retrying in 10s: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func (s *SlackManager) handleEvents(ctx context.Context, connected chan struct{}) {
	var once sync.Once
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-s.socket.Events:
			switch evt.Type {
			case socketmode.EventTypeConnected:
				once.Do(func() { close(connected) })
			case socketmode.EventTypeInvalidAuth:
				log.Print("slack: socket mode auth failed, check app_token")
			case socketmode.EventTypeEventsAPI:
				if evt.Request != nil {
					s.socket.Ack(*evt.Request)
				}
				apiEvent, ok := evt.Data.(slackevents.EventsAPIEvent)
				if !ok {
					continue
				}
				if msg, ok := apiEvent.InnerEvent.Data.(*slackevents.MessageEvent); ok {
					s.handleMessage(msg)
				}
			}
		}
	}
}

func (s *SlackManager) handleMessage(ev *slackevents.MessageEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	st, ok := s.stats[ev.Channel]
	// no cutoff yet means the backfill hasn't started, and it'll pick this message up
	if !ok || s.cutoff == "" {
		return
	}

	switch {
	case countedSubtypes[ev.SubType]:
		if ev.TimeStamp <= s.cutoff {
			return
		}
		if isReply(ev.ThreadTimeStamp, ev.TimeStamp) {
			st.Replies++
		} else {
			st.Messages++
		}
	case ev.SubType == "message_deleted" && ev.PreviousMessage != nil:
		prev := ev.PreviousMessage
		if !countedSubtypes[prev.SubType] {
			return
		}
		if isReply(prev.ThreadTimestamp, prev.Timestamp) {
			st.Replies--
		} else {
			st.Messages--
		}
	}
}

// backfill pages through a channel's history (newest first) and counts
// everything posted at or before the cutoff.
func (s *SlackManager) backfill(ctx context.Context, channelID, cutoff string) error {
	params := &slack.GetConversationHistoryParameters{
		ChannelID: channelID,
		Latest:    cutoff,
		Inclusive: true,
		Limit:     200,
	}
	var messages, replies int
	for {
		resp, err := s.api.GetConversationHistoryContext(ctx, params)
		var rateLimited *slack.RateLimitedError
		if errors.As(err, &rateLimited) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(rateLimited.RetryAfter):
			}
			continue
		}
		if err != nil {
			return err
		}

		for _, m := range resp.Messages {
			// people reply in threads under join messages too, so count replies no matter the parent's subtype
			replies += m.ReplyCount
			// broadcasts are already in their parent's reply_count
			if countedSubtypes[m.SubType] && m.SubType != "thread_broadcast" {
				messages++
			}
		}

		if !resp.HasMore {
			break
		}
		params.Cursor = resp.ResponseMetaData.NextCursor
	}

	s.mu.Lock()
	st := s.stats[channelID]
	// += because live events may have already adjusted the counts while we were paging
	st.Messages += messages
	st.Replies += replies
	st.Ready = true
	log.Printf("slack: backfilled #%s (%s): %d messages, %d replies", st.Name, channelID, st.Messages, st.Replies)
	s.mu.Unlock()
	return nil
}

// pollInfo keeps channel names and member counts fresh.
func (s *SlackManager) pollInfo(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		for _, id := range s.channels {
			ch, err := s.api.GetConversationInfoContext(ctx, &slack.GetConversationInfoInput{
				ChannelID:         id,
				IncludeNumMembers: true,
			})
			if err != nil {
				log.Printf("slack: couldn't get info for %s: %v", id, err)
				continue
			}
			s.mu.Lock()
			s.stats[id].Name = ch.Name
			s.stats[id].Members = ch.NumMembers
			s.mu.Unlock()
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type SiteStats struct {
	SiteID    string         `json:"site_id"`
	DateRange string         `json:"date_range"`
	Metrics   map[string]any `json:"metrics"`
	UpdatedAt time.Time      `json:"updated_at"`
	Ready     bool           `json:"ready"` // false until the first successful fetch
}

// PlausibleManager polls the Plausible Stats API (v2) and caches the results.
type PlausibleManager struct {
	apiKey    string
	baseURL   string
	sites     []string
	metrics   []string
	dateRange string
	client    *http.Client

	mu    sync.RWMutex
	stats map[string]*SiteStats
}

func NewPlausibleManager(apiKey, baseURL string, sites, metrics []string, dateRange string) *PlausibleManager {
	if baseURL == "" {
		baseURL = "https://plausible.io"
	}
	if len(metrics) == 0 {
		metrics = []string{"visitors", "pageviews"}
	}
	if dateRange == "" {
		dateRange = "7d"
	}
	p := &PlausibleManager{
		apiKey:    apiKey,
		baseURL:   baseURL,
		metrics:   metrics,
		dateRange: dateRange,
		client:    &http.Client{Timeout: 15 * time.Second},
		stats:     map[string]*SiteStats{},
	}
	for _, site := range sites {
		if _, dup := p.stats[site]; dup {
			continue
		}
		p.sites = append(p.sites, site)
		p.stats[site] = &SiteStats{SiteID: site, DateRange: dateRange}
	}
	return p
}

// Run refreshes every site every 5 minutes until ctx is cancelled. The API
// allows 600 requests an hour, so that's fine for up to ~50 sites.
func (p *PlausibleManager) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		for _, site := range p.sites {
			values, err := p.query(ctx, site)
			if err != nil {
				// keep serving the last good numbers
				log.Printf("plausible: couldn't get stats for %s: %v", site, err)
				continue
			}
			p.mu.Lock()
			st := p.stats[site]
			st.Metrics = values
			st.UpdatedAt = time.Now()
			st.Ready = true
			p.mu.Unlock()
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Stats returns a snapshot of every tracked site, in config order.
func (p *PlausibleManager) Stats() []SiteStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]SiteStats, 0, len(p.sites))
	for _, site := range p.sites {
		out = append(out, *p.stats[site])
	}
	return out
}

func (p *PlausibleManager) query(ctx context.Context, site string) (map[string]any, error) {
	body, err := json.Marshal(map[string]any{
		"site_id":    site,
		"metrics":    p.metrics,
		"date_range": p.dateRange,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", p.baseURL+"/api/v2/query", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s: %s", resp.Status, msg)
	}

	var result struct {
		Results []struct {
			Metrics []any `json:"metrics"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	// with no dimensions there's one row, with metrics in the order we asked for them
	values := map[string]any{}
	if len(result.Results) > 0 {
		row := result.Results[0].Metrics
		for i, name := range p.metrics {
			if i < len(row) {
				values[name] = row[i]
			}
		}
	}
	return values, nil
}

func isReply(threadTS, ts string) bool {
	return threadTS != "" && threadTS != ts
}

// slack timestamps always look like "1700000000.123456", so they compare correctly as strings
func slackTimestamp(t time.Time) string {
	return fmt.Sprintf("%d.%06d", t.Unix(), t.Nanosecond()/1000)
}