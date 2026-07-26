package quota

import (
	"fmt"
	"sync"

	"statground_naver_book_go/internal/envx"
)

const (
	DefaultOfficialDailyLimit      = 30_000
	DefaultAutoDailyBudget         = 12_000
	DefaultManualDailyReserve      = 4_000
	DefaultDaumSearchSharedReserve = 8_000
	DefaultEmergencyReserve        = 6_000
	DefaultMaxRequestsPerRun       = 4_000
)

// Config describes the conservative portion of the Kakao app-wide quota that
// this automated collector may consume. Multiple REST keys do not change these
// limits because Kakao accounts usage at the app level.
type Config struct {
	OfficialDailyLimit      int
	AutoDailyBudget         int
	ManualDailyReserve      int
	DaumSearchSharedReserve int
	EmergencyReserve        int
	MaxRequestsPerRun       int
}

func DefaultConfig() Config {
	return Config{
		OfficialDailyLimit:      DefaultOfficialDailyLimit,
		AutoDailyBudget:         DefaultAutoDailyBudget,
		ManualDailyReserve:      DefaultManualDailyReserve,
		DaumSearchSharedReserve: DefaultDaumSearchSharedReserve,
		EmergencyReserve:        DefaultEmergencyReserve,
		MaxRequestsPerRun:       DefaultMaxRequestsPerRun,
	}
}

// ConfigFromEnv reads only numeric budget policy. It does not read an API key
// and performs no external call.
func ConfigFromEnv() Config {
	cfg := DefaultConfig()
	cfg.OfficialDailyLimit = envx.Int("KAKAO_BOOK_OFFICIAL_DAILY_LIMIT", cfg.OfficialDailyLimit)
	cfg.AutoDailyBudget = envx.Int("KAKAO_BOOK_AUTO_DAILY_BUDGET", cfg.AutoDailyBudget)
	cfg.ManualDailyReserve = envx.Int("KAKAO_BOOK_MANUAL_DAILY_RESERVE", cfg.ManualDailyReserve)
	cfg.DaumSearchSharedReserve = envx.Int(
		"KAKAO_BOOK_DAUM_SEARCH_SHARED_RESERVE",
		envx.Int("KAKAO_BOOK_OTHER_APP_RESERVE", cfg.DaumSearchSharedReserve),
	)
	cfg.EmergencyReserve = envx.Int("KAKAO_BOOK_EMERGENCY_RESERVE", cfg.EmergencyReserve)
	cfg.MaxRequestsPerRun = envx.Int("KAKAO_MAX_REQUESTS_PER_RUN", cfg.MaxRequestsPerRun)
	return cfg
}

func (c Config) Validate() error {
	values := map[string]int{
		"official daily limit":       c.OfficialDailyLimit,
		"auto daily budget":          c.AutoDailyBudget,
		"manual daily reserve":       c.ManualDailyReserve,
		"Daum search shared reserve": c.DaumSearchSharedReserve,
		"emergency reserve":          c.EmergencyReserve,
		"max requests per run":       c.MaxRequestsPerRun,
	}
	for name, value := range values {
		if value < 0 {
			return fmt.Errorf("%s must not be negative", name)
		}
	}
	if c.OfficialDailyLimit == 0 {
		return fmt.Errorf("official daily limit must be positive")
	}
	if c.AutoDailyBudget == 0 {
		return fmt.Errorf("auto daily budget must be positive")
	}
	if c.MaxRequestsPerRun == 0 {
		return fmt.Errorf("max requests per run must be positive")
	}
	reserved := c.ManualDailyReserve + c.DaumSearchSharedReserve + c.EmergencyReserve
	if reserved >= c.OfficialDailyLimit {
		return fmt.Errorf("daily reserves must leave capacity below the official limit")
	}
	if c.AutoDailyBudget+reserved > c.OfficialDailyLimit {
		return fmt.Errorf("auto budget plus reserves exceeds the official daily limit")
	}
	return nil
}

type Budget struct {
	mu            sync.Mutex
	config        Config
	observedCalls int
	reservedCalls int
	exhausted     bool
}

type Snapshot struct {
	OfficialDailyLimit      int
	AutoDailyBudget         int
	ManualDailyReserve      int
	DaumSearchSharedReserve int
	EmergencyReserve        int
	MaxRequestsPerRun       int
	ObservedCallsToday      int
	ReservedCallsThisRun    int
	RemainingAutoCalls      int
	RemainingRunCalls       int
	Exhausted               bool
}

func NewBudget(config Config, observedCallsToday int) (*Budget, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if observedCallsToday < 0 {
		return nil, fmt.Errorf("observed calls today must not be negative")
	}
	return &Budget{
		config:        config,
		observedCalls: observedCallsToday,
	}, nil
}

func (b *Budget) RemainingAutoCalls() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remainingAutoLocked()
}

func (b *Budget) RemainingRunCalls() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remainingRunLocked()
}

// Reserve atomically claims calls for the current run. It preserves every
// configured reserve and refuses to exceed the per-run ceiling.
func (b *Budget) Reserve(calls int) bool {
	if b == nil || calls <= 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.exhausted || calls > b.remainingRunLocked() {
		return false
	}
	b.reservedCalls += calls
	return true
}

func (b *Budget) MarkExhausted() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.exhausted = true
	b.mu.Unlock()
}

func (b *Budget) IsExhausted() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.exhausted
}

func (b *Budget) Snapshot() Snapshot {
	if b == nil {
		return Snapshot{Exhausted: true}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return Snapshot{
		OfficialDailyLimit:      b.config.OfficialDailyLimit,
		AutoDailyBudget:         b.config.AutoDailyBudget,
		ManualDailyReserve:      b.config.ManualDailyReserve,
		DaumSearchSharedReserve: b.config.DaumSearchSharedReserve,
		EmergencyReserve:        b.config.EmergencyReserve,
		MaxRequestsPerRun:       b.config.MaxRequestsPerRun,
		ObservedCallsToday:      b.observedCalls,
		ReservedCallsThisRun:    b.reservedCalls,
		RemainingAutoCalls:      b.remainingAutoLocked(),
		RemainingRunCalls:       b.remainingRunLocked(),
		Exhausted:               b.exhausted,
	}
}

func (b *Budget) remainingAutoLocked() int {
	if b.exhausted {
		return 0
	}
	reserves := b.config.ManualDailyReserve + b.config.DaumSearchSharedReserve + b.config.EmergencyReserve
	appWideRemaining := b.config.OfficialDailyLimit - b.observedCalls - reserves
	if appWideRemaining > b.config.AutoDailyBudget {
		appWideRemaining = b.config.AutoDailyBudget
	}
	appWideRemaining -= b.reservedCalls
	if appWideRemaining < 0 {
		return 0
	}
	return appWideRemaining
}

func (b *Budget) remainingRunLocked() int {
	autoRemaining := b.remainingAutoLocked()
	runRemaining := b.config.MaxRequestsPerRun - b.reservedCalls
	if runRemaining < 0 {
		runRemaining = 0
	}
	if autoRemaining < runRemaining {
		return autoRemaining
	}
	return runRemaining
}
