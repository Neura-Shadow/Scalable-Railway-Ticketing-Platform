package httpapi

import (
	"context"
	"time"
)

type Role string

const (
	RoleCustomer Role = "customer"
	RoleAdmin    Role = "admin"
	RoleOperator Role = "operator"
)

type Identity struct {
	Subject string
	Role    Role
}

type BearerTokenParser interface {
	ParseAccessToken(ctx context.Context, raw string) (Identity, error)
}

type HTTPMetrics interface {
	ObserveHTTP(method, path string, status int, duration time.Duration)
}

type CreateReservationCommand struct {
	OwnerID                string
	IdempotencyKey         string
	TrainRunID             string
	OriginStationCode      string
	DestinationStationCode string
	SeatClass              string
	PassengerIDs           []string
}

type ReservationView struct {
	ID                     string     `json:"id"`
	Status                 string     `json:"status"`
	TrainRunID             string     `json:"train_run_id,omitempty"`
	OriginStationCode      string     `json:"origin_station_code,omitempty"`
	DestinationStationCode string     `json:"destination_station_code,omitempty"`
	SeatClass              string     `json:"seat_class,omitempty"`
	PassengerIDs           []string   `json:"passenger_ids,omitempty"`
	ExpiresAt              *time.Time `json:"expires_at,omitempty"`
}

type ReservationMutationCommand struct {
	OwnerID        string
	ReservationID  string
	IdempotencyKey string
}

type ReservationService interface {
	CreateHold(ctx context.Context, command CreateReservationCommand) (ReservationView, error)
	GetReservation(ctx context.Context, ownerID, reservationID string) (ReservationView, error)
	ConfirmReservation(ctx context.Context, command ReservationMutationCommand) (ReservationView, error)
	CancelReservation(ctx context.Context, command ReservationMutationCommand) (ReservationView, error)
}

type PageRequest struct {
	Page  int
	Limit int
	Sort  string
}

type StationView struct {
	ID       string `json:"id"`
	Code     string `json:"code"`
	Name     string `json:"name"`
	Timezone string `json:"timezone"`
}

type StationPage struct {
	Items []StationView `json:"items"`
	Page  int           `json:"page"`
	Limit int           `json:"limit"`
	Total int64         `json:"total"`
}

type TrainRunSearch struct {
	OriginStationCode      string
	DestinationStationCode string
	ServiceDate            time.Time
	SeatClass              string
	Page                   PageRequest
}

type TrainRunView struct {
	ID                     string    `json:"id"`
	TrainCode              string    `json:"train_code"`
	OriginStationCode      string    `json:"origin_station_code"`
	DestinationStationCode string    `json:"destination_station_code"`
	DepartureAt            time.Time `json:"departure_at"`
	ArrivalAt              time.Time `json:"arrival_at"`
	SeatClass              string    `json:"seat_class,omitempty"`
	AvailableSeatCount     int       `json:"available_seat_count,omitempty"`
	FareMinor              int64     `json:"fare_minor,omitempty"`
	Currency               string    `json:"currency,omitempty"`
}

type TrainRunPage struct {
	Items []TrainRunView `json:"items"`
	Page  int            `json:"page"`
	Limit int            `json:"limit"`
	Total int64          `json:"total"`
}

type AvailabilityQuery struct {
	TrainRunID             string
	OriginStationCode      string
	DestinationStationCode string
	SeatClass              string
}

type AvailabilityView struct {
	TrainRunID             string    `json:"train_run_id"`
	TrainCode              string    `json:"train_code"`
	OriginStationCode      string    `json:"origin"`
	DestinationStationCode string    `json:"destination"`
	DepartureAt            time.Time `json:"departure_at"`
	ArrivalAt              time.Time `json:"arrival_at"`
	SeatClass              string    `json:"seat_class"`
	AvailableSeatCount     int       `json:"available_seat_count"`
	FareMinor              int64     `json:"fare_minor"`
	Currency               string    `json:"currency"`
}

type OfferingQueries interface {
	ListStations(ctx context.Context, page PageRequest) (StationPage, error)
	SearchTrainRuns(ctx context.Context, search TrainRunSearch) (TrainRunPage, error)
	GetAvailability(ctx context.Context, query AvailabilityQuery) (AvailabilityView, error)
}

type RegisterCommand struct {
	Email       string
	Password    string
	DisplayName string
}

type LoginCommand struct {
	Email    string
	Password string
}

type TokenPairView struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
}

type AuthService interface {
	Register(ctx context.Context, command RegisterCommand) (TokenPairView, error)
	Login(ctx context.Context, command LoginCommand) (TokenPairView, error)
	Refresh(ctx context.Context, refreshToken string) (TokenPairView, error)
	Logout(ctx context.Context, subject, refreshToken string) error
}

type RateLimitScope string

const (
	RateLimitRegister          RateLimitScope = "auth_register"
	RateLimitLogin             RateLimitScope = "auth_login"
	RateLimitReservationCreate RateLimitScope = "reservation_create"
)

type RateLimitRequest struct {
	Scope RateLimitScope
	Key   string
}

type RateLimiter interface {
	Allow(ctx context.Context, request RateLimitRequest) (bool, error)
}

type PassengerView struct {
	ID          string    `json:"id"`
	DisplayName string    `json:"display_name"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type PassengerPage struct {
	Items []PassengerView `json:"items"`
	Page  int             `json:"page"`
	Limit int             `json:"limit"`
	Total int64           `json:"total"`
}

type PassengerService interface {
	ListPassengers(ctx context.Context, ownerID string, page PageRequest) (PassengerPage, error)
	CreatePassenger(ctx context.Context, ownerID, displayName string) (PassengerView, error)
	GetPassenger(ctx context.Context, ownerID, passengerID string) (PassengerView, error)
	UpdatePassenger(ctx context.Context, ownerID, passengerID, displayName string) (PassengerView, error)
	DeletePassenger(ctx context.Context, ownerID, passengerID string) error
}

type TicketView struct {
	ID          string `json:"id"`
	TicketCode  string `json:"ticket_code"`
	PassengerID string `json:"passenger_id"`
	SeatID      string `json:"seat_id"`
	Status      string `json:"status"`
}

type TicketOrderView struct {
	ID               string       `json:"id"`
	ReservationID    string       `json:"reservation_id"`
	Status           string       `json:"status"`
	TotalAmountMinor int64        `json:"total_amount_minor"`
	Currency         string       `json:"currency"`
	Tickets          []TicketView `json:"tickets,omitempty"`
	CreatedAt        time.Time    `json:"created_at"`
}

type TicketOrderPage struct {
	Items []TicketOrderView `json:"items"`
	Page  int               `json:"page"`
	Limit int               `json:"limit"`
	Total int64             `json:"total"`
}

type TicketQueries interface {
	ListTicketOrders(ctx context.Context, ownerID string, page PageRequest) (TicketOrderPage, error)
	GetTicketOrder(ctx context.Context, ownerID, ticketOrderID string) (TicketOrderView, error)
}

type ResourceView struct {
	ID string `json:"id"`
}

type StationWrite struct {
	Code     string `json:"code"`
	Name     string `json:"name"`
	Timezone string `json:"timezone"`
}

type RouteWrite struct {
	Code       string   `json:"code"`
	StationIDs []string `json:"station_ids"`
}

type TrainWrite struct {
	Code string `json:"code"`
}

type CoachWrite struct {
	TrainID   string `json:"train_id"`
	Code      string `json:"code"`
	SeatClass string `json:"seat_class"`
}

type SeatWrite struct {
	CoachID string `json:"coach_id"`
	Number  string `json:"number"`
}

type FareWrite struct {
	TrainRunID    string `json:"train_run_id"`
	FromStopIndex int    `json:"from_stop_index"`
	ToStopIndex   int    `json:"to_stop_index"`
	SeatClass     string `json:"seat_class"`
	AmountMinor   int64  `json:"amount_minor"`
	Currency      string `json:"currency"`
}

type AdminAction string

const (
	AdminCreateStation AdminAction = "create_station"
	AdminCreateRoute   AdminAction = "create_route"
	AdminCreateTrain   AdminAction = "create_train"
	AdminCreateCoach   AdminAction = "create_coach"
	AdminCreateSeat    AdminAction = "create_seat"
	AdminCreateFare    AdminAction = "create_fare"
)

type AdminCommand struct {
	ActorID string
	Action  AdminAction
	Station *StationWrite
	Route   *RouteWrite
	Train   *TrainWrite
	Coach   *CoachWrite
	Seat    *SeatWrite
	Fare    *FareWrite
}

type AdminCommands interface {
	ExecuteAdmin(ctx context.Context, command AdminCommand) (ResourceView, error)
}

type TrainRunWrite struct {
	TrainID     string    `json:"train_id"`
	RouteID     string    `json:"route_id"`
	ServiceDate string    `json:"service_date"`
	DepartureAt time.Time `json:"departure_at"`
}

type TrainRunStatusWrite struct {
	Status string `json:"status"`
}

type OperatorAction string

const (
	OperatorCreateTrainRun       OperatorAction = "create_train_run"
	OperatorInitializeInventory  OperatorAction = "initialize_inventory"
	OperatorUpdateTrainRunStatus OperatorAction = "update_train_run_status"
)

type OperatorCommand struct {
	ActorID    string
	Action     OperatorAction
	TrainRunID string
	TrainRun   *TrainRunWrite
	Status     *TrainRunStatusWrite
}

type OperatorCommands interface {
	ExecuteOperator(ctx context.Context, command OperatorCommand) (ResourceView, error)
}

// ReadinessCheck reports only bounded component health, never an underlying
// error or connection string.
type ReadinessCheck struct {
	Name  string
	Ready bool
}

// ReadinessChecker checks external dependencies using the supplied bounded
// context.
type ReadinessChecker interface {
	CheckReadiness(ctx context.Context) ([]ReadinessCheck, error)
}
