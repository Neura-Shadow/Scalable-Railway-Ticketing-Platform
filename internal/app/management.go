package app

import (
	"context"
	"errors"
	"strings"
	"time"

	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	offeringpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type managementOffering interface {
	CreateStation(context.Context, offeringpostgres.CreateStationParams) (offeringpostgres.Station, error)
	CreateRoute(context.Context, offeringpostgres.CreateRouteParams) (offeringpostgres.Route, error)
	CreateTrain(context.Context, offeringpostgres.CreateTrainParams) (offeringpostgres.Train, error)
	CreateCoach(context.Context, offeringpostgres.CreateCoachParams) (offeringpostgres.Coach, error)
	CreateSeat(context.Context, offeringpostgres.CreateSeatParams) (offeringpostgres.Seat, error)
	CreateFare(context.Context, offeringpostgres.CreateFareParams) (offeringpostgres.Fare, error)
	CommissionTrainRun(context.Context, offeringpostgres.CommissionTrainRunParams) (offeringpostgres.TrainRun, error)
	UpdateTrainRunStatus(context.Context, string, offeringdomain.TrainRunStatus) (offeringpostgres.TrainRun, error)
}
type AdminCommands struct{ store managementOffering }

func NewAdminCommands(store managementOffering) *AdminCommands { return &AdminCommands{store: store} }
func (a *AdminCommands) ExecuteAdmin(ctx context.Context, c httpapi.AdminCommand) (httpapi.ResourceView, error) {
	if a == nil || a.store == nil {
		return httpapi.ResourceView{}, httpapi.ErrUnavailable
	}
	if strings.TrimSpace(c.ActorID) == "" {
		return httpapi.ResourceView{}, httpapi.ErrForbidden
	}
	var id string
	switch c.Action {
	case httpapi.AdminCreateStation:
		if c.Station == nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		v, err := a.store.CreateStation(ctx, offeringpostgres.CreateStationParams{Code: c.Station.Code, Name: c.Station.Name, Timezone: c.Station.Timezone})
		if err != nil {
			return httpapi.ResourceView{}, mapOfferingError(err)
		}
		id = v.ID
	case httpapi.AdminCreateRoute:
		if c.Route == nil || len(c.Route.Stops) < 2 {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		stops := make([]offeringdomain.RouteStop, 0, len(c.Route.Stops))
		for _, input := range c.Route.Stops {
			stationCode, err := offeringdomain.NewStationCode(input.StationCode)
			if err != nil {
				return httpapi.ResourceView{}, httpapi.ErrInvalidInput
			}
			stop, err := offeringdomain.NewRouteStop(stationCode, input.StopIndex, input.ArrivalOffsetMinutes, input.DepartureOffsetMinutes)
			if err != nil {
				return httpapi.ResourceView{}, httpapi.ErrInvalidInput
			}
			stops = append(stops, stop)
		}
		route, err := offeringdomain.NewRoute(c.Route.Code, c.Route.Name, stops)
		if err != nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		v, err := a.store.CreateRoute(ctx, offeringpostgres.CreateRouteParams{Route: route, OperatingTimezone: c.Route.OperatingTimezone})
		if err != nil {
			return httpapi.ResourceView{}, mapOfferingError(err)
		}
		id = v.ID
	case httpapi.AdminCreateTrain:
		if c.Train == nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		v, err := a.store.CreateTrain(ctx, offeringpostgres.CreateTrainParams{Code: c.Train.Code, Name: c.Train.Code})
		if err != nil {
			return httpapi.ResourceView{}, mapOfferingError(err)
		}
		id = v.ID
	case httpapi.AdminCreateCoach:
		if c.Coach == nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		class, err := offeringdomain.ParseSeatClass(c.Coach.SeatClass)
		if err != nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		v, err := a.store.CreateCoach(ctx, offeringpostgres.CreateCoachParams{TrainID: c.Coach.TrainID, CoachNumber: c.Coach.Code, SeatClass: class})
		if err != nil {
			return httpapi.ResourceView{}, mapOfferingError(err)
		}
		id = v.ID
	case httpapi.AdminCreateSeat:
		if c.Seat == nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		v, err := a.store.CreateSeat(ctx, offeringpostgres.CreateSeatParams{CoachID: c.Seat.CoachID, SeatNumber: c.Seat.Number, SeatType: "other"})
		if err != nil {
			return httpapi.ResourceView{}, mapOfferingError(err)
		}
		id = v.ID
	case httpapi.AdminCreateFare:
		if c.Fare == nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		class, err := offeringdomain.ParseSeatClass(c.Fare.SeatClass)
		if err != nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		v, err := a.store.CreateFare(ctx, offeringpostgres.CreateFareParams{TrainRunID: c.Fare.TrainRunID, FromStopIndex: c.Fare.FromStopIndex, ToStopIndex: c.Fare.ToStopIndex, SeatClass: class, AmountMinor: c.Fare.AmountMinor, Currency: c.Fare.Currency})
		if err != nil {
			return httpapi.ResourceView{}, mapOfferingError(err)
		}
		id = v.ID
	default:
		return httpapi.ResourceView{}, httpapi.ErrInvalidInput
	}
	return httpapi.ResourceView{ID: id}, nil
}

type inventoryInitializer interface {
	InitializeInventory(context.Context, uuid.UUID) (int64, error)
}
type OperatorCommands struct {
	offering  managementOffering
	inventory inventoryInitializer
	metrics   interface {
		RecordOutbox(operation, eventType, result, reason string)
	}
}

func NewOperatorCommands(offering managementOffering, inventory inventoryInitializer, metrics ...interface {
	RecordOutbox(operation, eventType, result, reason string)
}) *OperatorCommands {
	commands := &OperatorCommands{offering: offering, inventory: inventory}
	if len(metrics) > 0 {
		commands.metrics = metrics[0]
	}
	return commands
}
func (o *OperatorCommands) ExecuteOperator(ctx context.Context, c httpapi.OperatorCommand) (httpapi.ResourceView, error) {
	if o == nil || o.offering == nil {
		return httpapi.ResourceView{}, httpapi.ErrUnavailable
	}
	if strings.TrimSpace(c.ActorID) == "" {
		return httpapi.ResourceView{}, httpapi.ErrForbidden
	}
	switch c.Action {
	case httpapi.OperatorCreateTrainRun:
		if c.TrainRun == nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		date, err := time.Parse(time.DateOnly, c.TrainRun.ServiceDate)
		if err != nil || c.TrainRun.DepartureAt.IsZero() {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		v, err := o.offering.CommissionTrainRun(ctx, offeringpostgres.CommissionTrainRunParams{TrainID: c.TrainRun.TrainID, RouteID: c.TrainRun.RouteID, ServiceDate: date, ScheduledDepartureAt: c.TrainRun.DepartureAt})
		if err != nil {
			return httpapi.ResourceView{}, mapOfferingError(err)
		}
		return httpapi.ResourceView{ID: v.ID}, nil
	case httpapi.OperatorInitializeInventory:
		if o.inventory == nil {
			return httpapi.ResourceView{}, httpapi.ErrUnavailable
		}
		id, err := uuid.Parse(c.TrainRunID)
		if err != nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		if _, err = o.inventory.InitializeInventory(ctx, id); err != nil {
			return httpapi.ResourceView{}, mapBookingError(err)
		}
		return httpapi.ResourceView{ID: id.String()}, nil
	case httpapi.OperatorUpdateTrainRunStatus:
		if c.Status == nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		status, err := offeringdomain.ParseTrainRunStatus(c.Status.Status)
		if err != nil {
			return httpapi.ResourceView{}, httpapi.ErrInvalidInput
		}
		v, err := o.offering.UpdateTrainRunStatus(ctx, c.TrainRunID, status)
		if err != nil {
			return httpapi.ResourceView{}, mapOfferingError(err)
		}
		if v.OutboxCreated && o.metrics != nil {
			o.metrics.RecordOutbox("create", "trainrun.cancelled", "success", "none")
		}
		return httpapi.ResourceView{ID: v.ID}, nil
	default:
		return httpapi.ResourceView{}, httpapi.ErrInvalidInput
	}
}
func mapOfferingError(err error) error {
	switch {
	case errors.Is(err, offeringpostgres.ErrInvalidInput), errors.Is(err, offeringdomain.ErrInvalidSeatClass), errors.Is(err, offeringdomain.ErrInvalidTrainRunStatus):
		return httpapi.ErrInvalidInput
	case errors.Is(err, offeringpostgres.ErrNotFound):
		return httpapi.ErrNotFound
	case errors.Is(err, offeringpostgres.ErrConflict):
		return httpapi.ErrConflict
	case errors.Is(err, offeringdomain.ErrInvalidTrainRunTransition):
		return httpapi.ErrConflict
	default:
		return httpapi.ErrUnavailable
	}
}

var (
	_ httpapi.AdminCommands    = (*AdminCommands)(nil)
	_ httpapi.OperatorCommands = (*OperatorCommands)(nil)
)
