package app

import (
	"context"
	"testing"
	"time"

	offeringdomain "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/domain"
	offeringpostgres "github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/offering/postgres"
	"github.com/Neura-Shadow/Scalable-Railway-Ticketing-Platform/internal/transport/httpapi"
	"github.com/google/uuid"
)

type managementOfferingFake struct {
	station offeringpostgres.CreateStationParams
	route   offeringpostgres.CreateRouteParams
	train   offeringpostgres.CreateTrainParams
	coach   offeringpostgres.CreateCoachParams
	seat    offeringpostgres.CreateSeatParams
	fare    offeringpostgres.CreateFareParams
	run     offeringpostgres.CommissionTrainRunParams
	status  string
}

func (f *managementOfferingFake) CreateStation(_ context.Context, p offeringpostgres.CreateStationParams) (offeringpostgres.Station, error) {
	f.station = p
	return offeringpostgres.Station{ID: "station"}, nil
}
func (f *managementOfferingFake) CreateRoute(_ context.Context, p offeringpostgres.CreateRouteParams) (offeringpostgres.Route, error) {
	f.route = p
	return offeringpostgres.Route{ID: "route"}, nil
}
func (f *managementOfferingFake) CreateTrain(_ context.Context, p offeringpostgres.CreateTrainParams) (offeringpostgres.Train, error) {
	f.train = p
	return offeringpostgres.Train{ID: "train"}, nil
}
func (f *managementOfferingFake) CreateCoach(_ context.Context, p offeringpostgres.CreateCoachParams) (offeringpostgres.Coach, error) {
	f.coach = p
	return offeringpostgres.Coach{ID: "coach"}, nil
}
func (f *managementOfferingFake) CreateSeat(_ context.Context, p offeringpostgres.CreateSeatParams) (offeringpostgres.Seat, error) {
	f.seat = p
	return offeringpostgres.Seat{ID: "seat"}, nil
}
func (f *managementOfferingFake) CreateFare(_ context.Context, p offeringpostgres.CreateFareParams) (offeringpostgres.Fare, error) {
	f.fare = p
	return offeringpostgres.Fare{ID: "fare"}, nil
}
func (f *managementOfferingFake) CommissionTrainRun(_ context.Context, p offeringpostgres.CommissionTrainRunParams) (offeringpostgres.TrainRun, error) {
	f.run = p
	return offeringpostgres.TrainRun{ID: "run"}, nil
}
func (f *managementOfferingFake) UpdateTrainRunStatus(_ context.Context, id string, status offeringdomain.TrainRunStatus) (offeringpostgres.TrainRun, error) {
	created := status == offeringdomain.TrainRunStatusCancelled && f.status != offeringdomain.TrainRunStatusCancelled.String()
	f.status = status.String()
	return offeringpostgres.TrainRun{ID: id, OutboxCreated: created}, nil
}

type outboxMetricsSpy struct{ created int }

func (s *outboxMetricsSpy) RecordOutbox(operation, eventType, result, reason string) {
	if operation == "create" && eventType == "trainrun.cancelled" && result == "success" && reason == "none" {
		s.created++
	}
}

type inventoryFake struct{ run uuid.UUID }

func (f *inventoryFake) InitializeInventory(_ context.Context, id uuid.UUID) (int64, error) {
	f.run = id
	return 12, nil
}

func TestAdminCommandsDispatchToOfferingStoreWithDocumentedDefaults(t *testing.T) {
	store := &managementOfferingFake{}
	admin := NewAdminCommands(store)
	if _, err := admin.ExecuteAdmin(context.Background(), httpapi.AdminCommand{ActorID: "admin", Action: httpapi.AdminCreateTrain, Train: &httpapi.TrainWrite{Code: "R100"}}); err != nil {
		t.Fatal(err)
	}
	if store.train.Code != "R100" || store.train.Name != "R100" {
		t.Fatalf("train defaults = %#v", store.train)
	}
	if _, err := admin.ExecuteAdmin(context.Background(), httpapi.AdminCommand{ActorID: "admin", Action: httpapi.AdminCreateSeat, Seat: &httpapi.SeatWrite{CoachID: "coach", Number: "1A"}}); err != nil {
		t.Fatal(err)
	}
	if store.seat.SeatType != "other" {
		t.Fatalf("seat default = %#v", store.seat)
	}
}

func TestAdminRouteBuildsValidatedDomainRouteAndPersistsIt(t *testing.T) {
	store := &managementOfferingFake{}
	view, err := NewAdminCommands(store).ExecuteAdmin(context.Background(), httpapi.AdminCommand{ActorID: "admin", Action: httpapi.AdminCreateRoute, Route: &httpapi.RouteWrite{
		Code: "west", Name: "Western Line", OperatingTimezone: "Asia/Taipei",
		Stops: []httpapi.RouteStopWrite{
			{StationCode: "tpe", StopIndex: 0, ArrivalOffsetMinutes: 0, DepartureOffsetMinutes: 5},
			{StationCode: "khh", StopIndex: 1, ArrivalOffsetMinutes: 120, DepartureOffsetMinutes: 125},
		},
	}})
	if err != nil || view.ID != "route" {
		t.Fatalf("view=%#v error=%v", view, err)
	}
	if store.route.Route.Code() != "WEST" || store.route.Route.Name() != "Western Line" || store.route.Route.SegmentCount() != 1 || store.route.OperatingTimezone != "Asia/Taipei" {
		t.Fatalf("route params = %#v", store.route)
	}
}

func TestOperatorCommandsCommissionInitializeAndTransitionThroughStores(t *testing.T) {
	offering, inventory := &managementOfferingFake{}, &inventoryFake{}
	operator := NewOperatorCommands(offering, inventory)
	departure := time.Date(2026, 7, 20, 8, 0, 0, 0, time.FixedZone("TST", 8*60*60))
	view, err := operator.ExecuteOperator(context.Background(), httpapi.OperatorCommand{ActorID: "operator", Action: httpapi.OperatorCreateTrainRun, TrainRun: &httpapi.TrainRunWrite{TrainID: "train", RouteID: "route", ServiceDate: "2026-07-20", DepartureAt: departure}})
	if err != nil || view.ID != "run" || offering.run.ServiceDate.Format(time.DateOnly) != "2026-07-20" || offering.run.ScheduledDepartureAt != departure {
		t.Fatalf("view=%#v input=%#v err=%v", view, offering.run, err)
	}
	runID := uuid.New()
	_, err = operator.ExecuteOperator(context.Background(), httpapi.OperatorCommand{ActorID: "operator", Action: httpapi.OperatorInitializeInventory, TrainRunID: runID.String()})
	if err != nil || inventory.run != runID {
		t.Fatalf("inventory=%v err=%v", inventory.run, err)
	}
	_, err = operator.ExecuteOperator(context.Background(), httpapi.OperatorCommand{ActorID: "operator", Action: httpapi.OperatorUpdateTrainRunStatus, TrainRunID: runID.String(), Status: &httpapi.TrainRunStatusWrite{Status: "boarding"}})
	if err != nil || offering.status != "boarding" {
		t.Fatalf("status=%q err=%v", offering.status, err)
	}
}

func TestOperatorCancellationRecordsCommittedOutboxCreationOnce(t *testing.T) {
	offering, metrics := &managementOfferingFake{}, &outboxMetricsSpy{}
	operator := NewOperatorCommands(offering, &inventoryFake{}, metrics)
	command := httpapi.OperatorCommand{ActorID: "operator", Action: httpapi.OperatorUpdateTrainRunStatus, TrainRunID: uuid.NewString(), Status: &httpapi.TrainRunStatusWrite{Status: "cancelled"}}
	if _, err := operator.ExecuteOperator(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if _, err := operator.ExecuteOperator(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if metrics.created != 1 {
		t.Fatalf("train-run cancellation create metrics = %d, want 1", metrics.created)
	}
}
