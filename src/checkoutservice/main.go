// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	nrlogrusplugin "github.com/newrelic/go-agent/v3/integrations/logcontext/nrlogrusplugin"
	nrgrpc "github.com/newrelic/go-agent/v3/integrations/nrgrpc"
	newrelic "github.com/newrelic/go-agent/v3/newrelic"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice/genproto"
	money "github.com/GoogleCloudPlatform/microservices-demo/src/checkoutservice/money"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	listenPort  = "5050"
	usdCurrency = "USD"
)

var log *logrus.Logger

func init() {
	log = logrus.New()
	log.SetFormatter(nrlogrusplugin.ContextFormatter{})
	log.SetLevel(logrus.TraceLevel)
	log.Out = os.Stdout
}

type checkoutService struct {
	productCatalogSvcAddr string
	cartSvcAddr           string
	currencySvcAddr       string
	shippingSvcAddr       string
	emailSvcAddr          string
	paymentSvcAddr        string
}

func main() {
	app, _ := newrelic.NewApplication(
		newrelic.ConfigAppName(os.Getenv("NEW_RELIC_APP_NAME")),
		newrelic.ConfigLicense(os.Getenv("NEW_RELIC_LICENSE_KEY")),
		func(config *newrelic.Config) {
			// add more specific configuration of the agent within a custom ConfigOption
			config.DistributedTracer.Enabled = true
		},
	)

	port := listenPort
	if os.Getenv("PORT") != "" {
		port = os.Getenv("PORT")
	}

	svc := new(checkoutService)
	mustMapEnv(&svc.shippingSvcAddr, "SHIPPING_SERVICE_ADDR")
	mustMapEnv(&svc.productCatalogSvcAddr, "PRODUCT_CATALOG_SERVICE_ADDR")
	mustMapEnv(&svc.cartSvcAddr, "CART_SERVICE_ADDR")
	mustMapEnv(&svc.currencySvcAddr, "CURRENCY_SERVICE_ADDR")
	mustMapEnv(&svc.emailSvcAddr, "EMAIL_SERVICE_ADDR")
	mustMapEnv(&svc.paymentSvcAddr, "PAYMENT_SERVICE_ADDR")

	log.Infof("service config: %+v", svc)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(err)
	}

	var srv *grpc.Server
	nrgrpc.Configure(
		nrgrpc.WithStatusHandler(codes.DeadlineExceeded, nrgrpc.ErrorInterceptorStatusHandler),
		nrgrpc.WithStatusHandler(codes.NotFound, nrgrpc.WarningInterceptorStatusHandler))
	
	srv = grpc.NewServer(
		grpc.UnaryInterceptor(nrgrpc.UnaryServerInterceptor(app)),
		grpc.StreamInterceptor(nrgrpc.StreamServerInterceptor(app)))

	pb.RegisterCheckoutServiceServer(srv, svc)
	healthpb.RegisterHealthServer(srv, svc)
	log.Infof("starting to listen on tcp: %q", lis.Addr().String())
	err = srv.Serve(lis)
	log.Fatal(err)
}

func mustMapEnv(target *string, envKey string) {
	v := os.Getenv(envKey)
	if v == "" {
		panic(fmt.Sprintf("environment variable %q not set", envKey))
	}
	*target = v
}

func (cs *checkoutService) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (cs *checkoutService) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "health check via Watch not implemented")
}

func (cs *checkoutService) PlaceOrder(ctx context.Context, req *pb.PlaceOrderRequest) (*pb.PlaceOrderResponse, error) {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("PlaceOrder").End()
	log.Infof("[PlaceOrder] user_id=%q user_currency=%q", req.UserId, req.UserCurrency)

	txn.AddAttribute("email", req.Email)

	orderID, err := uuid.NewUUID()
	txn.Application().RecordCustomEvent(
		"BuyingFlow", map[string]interface{}{
			"email":  req.Email,
			"status": false,
			"step":   "Create Order ID"})
	txn.AddAttribute("status", "failed to generate order uuid")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate order uuid")
	}
	txn.Application().RecordCustomEvent(
		"BuyingFlow", map[string]interface{}{
			"email":   req.Email,
			"orderId": orderID.String(),
			"status":  true,
			"step":    "Create Order ID"})

	prep, err := cs.prepareOrderItemsAndShippingQuoteFromCart(ctx, req.UserId, req.UserCurrency, req.Address)
	if err != nil {
		txn.Application().RecordCustomEvent(
			"BuyingFlow", map[string]interface{}{
				"email":   req.Email,
				"orderId": orderID.String(),
				"status":  false,
				"step":    "Prepare Order Items"})
		txn.AddAttribute("status", "failed to prepare order")
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	txn.Application().RecordCustomEvent(
		"BuyingFlow", map[string]interface{}{
			"email":   req.Email,
			"orderId": orderID.String(),
			"status":  true,
			"step":    "Prepare Order Items"})

	total := pb.Money{CurrencyCode: req.UserCurrency,
		Units: 0,
		Nanos: 0}
	total = money.Must(money.Sum(total, *prep.shippingCostLocalized))
	for _, it := range prep.orderItems {
		multPrice := money.MultiplySlow(*it.Cost, uint32(it.GetItem().GetQuantity()))
		total = money.Must(money.Sum(total, multPrice))
	}

	txID, err := cs.chargeCard(ctx, &total, req.CreditCard)
	if err != nil {
		txn.Application().RecordCustomEvent(
			"BuyingFlow", map[string]interface{}{
				"email":   req.Email,
				"orderId": orderID.String(),
				"status":  false,
				"step":    "Charge Credit Card"})
		txn.AddAttribute("status", "failed to charge card")
		return nil, status.Errorf(codes.Internal, "failed to charge card: %+v", err)
	}
	txn.Application().RecordCustomEvent(
		"BuyingFlow", map[string]interface{}{
			"email":     req.Email,
			"orderId":   orderID.String(),
			"paymentId": txID,
			"status":    true,
			"step":      "Charge Credit Card"})

	txn.AddAttribute("status", "payment went through")
	txn.AddAttribute("paymentId", txID)
	log.Infof("payment went through (transaction_id: %s)", txID)

	shippingTrackingID, err := cs.shipOrder(ctx, req.Address, prep.cartItems)
	if err != nil {
		txn.Application().RecordCustomEvent(
			"BuyingFlow", map[string]interface{}{
				"email":     req.Email,
				"orderId":   orderID.String(),
				"paymentId": txID,
				"status":    false,
				"step":      "Process Shipment"})
		txn.AddAttribute("status", "shipping error")
		return nil, status.Errorf(codes.Unavailable, "shipping error: %+v", err)
	}
	txn.Application().RecordCustomEvent(
		"BuyingFlow", map[string]interface{}{
			"email":     req.Email,
			"orderId":   orderID.String(),
			"paymentId": txID,
			"status":    true,
			"step":      "Process Shipment"})

	_ = cs.emptyUserCart(ctx, req.UserId)

	orderResult := &pb.OrderResult{
		OrderId:            orderID.String(),
		ShippingTrackingId: shippingTrackingID,
		ShippingCost:       prep.shippingCostLocalized,
		ShippingAddress:    req.Address,
		Items:              prep.orderItems,
	}

	if err := cs.sendOrderConfirmation(ctx, req.Email, orderResult); err != nil {
		txn.Application().RecordCustomEvent(
			"BuyingFlow", map[string]interface{}{
				"email":     req.Email,
				"orderId":   orderID.String(),
				"paymentId": txID,
				"status":    false,
				"step":      "Send Confirmation"})
		txn.AddAttribute("feedback", "failed to send order confirmation")
		log.Warnf("failed to send order confirmation to %q: %+v", req.Email, err)
	} else {
		txn.Application().RecordCustomEvent(
			"BuyingFlow", map[string]interface{}{
				"email":     req.Email,
				"orderId":   orderID.String(),
				"paymentId": txID,
				"status":    true,
				"step":      "Send Confirmation"})
		txn.AddAttribute("feedback", "order confirmation email sent")
		log.Infof("order confirmation email sent to %q", req.Email)
	}
	resp := &pb.PlaceOrderResponse{Order: orderResult}
	return resp, nil
}

type orderPrep struct {
	orderItems            []*pb.OrderItem
	cartItems             []*pb.CartItem
	shippingCostLocalized *pb.Money
}

func (cs *checkoutService) prepareOrderItemsAndShippingQuoteFromCart(ctx context.Context, userID, userCurrency string, address *pb.Address) (orderPrep, error) {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("prepareOrderItemsAndShippingQuoteFromCart").End()

	var out orderPrep
	cartItems, err := cs.getUserCart(ctx, userID)
	if err != nil {
		return out, fmt.Errorf("cart failure: %+v", err)
	}
	orderItems, err := cs.prepOrderItems(ctx, cartItems, userCurrency)
	if err != nil {
		return out, fmt.Errorf("failed to prepare order: %+v", err)
	}
	shippingUSD, err := cs.quoteShipping(ctx, address, cartItems)
	if err != nil {
		return out, fmt.Errorf("shipping quote failure: %+v", err)
	}
	shippingPrice, err := cs.convertCurrency(ctx, shippingUSD, userCurrency)
	if err != nil {
		return out, fmt.Errorf("failed to convert shipping cost to currency: %+v", err)
	}

	out.shippingCostLocalized = shippingPrice
	out.cartItems = cartItems
	out.orderItems = orderItems
	return out, nil
}

func (cs *checkoutService) quoteShipping(ctx context.Context, address *pb.Address, items []*pb.CartItem) (*pb.Money, error) {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("quoteShipping").End()

	conn, err := grpc.DialContext(ctx, cs.shippingSvcAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(nrgrpc.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(nrgrpc.StreamClientInterceptor),
	)
	if err != nil {
		return nil, fmt.Errorf("could not connect shipping service: %+v", err)
	}
	defer conn.Close()

	shippingQuote, err := pb.NewShippingServiceClient(conn).
		GetQuote(ctx, &pb.GetQuoteRequest{
			Address: address,
			Items:   items})
	if err != nil {
		return nil, fmt.Errorf("failed to get shipping quote: %+v", err)
	}
	return shippingQuote.GetCostUsd(), nil
}

func (cs *checkoutService) getUserCart(ctx context.Context, userID string) ([]*pb.CartItem, error) {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("getUserCart").End()

	conn, err := grpc.DialContext(ctx, cs.cartSvcAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(nrgrpc.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(nrgrpc.StreamClientInterceptor),
	)
	if err != nil {
		return nil, fmt.Errorf("could not connect cart service: %+v", err)
	}
	defer conn.Close()

	cart, err := pb.NewCartServiceClient(conn).GetCart(ctx, &pb.GetCartRequest{UserId: userID})
	if err != nil {
		return nil, fmt.Errorf("failed to get user cart during checkout: %+v", err)
	}
	return cart.GetItems(), nil
}

func (cs *checkoutService) emptyUserCart(ctx context.Context, userID string) error {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("emptyUserCart").End()

	conn, err := grpc.DialContext(ctx, cs.cartSvcAddr, 
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(nrgrpc.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(nrgrpc.StreamClientInterceptor),
	)
	if err != nil {
		return fmt.Errorf("could not connect cart service: %+v", err)
	}
	defer conn.Close()

	if _, err = pb.NewCartServiceClient(conn).EmptyCart(ctx, &pb.EmptyCartRequest{UserId: userID}); err != nil {
		
		return fmt.Errorf("failed to empty user cart during checkout: %+v", err)
	}
	return nil
}

func (cs *checkoutService) prepOrderItems(ctx context.Context, items []*pb.CartItem, userCurrency string) ([]*pb.OrderItem, error) {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("prepOrderItems").End()
	out := make([]*pb.OrderItem, len(items))

	conn, err := grpc.DialContext(ctx, cs.productCatalogSvcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(nrgrpc.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(nrgrpc.StreamClientInterceptor),
	)
	if err != nil {
		return nil, fmt.Errorf("could not connect product catalog service: %+v", err)
	}
	defer conn.Close()
	cl := pb.NewProductCatalogServiceClient(conn)

	for i, item := range items {
		product, err := cl.GetProduct(ctx, &pb.GetProductRequest{Id: item.GetProductId()})
		if err != nil {
			return nil, fmt.Errorf("failed to get product #%q", item.GetProductId())
		}
		price, err := cs.convertCurrency(ctx, product.GetPriceUsd(), userCurrency)
		if err != nil {
			return nil, fmt.Errorf("failed to convert price of %q to %s", item.GetProductId(), userCurrency)
		}
		out[i] = &pb.OrderItem{
			Item: item,
			Cost: price}
	}
	return out, nil
}

func (cs *checkoutService) convertCurrency(ctx context.Context, from *pb.Money, toCurrency string) (*pb.Money, error) {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("convertCurrency").End()

	conn, err := grpc.DialContext(ctx, cs.currencySvcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(nrgrpc.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(nrgrpc.StreamClientInterceptor),
	)
	if err != nil {
		return nil, fmt.Errorf("could not connect currency service: %+v", err)
	}
	defer conn.Close()
	result, err := pb.NewCurrencyServiceClient(conn).Convert(context.TODO(), &pb.CurrencyConversionRequest{
		From:   from,
		ToCode: toCurrency})
	if err != nil {
		return nil, fmt.Errorf("failed to convert currency: %+v", err)
	}
	return result, err
}

func (cs *checkoutService) chargeCard(ctx context.Context, amount *pb.Money, paymentInfo *pb.CreditCardInfo) (string, error) {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("chargeCard").End()

	conn, err := grpc.DialContext(ctx, cs.paymentSvcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(nrgrpc.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(nrgrpc.StreamClientInterceptor),
	)
	if err != nil {
		return "", fmt.Errorf("failed to connect payment service: %+v", err)
	}
	defer conn.Close()

	paymentResp, err := pb.NewPaymentServiceClient(conn).Charge(ctx, &pb.ChargeRequest{
		Amount:     amount,
		CreditCard: paymentInfo})
	if err != nil {
		return "", fmt.Errorf("could not charge the card: %+v", err)
	}
	return paymentResp.GetTransactionId(), nil
}

func (cs *checkoutService) sendOrderConfirmation(ctx context.Context, email string, order *pb.OrderResult) error {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("sendOrderConfirmation").End()
	conn, err := grpc.DialContext(ctx, cs.emailSvcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(nrgrpc.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(nrgrpc.StreamClientInterceptor),
	)
	if err != nil {
		return fmt.Errorf("failed to connect email service: %+v", err)
	}
	defer conn.Close()
	_, err = pb.NewEmailServiceClient(conn).SendOrderConfirmation(ctx, &pb.SendOrderConfirmationRequest{
		Email: email,
		Order: order})
	return err
}

func (cs *checkoutService) shipOrder(ctx context.Context, address *pb.Address, items []*pb.CartItem) (string, error) {
	txn := newrelic.FromContext(ctx)
	defer txn.StartSegment("shipOrder").End()
	
	conn, err := grpc.DialContext(ctx, cs.shippingSvcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(nrgrpc.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(nrgrpc.StreamClientInterceptor),
	)
	if err != nil {
		return "", fmt.Errorf("failed to connect email service: %+v", err)
	}
	defer conn.Close()
	resp, err := pb.NewShippingServiceClient(conn).ShipOrder(ctx, &pb.ShipOrderRequest{
		Address: address,
		Items:   items})
	if err != nil {
		return "", fmt.Errorf("shipment failed: %+v", err)
	}
	return resp.GetTrackingId(), nil
}

// TODO: Dial and create client once, reuse.
