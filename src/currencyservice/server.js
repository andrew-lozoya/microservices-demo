/*
 * Copyright 2018 Google LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const newrelic = require('newrelic');
const nrPino = require('@newrelic/pino-enricher');

const path = require('path');
const grpc = require('@grpc/grpc-js');
const pino = require('pino');
const protoLoader = require('@grpc/proto-loader');

const MAIN_PROTO_PATH = path.join(__dirname, './proto/demo.proto');
const HEALTH_PROTO_PATH = path.join(__dirname, './proto/grpc/health/v1/health.proto');

const PORT = process.env.PORT;

const shopProto = _loadProto(MAIN_PROTO_PATH).hipstershop;
const healthProto = _loadProto(HEALTH_PROTO_PATH).grpc.health.v1;

const logger = pino(nrPino())

/**
 * Helper function that loads a protobuf file.
 */
function _loadProto(path) {
  const packageDefinition = protoLoader.loadSync(
    path, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true
    }
  );
  return grpc.loadPackageDefinition(packageDefinition);
}

/**
 * Helper function that gets currency data from a stored JSON file
 * Uses public data from European Central Bank
 */
function _getCurrencyData(callback) {
  const data = require('./data/currency_conversion.json');
  callback(data);
}

/**
 * Helper function that handles decimal/fractional carrying
 */
function _carry(amount) {
  const fractionSize = Math.pow(10, 9);
  amount.nanos += (amount.units % 1) * fractionSize;
  amount.units = Math.floor(amount.units) + Math.floor(amount.nanos / fractionSize);
  amount.nanos = amount.nanos % fractionSize;
  return amount;
}

/**
 * Lists the supported currencies
 */
function getSupportedCurrencies(call, callback) {
  newrelic.startWebTransaction('hipstershop.CurrancyService/GetSupportedCurrencies', function () {
    const txn = newrelic.getTransaction();
    try {
      txn.acceptDistributedTraceHeaders('HTTP', call.metadata.getMap());
      logger.info('Getting supported currencies...');
      newrelic.startSegment('gerCurrencyData', true, _getCurrencyData, function getCurrencyHandler(data) {
        callback(null, {
          currency_codes: Object.keys(data)
        });
      });
    } catch (err) {
      logger.error(`Supported currencies request failed: ${err}`);
      callback(err.message);
    } finally {
      txn.end();
    }
  })
}

/**
 * Converts between currencies
 */
function convert(call, callback) {
  newrelic.startWebTransaction('hipstershop.CurrancyService/Convert', function () {
    const txn = newrelic.getTransaction();

    logger.info('[Convert] Received conversion request');
    try {
      txn.acceptDistributedTraceHeaders('HTTP', call.metadata.getMap());

      newrelic.startSegment('getCurrencyData', true, _getCurrencyData, function getCurrencyHandler(data) {
        const request = call.request;

        // Convert: from_currency --> EUR
        const from = request.from;
        const euros = _carry({
          units: from.units / data[from.currency_code],
          nanos: from.nanos / data[from.currency_code]
        });

        euros.nanos = Math.round(euros.nanos);

        // Convert: EUR --> to_currency
        const result = _carry({
          units: euros.units * data[request.to_code],
          nanos: euros.nanos * data[request.to_code]
        });

        result.units = Math.floor(result.units);
        result.nanos = Math.floor(result.nanos);
        result.currency_code = request.to_code;

        newrelic.addCustomAttributes({
          "currenyFrom": "EUR",
          "currencyTo": request.to_code,
          "orginalAmount": "EUR " + euros.units,
          "conversionAmount": result.currency_code + ' ' + result.units + '.' + result.nanos
        });

        logger.info(`[Convert] conversion request successful processed from: EUR ${euros.units}.${euros.nanos} to: ${request.to_code} ${result.units}.${result.nanos}`);
        callback(null, result);
      });
    } catch (err) {
      logger.error(`conversion request failed: ${err}`);
      callback(err.message);
    } finally {
      txn.end();
    }
  });
}

/**
 * Endpoint for health checks
 */
function check(call, callback) {
  callback(null, {
    status: 'SERVING'
  });
}

/**
 * Starts an RPC server that receives requests for the
 * CurrencyConverter service at the sample server port
 */
function main() {
  logger.info(`Starting gRPC server on port ${PORT}...`);
  const server = new grpc.Server();
  server.addService(shopProto.CurrencyService.service, {
    getSupportedCurrencies,
    convert
  });
  server.addService(healthProto.Health.service, {
    check
  });

  server.bindAsync(
    `0.0.0.0:${PORT}`,
    grpc.ServerCredentials.createInsecure(),
    function () {
      logger.info(`CurrencyService gRPC server started on port ${PORT}`);
      server.start();
    },
  );
}

main();