// Package projectors provides an asynchronous runtime for committed-event read models, with
// idempotency, retry classification, and telemetry.
//
// Projectors normally consume publisher SNS fan-out through raw-delivery SNS-to-SQS.
// NewSQSHandler and NewSNSHandler decode CloudWatchEvent envelopes into StreamRecords.
// NewLambdaHandler supports direct DynamoDB Streams consumption.
package projectors
