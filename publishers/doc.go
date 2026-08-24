// Package publishers provides the Lambda helper for the single consumer of the
// DynamoDB event-log stream: it reads INSERT records and publishes each event to
// an SNS topic for fan-out.
//
// A publisher Lambda is triggered by the event-log stream. HandleDynamoDBEvent
// wraps each event in a CloudWatchEvent envelope (via the SNS publisher in the
// stream package) and publishes it to the events topic. It can also publish to a
// FIFO companion topic for ordered consumers. Downstream consumers subscribe to
// the topic rather than reading the stream directly.
package publishers
