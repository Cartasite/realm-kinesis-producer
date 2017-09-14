/*
Copyright (c) 2017 Cartasite, LLC (www.cartasite.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

const AWS = require('aws-sdk');
const crc32 = require('fast-crc32c');
const fs = require('fs');

class KinesisClient {
    constructor(options) {
        if (!options) {
            options = { region:"us-east-1" };
        }
        this.kinesis = new AWS.Kinesis(options);
        // this is a queue of message queues (array of arrays), each no more than 500 messages long
        this._messageSizeLimit = 1000000;
        this._messageLimit = 250;   // max 500 records per PUT
        this._messageTimeout = 1000; // wait 500 ms between PUT
        this._messageQueue = [];
        this._messageTimer = null;
        this._iteratorWaitTime = 200;
        this._nextIteratorTimeout = 5000;
        this._count = 0;

        // check to see if the queue is full
        if (this._messageQueue.length === this._messageLimit) {
            // cancel the current timer
            clearTimeout(this._messageTimer);

            // send the messages now
            this._sendToKinesis(streamName);
        }
    }

    static get IteratorTypes() {
        return {
            AtSequenceNumber: { type: 'AT_SEQUENCE_NUMBER', sequenceNumber: null },
            AfterSequenceNumber: { type: 'AFTER_SEQUENCE_NUMBER', sequenceNumber: null },
            TrimHorizon: { type: 'TRIM_HORIZON' },
            Latest: { type: 'LATEST' },
            AtTimestamp: { type: 'AT_TIMESTAMP', timeStamp: null }
        };
    }

    describeStream(streamName, cb) {
        let params = {
            StreamName: streamName
        };
        return this.kinesis.describeStream(params, cb);
    }

    describeLimits(cb) {
        return this.kinesis.describeLimits({}, cb)
    };

    // private method gets an iterator from a given shard
    _getIterator(shardName, shardId, iteratorType, cb) {
        if (!this._isValidIteratorType(iteratorType))
            return `Invalid iteratorType "${iteratorType}"`;

        let params = {
            ShardId: shardId, /* required */
            ShardIteratorType: iteratorType.type, /* required */
            StreamName: shardName, /* required */
        };
        if (iteratorType.type === KinesisClient.IteratorTypes.AtSequenceNumber.type || iteratorType.type === KinesisClient.IteratorTypes.AfterSequenceNumber.type) {
            if (!iteratorType.sequenceNumber)
                return cb(`You must specify a sequence number with the '${iteratorType}' iterator`);
            params.StartingSequenceNumber = iteratorType.sequenceNumber;
        }
        return this.kinesis.getShardIterator(params, cb);
    }

    // private method that constantly looks for more data. When no data is found, it waits 1 second before trying again to prevent throttling by AWS
    // each time a record is found, it is passed to the callback function
    _loop (shardIterator, callback) {
        try {
            //console.log("looking for records to process");
            this.kinesis.getRecords({ShardIterator: shardIterator, Limit: 100}).promise().then((data) => {
                for(let record of data.Records) {
                    let json = record.Data.toString();
                    record.Data = JSON.parse(json);
                    callback(null, record);
                }

                //console.log("finished processing records");

                // if we had records to process, get the next shardIterator immediately, otherwise wait a period of time
                if (data.Records.length > 0)
                    this._loop(data.NextShardIterator, callback);
                else {
                    // wait 1000 ms before continuing
                    setTimeout(() => {
                        //console.log("1 second delay");
                        this._loop(data.NextShardIterator, callback);
                    }, this._nextIteratorTimeout);
                }
            });
        } catch (err) {
            getCallback(err);
        }
    }

    // a validation method to ensure only valid iterator types are used
    _isValidIteratorType(iteratorType) {
        if (typeof(iteratorType) !== "object")
            return false;
        if (!iteratorType.type)
            return false;
        return Object.keys(KinesisClient.IteratorTypes).reduce((accum, _iteratorType) => {
            return (KinesisClient.IteratorTypes[_iteratorType].type === iteratorType.type) || accum;
        }, false);
    }

    // a high-level method to listen to a stream and execute a callback function on every record received
    listen(streamName, recordCallback, iteratorType = KinesisClient.IteratorTypes.Latest) {
        this.describeStream(streamName, (err, response) => {
            let streamDescription = response.StreamDescription;
            if (err) {
                return console.log("[Error]:\t", err);
            }

            // get an array of all the shards in the stream
            let shardIdArray = streamDescription.Shards.map((shard) => {
                    return shard.ShardId;
            });

            // listen to all known shards - each shard gets its own iterator and thread
            for(let shardId of shardIdArray) {
                this._getIterator(streamName, shardId, iteratorType, (err, iterator) => {
                    if (err) {
                        return console.log('[Error]:\tGet iterator', err);
                    }
                    console.log('[Shard]:\t', iterator,'\n');
                    this._loop(iterator.ShardIterator, recordCallback);
                });
            }
        });
    }

    // a private method that converts the results of the eventTranslator into a parameter for putRecords
    _prepareRecord(message) {
        // if the data is not already a string, turn it into a json string
        if (typeof message.Data !== 'string')
            message.Data = JSON.stringify(message.Data);

        // if no partition key is provided, compute one from the data
        if (!message.PartitionKey)
            message.PartitionKey = crc32.calculate(message.Data);

        // we presume that the partition key is a 32-bit integer. if other keys are used, this code must be modified
        if (typeof message.PartitionKey !== 'string') {
            // convert the 32-bit integer into a base64-encoded string
            let buf = new Buffer(4);
            buf.writeUInt32LE(message.PartitionKey);
            message.PartitionKey = buf.toString('base64');
        }
        return message;
    }

    // this method adds all records to a queue and starts a timer. when the queue is full or the timeout expires it sends the records to Kinesis
    putRecord(streamName, record) {
        // add the record to the queue
        this._messageQueue.push(record);

        // write the messages to the log
        if (process.env.verbose && process.env.verbose.toUpperCase() === 'TRUE')
            console.log(`[Put]:\t1 record added to queue. Size is now ${this._messageQueue.length} records`);

        // set a timer to send all records in 1 second
        this._sendMessages(streamName);
    }

    // when putting many records, simply call putRecord repeatedly - this is ok because records are sent no faster than once every 500 ms
    putRecords(streamName, records) {
        this._messageQueue = this._messageQueue.concat(records);

        // write the messages to the log
        if (process.env.verbose && process.env.verbose.toUpperCase() === 'TRUE')
            console.log(`[Put]:\t${records.length} record(s) added to queue. Size is now ${this._messageQueue.length} record(s)`);

        // set a timer to send all records in _messageTimeout milliseconds
        this._sendMessages(streamName);
    }

    // set a timer to send messages in a timeout period. allows multiple messages to accumulate in a queue before sending them
    _sendMessages(streamName) {
        if (this._messageQueue.length > 0) {
            if (!this._messageTimer)
                this._messageTimer = setInterval(() => {
                    this._sendToKinesis(streamName);
                }, this._messageTimeout);
        } else {
            if (process.env.verbose && process.env.verbose.toUpperCase() === 'TRUE')
                console.log(`[Send]:\tQueue is empty`);
        }
    }

    // the private method that actually sends all messages in the messageQueue to Kinesis - it is called manually when the queue is full or when a timeout expires
    _sendToKinesis(streamName) {
        // move up to
        let messageCount = this._messageQueue.length <= this._messageLimit ? this._messageQueue.length : this._messageLimit;
        if (messageCount < 1) {
            return;
        }

        // prepare the parameter payload for putRecord
        let params = {
            Records: [], /* required */
            StreamName: streamName /* required */
        };
        let estimatedSize = 0;

        // add as many records to the parameter message as will fit, as constrained by kinesis message limits
        while(estimatedSize < this._messageSizeLimit && params.Records.length < this._messageLimit && params.Records.length < this._messageQueue.length) {
            let index =  params.Records.length;
            let queuedMessage = this._messageQueue[index];
            let message = this._prepareRecord(queuedMessage);
            estimatedSize += message.Data.length + 40;
            params.Records.push(message);
        }

        // remove messages from the front of the queue
        let messages = this._messageQueue.splice(0, params.Records.length);

        if (process.env.verbose && process.env.verbose.toUpperCase() === 'TRUE')
            console.log(`[Send]:\t${params.Records.length} will be sent. ${this._messageQueue.length} remain.`);

        // write the record to the stream
        this.kinesis.putRecords(params, (err, putResponse) => {
            if (err) {
                // add the unsent messages to the the error log
                fs.appendFile('errors.log', JSON.stringify(messages));
                return console.log('[Write Error]:\t' + err);
            }

            if (putResponse.FailedRecordCount > 0)
                this.putRecords(streamName, messages);

            console.log(`[Sent]:\t${params.Records.length} records. Response: ${JSON.stringify(putResponse)}`);
        });
    }
}

module.exports = KinesisClient;
