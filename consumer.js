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

const KinesisClient = require('./KinesisClient');

// set the name of the kinesis stream you wish to use for notifications here
let streamName = process.env.kinesisStreamName;

// create a kinesis client outside the handler so it persists between change events
let kinesisClient = new KinesisClient();

require('dotenv').config();

console.log(`Listening for events on stream '${streamName}'\n`);

// start listening to all shards of a stream
let iteratorType = KinesisClient.IteratorTypes.Latest;

kinesisClient.listen(streamName, recordHandler, iteratorType);

// this method is called once for every record it reads from Kinesis
function recordHandler(err, record) {
    if (err)
        return console.log("[Error]: ", err);

    // each time we get a record, convert it to json and write it out to the console
    // in a non-trivial example, you might do work
    console.log(`[Read]: SequenceNumber '${record.SequenceNumber}'`);

    console.log(JSON.stringify(record));
}

