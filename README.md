# Realm Kinesis Producer

A sample application that monitors a Realm Object Server's Global Notifier, translates the changeEvent objects into pure json payloads,
and pushes them to an AWS Kinesis (https://realm.io/products/realm-mobile-platform/) stream. It also contains a trivial Kinesis listener
that monitors the Kinesis stream and writes all messages out to the console.

This project is written for simplicity, not performance. For production implementaitons you should probably use the Kinesis Producer Library
to insert records into the stream and either AWS Lambda or the Kinesis Consumer Library to react to those records.

This sample is designed to work with the Realm Mobile Platform (https://realm.io/products/realm-mobile-platform/) version 1.8.2 or later.

## Required environment variables

To use, make sure you have defined the following environment variables:

* apiAccessToken - token used to unlock the realm professional/enterprise API
* adminToken - realm admin token for the specified realm server
* serverUrl - the URL of the realm server (eg 'realms://myrealms.somedomain.com:9443')
* notifierPath - a string or regular express representing the realm(s) being monitored
* region - the AWS region of the Kinesis stream
* streamName - the name of the Kinesis stream

You will also need to configure environment variables for your AWS credentials

* AWS_ACCESS_KEY_ID - your IAM access key id
* AWS_SECRET_ACCESS_KEY - your IAM secret access key
* AWS_DEFAULT_REGION - the default region used by AWS when no region is specified

Debug environment variables (these will help debugging configuration issues):

* verbose - emit the Kinesis write response to the console after every write
* showEnv - emit all current environment variables on startup

There are two ways you can configure these environment variables:

1) Add them to the shell where the project is running
2) Add them to a .env file (which should be ignored by git)

We suggest the latter.

## MIT License

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