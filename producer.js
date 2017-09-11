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

const Realm = require('realm');
const eventTranslator = require('./EventTranslator');
const KinesisClient = require('./KinesisClient');
const dotenv = require('dotenv');

// read in the .env file to process the environment variables (if any)
// NOTE: these will override any existing variables
dotenv.config();

// debug code to show environment - helpful to ensure you are using the correct keys
if (process.env.showEnv && process.env.showEnv.toUpperCase() === 'TRUE')
    console.log(process.env);

// add any user-specific Realm configuration items here
let realmConfig = {
    // professional/enterprise api access token
    apiAccessToken: process.env.apiAccessToken,

    // Insert the Realm admin token here
    //   Linux:  cat /etc/realm/admin_token.base64
    //   macOS:  cat realm-object-server/admin_token.base64
    adminToken: process.env.adminToken,

    // the URL to the Realm Object Server
    serverUrl: process.env.serverUrl,

    // The regular expression you provide restricts the observed Realm files to only the subset you
    // are actually interested in. This is done in a separate step to avoid the cost
    // of computing the fine-grained change set if it's not necessary.
    notifierPath: process.env.notifierPath
};

// set the name of the kinesis stream you wish to use for notifications here
let streamName = process.env.kinesisStreamName;

// create a kinesis client outside the handler so it persists between change events
let kinesisClient = new KinesisClient();

// Unlock Professional Edition APIs - this is only required if you need more than 3 functions per server
if (realmConfig.apiAccessToken)
    Realm.Sync.setAccessToken(realmConfig.apiAccessToken);

// create the admin user
let adminUser = Realm.Sync.User.adminUser(realmConfig.adminToken);

// register the event handler callback
Realm.Sync.addListener(realmConfig.serverUrl, adminUser, realmConfig.notifierPath, 'change', handleChange);

console.log(`Listening for Realm changes. Outputting all changes to stream '${streamName}'\n`);

// The handleChange callback is called for every observed Realm file whenever it
// has changes. It is called with a change event which contains the path, the Realm,
// a version of the Realm from before the change, and indexes indication all objects
// which were added, deleted, or modified in this change
function handleChange(changeEvent) {
    let messages = eventTranslator(changeEvent);

    kinesisClient.putRecords(streamName, messages);
}
