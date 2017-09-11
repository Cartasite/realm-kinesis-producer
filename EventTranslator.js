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

const crc32 = require('fast-crc32c');

// this method computes a CRC32 checksum of an object by serializing it to json
// we use it to quickly determine whether one record is identical to another
function checksum(obj) {
    if (!obj)
        return null;
    // note: this package uses C++ on installation to enable SSE4 instructions on 64-bit chips that support it.
    // if you installation does not supprt C++, feel free to use a different hash method
    return crc32.calculate(JSON.stringify(obj));
}

// this function translates the changeEvent payload from indexes into javascript objects
// it also removes redundant delete/insert events when a record moves but does not change
function eventTranslator(changeEvent) {
    let translated = [];

    for (let collectionName in changeEvent.changes) {
        try {
            let changes = changeEvent.changes[collectionName];
            let objects = null;
            let oldObjects = null;
            if (changeEvent.realm.schema.length > 0)
                objects = changeEvent.realm.objects(collectionName);
            if (changeEvent.oldRealm.schema.length > 0)
                oldObjects = changeEvent.oldRealm.objects(collectionName);

            // when an object is deleted, three events are fired:
            //  1) a delete event for the object being deleted. its value is in oldObjects
            //  2) if the item deleted was not at the end of the collection
            //      a) a delete event for the item at the end of the collection
            //      b) an insert event at the whole vacated by the deleted item

            // generate a checksum for each object (old and new, if necessary
            // so we can determine if an object has actually been inserted or deleted
            // add these checksums to the position
            let checksums = {
                insertions: changes.insertions.map((pos) => {
                    return {data: objects[pos], checksum: checksum(objects[pos])}
                }),
                modifications: changes.modifications.map((pos) => {
                    return {
                        data: objects[pos],
                        checksum: checksum(objects[pos]),
                        oldData: oldObjects[pos],
                        oldChecksum: checksum(oldObjects[pos])
                    }
                }),
                deletions: changes.deletions.map((pos) => {
                    return {data: oldObjects[pos], checksum: checksum(oldObjects[pos])}
                })
            };

            // iterate over each insertion
            for(let insertItem of checksums.insertions) {
                // check to see if there is a deletion that exactly matches this insert's checksum
                let index = checksums.deletions.findIndex((deleteItem) => {
                    if (!deleteItem)
                        return false;
                    return deleteItem.checksum === insertItem.checksum;
                });

                // if no matching deletion was found, add it to the collection
                if (index === -1) {
                    translated.push({
                        Data: {collectionName, insert: insertItem.data},
                        PartitionKey: insertItem.checksum
                    })
                } else {
                    // remove the matching deletion from the array of deletions so we don't process it again
                    delete checksums.deletions[index];
                }
            }

            // iterate over each modification to look for records where no data change actually occured
            for(let item of checksums.modifications) {
                // check to see if the checksum of the new item is the same as the checksum of the old object
                if (item.checksum !== item.oldChecksum)
                // if they are different, add it to the translated colleciton
                // otherwise the record was edited but no data changed. meaning we can safely ignore it
                    translated.push({
                        Data: {collectionName, modify: {data: item.data, oldData: item.oldData}},
                        PartitionKey: item.checksum
                    });
            }

            // iterate over all remaining deletions
            for(let deleteItem of checksums.deletions) {
                // if we didn't find a match above, this must be a real deletion
                translated.push({
                    Data: {collectionName, delete: deleteItem.data},
                    PartitionKey: deleteItem.checksum
                });
            }

        } catch (err) {
            console.log("[Error]: ", err);
        }
    }
    return translated;
}

module.exports = eventTranslator;