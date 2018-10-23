// Imports the Google Cloud client library
const Datastore = require('@google-cloud/datastore');
const { Storage } = require('@google-cloud/storage');
var es = require('event-stream')
const byline = require('byline');
const projectId = "project";

// Creates a client
const datastore = new Datastore({
  projectId: projectId,
});
const bucketName = "one2one-analytics";
const filename = "intent-classifications.csv";

const storage = new Storage({
  projectId: projectId,
});

const kind = 'UserProfile';

exports.CSVImport = (event, callback) => {
  //read from cloud
  var remoteFile = storage.bucket(bucketName).file(filename);
  const gcsStream = remoteFile.createReadStream()
  gcsStream.pipe(es.split())
  .pipe(es.map(function (doc, next) {
    saveProfile(doc, next);
    //console.log(doc);
  }));
}

var saveProfile = function(line, cb) {
  //console.log(line);
    var triple = line.toString().split(',');
    var userId = triple[1];
    // The Cloud Datastore key for the new entity
    var profileKey = datastore.key([kind, userId]);
    // Prepares the new entity
    var profile = {
      key: profileKey,
      data: {
        'userId': userId,
        'classification': triple[2],
        'date': triple[0]
      },
    };
    // Saves the entity
    datastore
      .save(profile)
      .then(() => {
        //console.log(`Saved ${profile.key.name}: ${profile.data.classification}`);
      })
      .catch(err => {
        console.error('ERROR:', err);
    });
    return cb();
}
