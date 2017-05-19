#!/usr/bin/env node

var app = require('express')(),
    fork = require('child_process').fork;

app.get('/srwquery2idlist', function(req, res) {
    var format = (req.query.format != null) ? req.query.format : 'tsv';
    var queryParams = ['-q', req.query.query, '-f', format, '-s'];

    // inc all items in query
    if (req.query.items == 'true' || req.query.items == null)
        queryParams.push('-i');
    // inc all annotations
    if (req.query.annos == 'true' || req.query.annos == null)
        queryParams.push('-a');

    res.setTimeout(0); //might take a while

    var file_data = new String();
    var query = fork(__dirname + '/srwquery2ids.js', queryParams);

    query.on('message', function(message) {
        if (message.data != null)
            file_data += message.data;
        else
            console.error('Received no data from child process.');
    });

    query.on('exit', function(code) {
        console.log('exit', code);
        console.log('data length', file_data.length);
        if (code == 0 && file_data.length > 0) {
            res.setHeader('Content-Type', getContentType(format));
            res.send(new Buffer(file_data));
        } else res.send(500, {
            error: 'error occurred!'
        });
    });
});

function getContentType(format) {
    switch (format) {
        case "tsv":
            return 'text/tab-separated-values';
        case "csv":
            return 'text/csv'
        case "json":
            return 'application/json';
        default:
            return 'text/tab-separated-values';
    }
}

app.listen(3001);

console.log('Listing on port 3001');
