var app = require('express')(), fork = require('child_process').fork;
app.get('/srwquery2idlist', function(req, res) {
    var query = fork(__dirname + '/srwquery2ids.js', ['-q', req.query.query, '-f', 'tsv', '-s']);
    var file_data = new String();
    query.on('message', function(message) {
	file_data += message;
    });
    query.on('exit', function(code) {
	if(code == 0) {
	    res.setHeader('Content-Type', 'text/tab-separated-values');
	    res.send(new Buffer(file_data));
	} else res.send(500, { error: 'error occurred!'});
    });
});

app.listen(3001);
console.log('Listing on port 3001');

