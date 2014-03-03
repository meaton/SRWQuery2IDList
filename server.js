var app = require('express')(), fork = require('child_process').fork;

app.get('/srwquery2idlist', function(req, res) {
    var query = fork(__dirname + '/srwquery2ids.js', ['-q', req.query.query, '-f', 'tsv', '-s']);
    var file_data = new String();

    /*query.stdout.on('data', function(data) {
	console.log('log from child: ' + data.toString());
    });
    query.stderr.on('data', function(data) {
	console.log('child process => stderr: ' + data.toString());
    });*/
    query.on('message', function(message) {
	file_data += message;
    });
    query.on('exit', function(code) {
	if(code == 0) {
	    res.set({
  		'Content-Type': 'text/tab-seperated-values',
  		'Content-Length': file_data.length});
	    res.send(new Buffer(file_data));
	} else res.send(500, { error: 'error occurred!'});
    });
});

app.listen(3001);
console.log('Listing on port 3001');

