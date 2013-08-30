#!/usr/bin/env node
/**
* @author Mitchell Seaton
*/
var http = require('http'), fs = require('fs'), path = require('path'), utile = require('utile'), libxml = require('libxmljs');
	var json_arr = [];
	var srw_config = {start: 1, limit: 10};
	// args
	var argv = require('optimist')
		.usage('Convert eSciDoc SRW query results to ID list.\nUsage: $0 -q [input]')
		.demand(['q'])
		.alias('q', 'query') // File eSciDoc SRW XML
		.alias('O', 'output') // Output file name
		.alias('d', 'dir') // Target directory for output file
		.alias('f', 'format') // Output type (csv, json)
		.alias('h', 'host') // SRW host
		.default({'d': 'output', 'O': 'output', 'f': 'csv', 'h': 'devtools.clarin.dk'})
		.describe('q', 'eSciDoc SRU/W query (CQL)')
		.describe('O', 'Output file name.')
		.describe('d', 'Target directory for output.')
		.describe('f', 'Output file format')
		.check(function(opts) {
		  return true;
		})
		.argv;


	// eSciDoc 1.3.x SRW 
	var ns_obj = {'sru-zr':'http://www.loc.gov/zing/srw/',
		'escidocItem':'http://www.escidoc.de/schemas/item/0.10',
		'escidocMetadataRecords':'http://www.escidoc.de/schemas/metadatarecords/0.5',
		'escidocContainer':'http://wwww.escidoc.de/schemas/container/0.4',
		'escidocContentStreams':'http://www.escidoc.de/schemas/contentstreams/0.7',
		'escidocComponents':'http://www.escidoc.de/schemas/components/0.9',
		'version':'http://escidoc.de/core/01/properties/version/',
		'release':'http://escidoc.de/core/01/properties/release/',
		'prop':'http://escidoc.de/core/01/properties/',
		'srel':'http://escidoc.de/core/01/structural-relations/',
		'xlink':'http://www.w3.org/1999/xlink' };

	// Parse and pull ID data from the XMLDocument
	var parse = function(doc, stream) {
		var totalRecords = doc.get('//sru-zr:numberOfRecords', ns_obj).text();
		var items = doc.find('//escidocItem:item', ns_obj);
		var containers = doc.find('//escidocContainer:container', ns_obj);

	    console.log('Total records in query: ' + totalRecords);

	    if(utile.isArray(items)) console.log('Found ' + items.length + ' items.');
	    else console.error('Found 0 items in XMLDocument.');
	 
	    if(utile.isArray(containers)) console.log('Found ' + containers.length + ' containers.');
	    else console.error('Found 0 containers in XMLDocument.');

	    // Add the items to list
	    utile.each(items, function(val, key) {
	        // item%{escidocID}%[{objectPID}}|{lastVersionPID}]
	      
	        var escidocID_href = val.attr('href').value();
	        var escidocID = escidocID_href.substring(escidocID_href.indexOf('dkclarin'), escidocID_href.length);
	        var obj_pid = val.get('escidocItem:properties/prop:pid', ns_obj);
	        var ver_pid = val.get('escidocItem:properties/prop:latest-version/version:pid', ns_obj);
		var pid = null;
		if(ver_pid != null)
		    pid = ver_pid.text();
		else
		    pid = obj_pid.text();	

		if(argv.f == 'csv')
		    addMemberToFile(['item', escidocID, pid].join('%') + '\n', stream);
		else if(argv.f == 'json') json_arr.push({type: 'item', systemID: escidocID, PID: pid});
		else throw new Error('Unsupported format:' + argv.f);

	    });

	    // TODO Handle containers array
	    utile.each(containers, function(val, key) {
	    });

	    // iterate over complete SRW result set
	    if(Number(totalRecords) >= (srw_config.limit+srw_config.start)) { 
		srw_config.start += srw_config.limit;
		retrieveSRWResult(srw_options(argv.q, srw_config), stream);
	    } else {
		if(argv.f == 'json') addMemberToFile(JSON.stringify(json_arr), stream);
		stream.destroySoon();
	    }
      	}
	    
	/** SRW Query fetch */
    	var retrieveSRWResult = function(options, stream) {
	    http.get(options, function(res) {
		var str = "";     
		res.on('data', function(chunk) {
		    str += chunk;
		});
		res.on('end', function() {
		    if(res.statusCode == "200")
			parse(libxml.parseXmlString(str), stream);
		    else throw new Error("Invalid SRU/W");
		});
	    }).on('error', function(e) {
		console.error(e);
	    });

	    return true;
    	}

	/** SRW query options */
	var srw_options = function(queryCQL, record_vals) {
		var search_path_limits = "&maximumRecords="+record_vals.limit+"&startRecord="+record_vals.start;
		var targetUrl = argv.h;
		return { 
			host: (targetUrl.indexOf('http://') != -1) ? targetUrl.replace('http://', '') : targetUrl,
			path: '/srw/search/escidoc_all?query='+encodeURIComponent(queryCQL.replace(/\s/g,"%20"))+search_path_limits+'&d='+Date.now()
		};
	};

	// append data to output file
	var addMemberToFile = function(data, stream) {
		if(stream.write(data)) console.log('Data written: ' + data);
		else console.log('Buffer full waiting for drain..');
	}

	var main = function() {
		var is_json = (argv.f == 'json');
	   	var is_csv = (argv.f == 'csv');

	   	var file_name = argv.O;
	  	var file_dir = argv.d;
	  	var file_path = null;

		var search_cql = argv.q
		
		fs.exists(file_dir, function(exists) {
		    if(exists) {
			if(is_json)
	    	        	file_path = path.join(file_dir, file_name + '.json');
		    	else if(is_csv)
		    		file_path = path.join(file_dir, file_name + '.csv');

		    var stream = fs.createWriteStream(file_path, {encoding: 'utf8'});
		    stream.on('error', function(err) {
			if (err) throw err;
	    	    });
		    stream.on('drain', function() {
		        console.log('Data drained from buffer to file:' + file_path);
		    });
		    stream.on('close', function() {
			console.log('Closing file stream.');
		    });

			console.log('Using query: ' + search_cql);
			retrieveSRWResult(srw_options(search_cql, srw_config), stream);
		
    
	            } else {
			console.log('Target directory doesn\'t exist: ' + file_dir);
			fs.mkdir(file_dir, function(err) { if(err) throw err; console.log('Created directory: ' + file_dir); main(); });
		    }
	        });
	}

	main();
