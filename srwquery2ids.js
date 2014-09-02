var http = require('http'),
  fs = require('fs'),
  path = require('path'),
  utile = require('utile'),
  libxml = require('libxmljs');

var json_arr = [];
var srw_config = { // TODO: limit options
  start: 1,
  limit: 100
};

// args
var argv = require('minimist')(process.argv.slice(2), {
  string: ['q, O, d, f, h'],
  boolean: ['s', 'i', 'a'],
  alias: {
    'q': 'query',
    'O': 'output',
    'd': 'dir',
    'f': 'format',
    'h': 'host',
    's': 'stream',
		'i': 'include-all'
    'a': 'include-anno'
  },
  default: {
    'd': 'output',
    'O': 'output',
    'f': 'csv',
    'h': 'devtools.clarin.dk',
  }
});

var delimiter = (argv.f == 'tsv') ? '\t' : '%'; // TODO: custom delimiter

var annotationsOnly = (!argv.i && argv.a);

// eSciDoc 1.3.x SRW Namespaces
var ns_obj = {
  'sru-zr': 'http://www.loc.gov/zing/srw/',
  'escidocItem': 'http://www.escidoc.de/schemas/item/0.10',
  'escidocMetadataRecords': 'http://www.escidoc.de/schemas/metadatarecords/0.5',
  'escidocContainer': 'http://wwww.escidoc.de/schemas/container/0.4',
  'escidocContentStreams': 'http://www.escidoc.de/schemas/contentstreams/0.7',
  'escidocComponents': 'http://www.escidoc.de/schemas/components/0.9',
  'version': 'http://escidoc.de/core/01/properties/version/',
  'release': 'http://escidoc.de/core/01/properties/release/',
  'prop': 'http://escidoc.de/core/01/properties/',
  'srel': 'http://escidoc.de/core/01/structural-relations/',
  'relations': 'http://www.escidoc.de/schemas/relations/0.3',
  'xlink': 'http://www.w3.org/1999/xlink'
};

var getItemProperties = function(item) {
  var props = {}; // Properties object

  var escidocID_href = val.attr('href').value();
  props.escidocID = escidocID_href.substring(escidocID_href.indexOf('dkclarin'), escidocID_href.length);
  var contentModelID_href = val.get('escidocItem:properties/srel:content-model', ns_obj).attr('href').value();
  props.contentModelID = contentModelID_href.substring(contentModelID_href.indexOf('dkclarin'), contentModelID_href.length);

  var obj_pid = val.get('escidocItem:properties/prop:pid', ns_obj);
  var ver_pid = val.get('escidocItem:properties/prop:version/version:pid', ns_obj);
  props.ver_no = val.get('escidocItem:properties/prop:latest-version/version:number', ns_obj).text();

  var last_date = val.attr('last-modification-date').value();

  props.pid = null;

  if (ver_pid != null)
    props.pid = ver_pid.text();
  else
    props.pid = obj_pid.text();

  return props;
}

// Parse and pull ID data from the XMLDocument
var parse = function(doc, stream) {
  var totalRecords = doc.get('//sru-zr:numberOfRecords', ns_obj).text();
  var items = doc.find('//escidocItem:item', ns_obj);
  var containers = doc.find('//escidocContainer:container', ns_obj);

  console.log('Total records in query: ' + totalRecords);

  if (utile.isArray(items)) console.log('Found ' + items.length + ' items.');
  else console.error('Found 0 items in XMLDocument.');

  if (utile.isArray(containers)) console.log('Found ' + containers.length + ' containers.');
  else console.error('Found 0 containers in XMLDocument.');

  // Add the items to list
  utile.each(items, function(val, key) {
    // CSV format: item%{escidocID}%[{objectPID}}|{lastVersionPID}]%{versionNo}
    if (!annotationOnly) {
      addMember(getItemProperties(val));
    }

		if(argv.a){
      var relations = val.find('relations:relations/relations:relation', ns_obj);
      relations.forEach(function(relation) {
        if (relation != undefined || relation != null) {
          relation_type = relation.attr('predicate').value();
          if (relation_type.indexOf('HasAnnotation') != -1) {
            var relation_href = relation.attr('href').value();
            var relationObjID = relation_href.substring(relation_href.indexOf('dkclarin'), relation.length);

            console.log('Found Annotation: ' + relationObjID);

            // retrieve properties for Annotation item from eSciDoc REST
            var annoPropsItem = retrieveItemProperties(relationObjID, function(callback) {
              var props = getItemProperties(annoPropsItem);
              addMember(props); // add annotation member to file
            });
          }
        }
      });
    }
  });

  // TODO: Handle containers array
  utile.each(containers, function(val, key) {});

  // iterate over complete SRW result set
  if (Number(totalRecords) >= (srw_config.limit + srw_config.start)) {
    srw_config.start += srw_config.limit;
    retrieveSRWResult(srw_config, function(xml) {
      parse(xml, stream);
    });
  } else {
    if (argv.f == 'json')
      addMemberToFile(JSON.stringify(json_arr), stream); // write JSON object to output file
    if (stream instanceof fs.WriteStream)
      stream.destroySoon();
  }
}

/** SRW Query fetch */
var retrieveSRWResult = function(config, callback) {
  return execGET(srw_options(argv.q, config), callback);
}

/* Item Properties */
var retrieveItemProperties = function(escidocID, callback) {
  return execGET(ir_options(escidocID, '/properties'), callback);
}

var execGET = function(options, callback) {
  http.get(options, function(res) {
    var str = "";
    res.on('data', function(chunk) {
      str += chunk;
    });
    res.on('end', function() {
      if (res.statusCode == "200")
        callback(libxml.parseXmlString(str));
      else
        throw new Error("Error code: " + res.statusCode);
    });
  }).on('error', function(e) {
    console.error(e);
  });

  return true;
}

/** SRW query options */
var srw_options = function(queryCQL, record_vals) {
  var search_path_limits = "&maximumRecords=" + record_vals.limit + "&startRecord=" + record_vals.start;
  var targetUrl = argv.h;
  return {
    host: (targetUrl.indexOf('http://') != -1) ? targetUrl.replace('http://', '') : targetUrl,
    path: '/srw/search/escidoc_all?query=' + encodeURIComponent(queryCQL.replace(/\s/g, "%20")) + search_path_limits + '&d=' + Date.now()
  };
};

/* eSciDoc Item REST */
var ir_options = function(escidocID, path) {
  var targetUrl = argv.h;
  return {
    host: (targetUrl.indexOf('http://') != -1) ? targetUrl.replace('http://', '') : targetUrl,
    path: '/ir/item/' + escidocID + path
  };
};

// append props to output file
var addMember = function(props) {
  if (argv.f == 'csv' || argv.f == 'tsv')
    addMemberToFile(['item', props.escidocID, props.contentModelID, props.pid, props.ver_no].join(delimiter) + '\n', stream);
  else if (argv.f == 'json') // write output to JSON object
    json_arr.push({
		  type: 'item',
		  systemID: props.escidocID,
		  contentModelID: props.contentModelID,
		  PID: props.pid,
		  versionNo: props.ver_no
		});
  else
    throw new Error('Unsupported format:' + argv.f);
}

// write output to file
var addMemberToFile = function(data, stream) {
  if (stream != null)
    if (stream.write(data)) console.log('Data written: ' + data);
    else console.log('Buffer full waiting for drain..');
  else
    process.send(data);
}

var run = function() {
  var is_json = (argv.f == 'json');
  var is_csv = (argv.f == 'csv');
  var is_tsv = (argv.f == 'tsv');

  var file_name = argv.O;
  var file_dir = argv.d;
  var file_path = null;

  var stream = null;

  if (!argv.s) {
    console.log("Exists?: " + file_dir);
    if (fs.existsSync(file_dir)) {
      if (is_json)
        file_path = path.join(file_dir, file_name + '.json');
      else if (is_csv)
        file_path = path.join(file_dir, file_name + '.csv');
      else if (is_tsv)
        file_path = path.join(file_dir, file_name + '.tsv');

      stream = fs.createWriteStream(file_path, {
        encoding: 'utf8'
      });
      stream.on('error', function(err) {
        if (err) throw err;
      });
      stream.on('drain', function() {
        console.log('Data drained from buffer to file:' + file_path);
      });
      stream.on('close', function() {
        console.log('Closing file stream.');
      });

      console.log('Using query: ' + argv.q);
    } else {
      console.log('Target directory doesn\'t exist: ' + file_dir);
      fs.mkdir(file_dir, function(err) {
        if (err) throw err;
        console.log('Created directory: ' + file_dir);
        run();
      });
    }
  }

  if (argv.s || stream != null)
    retrieveSRWResult(srw_config, function(xml) {
      parse(xml, stream);
    });
}

module.exports = run;

run();
