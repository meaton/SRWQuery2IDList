var http = require('http'),
  fs = require('fs'),
  path = require('path'),
  utile = require('utile'),
  libxml = require('libxmljs');

var stream = null;
var json_arr = [];
var srw_config = { // TODO: limit options
  start: 1,
  limit: 100
};

// args
var argv = require('minimist')(process.argv.slice(2), {
  string: ['q, O, d, f, h'],
  boolean: ['s', 'i', 'a', 'p'],
  alias: {
    'q': 'query',
    'O': 'output',
    'd': 'dir',
    'f': 'format',
    'h': 'host',
    's': 'stream',
		'i': 'include-all',
    'a': 'include-anno',
    'p': 'validate-pids',
  },
  default: {
    'd': 'output',
    'O': 'output',
    'f': 'csv',
    'h': 'devtools.clarin.dk'
  }
});

var delimiter = (argv.f == 'tsv') ? '\t' : '%'; // TODO: custom delimiter
var validate = argv.p;

console.log('include all: ' + argv.i);
console.log('include anno: ' + argv.a);
console.log('validate: ' + validate);

var annotationsOnly = (!argv.i && argv.a);

// eSciDoc 1.3.x SRW Namespaces
var ns_obj = {
  'sru-zr': 'http://www.loc.gov/zing/srw/',
  'escidocItem': 'http://www.escidoc.de/schemas/item/0.10',
  'escidocMetadataRecords': 'http://www.escidoc.de/schemas/metadatarecords/0.5',
  'container': 'http://www.escidoc.de/schemas/container/0.9',
  'escidocContentStreams': 'http://www.escidoc.de/schemas/contentstreams/0.7',
  'escidocComponents': 'http://www.escidoc.de/schemas/components/0.9',
  'version': 'http://escidoc.de/core/01/properties/version/',
  'release': 'http://escidoc.de/core/01/properties/release/',
  'prop': 'http://escidoc.de/core/01/properties/',
  'srel': 'http://escidoc.de/core/01/structural-relations/',
  'relations': 'http://www.escidoc.de/schemas/relations/0.3',
  'xlink': 'http://www.w3.org/1999/xlink'
};

var getProperties = function(obj, name, callback) {
  var props = {}; // Properties object
  props.name = (name == null) ? obj.name() : name;

  var xpathRoot = '*/';
  /*
  if(props.name == 'item') xpathRoot = 'escidocItem:properties/'
  else if(props.name == 'container') xpathRoot = 'container:properties/';
  */
	var escidocID_href = obj.attr('href').value();
  props.escidocID = escidocID_href.substring(escidocID_href.indexOf('dkclarin'), escidocID_href.length);
	if(props.escidocID.indexOf('properties') != -1) props.escidocID = props.escidocID.substring(0, props.escidocID.indexOf('properties')-1);

  var contentModelID_href = obj.get(xpathRoot + 'srel:content-model', ns_obj).attr('href').value();
  props.contentModelID = contentModelID_href.substring(contentModelID_href.indexOf('dkclarin'), contentModelID_href.length);

  var obj_pid = obj.get(xpathRoot + 'prop:pid', ns_obj);
	var ver_pid = obj.get(xpathRoot + 'prop:version/version:pid', ns_obj);
  props.ver_no = obj.get(xpathRoot + 'prop:latest-version/version:number', ns_obj).text();

  var last_date = obj.attr('last-modification-date').value();

  props.pid = "none";

  if (ver_pid != null)
    props.pid = ver_pid.text();
  else if(obj_pid != null)
    props.pid = obj_pid.text();
	else
		console.error('No PID value found for ' + props.name + ': ' + props.escidocID);

  if(validate) {
    if(validatePIDVersion(props)) { // PID must be in format of version PID
      console.log('validated found Handle PID: ' + props.pid); // add to member
      callback(props);
    }
  } else
    callback(props);
}

// Validate HDL PIDs against the version no ref (CLARIN-DK based PID implementaton)
var validatePIDVersion = function(props, callback) {
  if(props.pid.indexOf('hdl:') == 0) {
    var sProps = props.pid.split('-');
    if(sProps.length < 5)
      return true; // object PID
    else
      return (parseInt(sProps[sProps.length-1], 16) == props.ver_no);
  } else
    return true;
}

// Parse and pull ID data from the XMLDocument
var parse = function(doc) {
  var totalRecords = doc.get('//sru-zr:numberOfRecords', ns_obj).text();
  var items = doc.find('//escidocItem:item', ns_obj);
  var containers = doc.find('//container:container', ns_obj);

  console.log('Total records in query: ' + totalRecords);

  if (utile.isArray(items)) console.log('Found ' + items.length + ' items.');
  else console.error('Found 0 items in XMLDocument.');

  if (utile.isArray(containers)) console.log('Found ' + containers.length + ' containers.');
  else console.error('Found 0 containers in XMLDocument.');

  // Add the items to list
  items.forEach(function(item){
    // CSV format: item%{escidocID}%[{objectPID}}|{lastVersionPID}]%{versionNo}
    if (!annotationsOnly) {
      getProperties(item, item.name(), addMember);
    }

		if(argv.a && item != null){
      var relations = item.find('relations:relations/relations:relation', ns_obj);
      relations.forEach(function(relation) {
        if (relation != undefined || relation != null) {
          relation_type = relation.attr('predicate').value();
          if (relation_type.indexOf('HasAnnotation') != -1) {
            var relation_href = relation.attr('href').value();
            var relationObjID = relation_href.substring(relation_href.indexOf('dkclarin'), relation.length);

            console.log('Found Annotation: ' + relationObjID);

            // retrieve properties for Annotation item from eSciDoc REST
            retrieveItemProperties(relationObjID, function(annoPropsItem) {
							console.log('props xml: ' + annoPropsItem.toString());
              getProperties(annoPropsItem.root(), "annotation",
                function(props) {
                  addMember(props);
                  if(items.indexOf(item) >= items.length)
                    if(Number(totalRecords) < (srw_config.limit + srw_config.start))
                      if(argv.f == 'json')
                        createStream(function() { addMemberToFile(JSON.stringify(json_arr)); });
                }); // add annotation member to file
            });
          } else if (relation_type.indexOf('IsAnnotationOf') != -1
                     || relation_type.indexOf('isDependentOf') != -1
                     || relation_type.indexOf('hasDependent') != -1) { // add annotation member to file including parent ID (annotation)
            retrieveItemProperties(relationObjID, function(annoPropsItem) {
              getProperties(annoPropsItem.root(), "annotation", function(propsAnno) {
                getProperties(item, "item", function(props) { addMember(propsAnno, props.escidocID); });
              });
            });
          }
        }
      });
    }
  });

  // TODO: Handle containers array
  containers.forEach(function(container) {
      if(!annotationsOnly)
          getProperties(container, container.name(), addMember);
  });

  // iterate over complete SRW result set
  if(Number(totalRecords) >= (srw_config.start + srw_config.limit)) {
    srw_config.start += srw_config.limit;
    retrieveSRWResult(srw_config, function(xml) {
      parse(xml);
    });
  } else {
    if(!argv.a && argv.f == 'json')
      createStream(function() { addMemberToFile(JSON.stringify(json_arr)); }); // write JSON object to output file
    //if(stream instanceof fs.WriteStream)
      //stream.destroySoon();
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
var addMember = function(props, parentID) {
  if (argv.f == 'csv' || argv.f == 'tsv') {
    var dataArr = [props.name, props.escidocID, props.contentModelID, props.pid, props.ver_no];
    if(parentID != null)
      dataArr.push(parentID);

    addMemberToFile(dataArr.join(delimiter) + '\n');
  } else if (argv.f == 'json') // write output to JSON object
    json_arr.push({
		  type: props.name,
		  systemID: props.escidocID,
		  contentModelID: props.contentModelID,
		  PID: props.pid,
		  versionNo: props.ver_no,
      parentID: (parentID != null) ? parentID : undefined
		});
  else
    throw new Error('Unsupported format:' + argv.f);
}

// write output to file
var addMemberToFile = function(data) {
  if (stream != null)
    if (stream.write(data)) console.log('Data written: ' + data);
    else console.log('Buffer full waiting for drain..');
  else
    process.send(data);
}

var createStream = function(callback) {

  var is_json = (argv.f == 'json');
  var is_csv = (argv.f == 'csv');
  var is_tsv = (argv.f == 'tsv');

  var file_name = argv.O;
  var file_dir = argv.d;
  var file_path = null;

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
        stream.end();
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
        return callback();
      });
    }
  }

  return callback();
}

var run = function() {
   if(argv.f != "json")
   createStream(function() {
     if (argv.s || stream != null)
      retrieveSRWResult(srw_config, function(xml) {
        parse(xml);
      });
   });
   else
      retrieveSRWResult(srw_config, function(xml) {
        parse(xml);
      });

}

module.exports = run;

run();
