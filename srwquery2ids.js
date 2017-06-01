#!/usr/bin/env node

var http = require('http'),
    fs = require('fs'),
    path = require('path'),
    utile = require('utile'),
    libxml = require('libxmljs'),
    Q = require('q');

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

//console.log('include all: ' + argv.i);
//console.log('include anno: ' + argv.a);
//console.log('validate: ' + validate);

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
    props.parent = obj.parent();
    props.name = (name == null) ? obj.name() : name;

    var xpathRoot = '//';
    if (props.name == 'item' || props.name == 'annotation') xpathRoot = 'escidocItem:properties/'
    else if (props.name == 'container') xpathRoot = 'container:properties/';

    var escidocID_href = obj.attr('href').value();
    props.escidocID = escidocID_href.substring(escidocID_href.indexOf('dkclarin'), escidocID_href.length);

    if (props.escidocID.indexOf('properties') != -1)
        props.escidocID = props.escidocID.substring(0, props.escidocID.indexOf('properties') - 1);

    var contentModelID_href = obj.get(xpathRoot + 'srel:content-model', ns_obj).attr('href').value();
    props.contentModelID = contentModelID_href.substring(contentModelID_href.indexOf('dkclarin'), contentModelID_href.length);

    console.log('xpathRoot: ', xpathRoot);

    var obj_pid = obj.get(xpathRoot + 'prop:pid', ns_obj);
    var ver_pid = obj.get(xpathRoot + 'prop:version/version:pid', ns_obj);
    props.ver_no = obj.get(xpathRoot + 'prop:latest-version/version:number', ns_obj).text();

    var last_date = obj.attr('last-modification-date').value();

    props.pid = "none";

    if (ver_pid != null)
        props.pid = ver_pid.text();
    else if (obj_pid != null)
        props.pid = obj_pid.text();
    else
        console.error('No PID value found for ' + props.name + ': ' + props.escidocID);

    if (validate) {
        if (validatePIDVersion(props)) { // PID must be in format of version PID
            console.log('validated found Handle PID: ' + props.pid); // add to member
            return props;
        } else
            throw new Error('Invalid PID handle: ' + props.pid);
    } else
        return props;
}

// Validate HDL PIDs against the version no ref (CLARIN-DK based PID implementaton)
var validatePIDVersion = function(props, callback) {
    if (props.pid.indexOf('hdl:') == 0) {
        var sProps = props.pid.split('-');
        if (sProps.length < 5)
            return true; // object PID
        else
            return (parseInt(sProps[sProps.length - 1], 16) == props.ver_no);
    } else
        return true;
}

// Parse and pull ID data from the XMLDocument
var parse = function(doc) {
    var promises = [];
    var totalRecords = doc.get('//sru-zr:numberOfRecords', ns_obj).text();
    var items = doc.find('//escidocItem:item', ns_obj);
    var containers = doc.find('//container:container', ns_obj);

    console.log('Total records in query: ' + totalRecords);

    if (utile.isArray(items))
        console.log('Found ' + items.length + ' items.');
    else
        console.error('Found 0 items in XMLDocument.');

    if (utile.isArray(containers))
        console.log('Found ' + containers.length + ' containers.');
    else
        console.error('Found 0 containers in XMLDocument.');

    //Add containers to the list
    for (var i = 0; i < containers.length; i++) {
        var container = containers[i];
        return Q.when(getProperties(container, container.name()), function(props) {
            console.log('retrieved container props data', props.escidocID);
            addMember(props);
        });
    }

    // Add the items to list
    // CSV format: item%{escidocID}%[{objectPID}}|{lastVersionPID}]%{versionNo}
    for (var i = 0; i < items.length; i++) {
        var item = items[i];
        if (!annotationsOnly) {
            Q.when(getProperties(item, item.name()), function(props) {
                console.log('retrieved item props data', props.escidocID);
                return addMember(props);
            });
        }

        if (argv.a && item != null) {
            var relations = item.find('relations:relations/relations:relation', ns_obj);

            relations.forEach(function(relation) {
                if (relation != undefined || relation != null) {
                    var relation_type = relation.attr('predicate').value();
                    var relation_href = relation.attr('href').value();
                    var relationObjID = relation_href.substring(relation_href.indexOf('dkclarin'), relation.length);
                    var promise;

                    if (relation_type.indexOf('HasAnnotation') != -1) {
                        // retrieve properties for Annotation item from eSciDoc REST
                        console.log('Found Annotation: ' + relationObjID);

                        promise = retrieveItemProperties(relationObjID).then(function(annoPropsItem) {
                            var props = getProperties(annoPropsItem.root(), "annotation");
                            console.log('retrieved annotation props data', props.escidocID);
                            //console.log('props xml: ' + annoPropsItem.toString());
                            return props;
                        }).then(function(props) {
                            return addMember(props); // add annotation member to file
                        });

                    } else if (relation_type.indexOf('IsAnnotationOf') != -1 ||
                        relation_type.indexOf('isDependentOf') != -1 ||
                        relation_type.indexOf('hasDependent') != -1) { // add annotation member to file including parent ID (annotation)

                        promise = retrieveItemProperties(relationObjID).then(function(annoPropsItem) {
                            var props = getProperties(annoPropsItem.root(), "annotation");
                            console.log('retrieved annotation props data', props.escidocID);
                            return props;
                        }).then(function(propsAnno) {
                            var props = getProperties(item, "item");
                            console.log('retrieved item props data', props.escidocID);
                            return props;
                        }).then(function(props) {
                            return addMember(propsAnno, props.escidocID);
                        });
                    }
                }

                if (promise)
                    promises.push(promise);
            });
        }
    }

    return Q.allSettled(promises).done(function() {
        // iterate over complete SRW result set
        if (Number(totalRecords) >= (srw_config.start + srw_config.limit)) {
            srw_config.start += srw_config.limit;
            return retrieveSRWResult(srw_config)
                .then(function(result) {
                    parse(result);
                });
        } else if (argv.f == 'json') {
            return addMemberToFile(JSON.stringify(json_arr)); // write JSON object to output file
        }
    });
}

/** SRW Query fetch */
var retrieveSRWResult = function(config) {
    return execGET(srw_options(argv.q, config));
}

/* Item Properties */
var retrieveItemProperties = function(escidocID) {
    return execGET(ir_options(escidocID, '/properties'));
}

var execGET = function(options) {
    var deferred = Q.defer();

    http.get(options, function(res) {
        var str = "";
        res.on('data', function(chunk) {
            str += chunk;
        });
        res.on('end', function() {
            if (res.statusCode == "200")
                deferred.resolve(libxml.parseXmlString(str));
            else
                throw new Error("Error code: " + res.statusCode);
        });
    }).on('error', function(e) {
        deferred.reject(e);
    });

    return deferred.promise;
}

/** SRW query options */
var srw_options = function(queryCQL, record_vals) {
    var search_path_limits = "&maximumRecords=" + record_vals.limit + "&startRecord=" + record_vals.start;
    var targetUrl = argv.h;
    var query = (queryCQL != null && queryCQL.length > 0) ? encodeURIComponent(queryCQL.replace(/\s/g, "%20")) + search_path_limits : '';
    query += '&d=' + Date.now();

    return {
        host: (targetUrl.indexOf('http://') != -1) ? targetUrl.replace('http://', '') : targetUrl,
        path: '/srw/search/escidoc_all?query=' + query
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
        if (parentID != null)
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
        if (stream.write(data))
            console.log('Data written: ' + data);
        else
            console.log('Buffer full waiting for drain..');
    else
        process.send({
            data: data
        });
}

var createStream = function() {
    var is_json = (argv.f == 'json');
    var is_csv = (argv.f == 'csv');
    var is_tsv = (argv.f == 'tsv');

    var file_name = argv.O;
    var file_dir = argv.d;
    var file_path = null;

    if (!argv.s) {
        if (is_json)
            file_path = path.join(file_dir, file_name + '.json');
        else if (is_csv)
            file_path = path.join(file_dir, file_name + '.csv');
        else if (is_tsv)
            file_path = path.join(file_dir, file_name + '.tsv');

        if (stream == null) {
            var deferred = Q.defer();

            stream = fs.createWriteStream(file_path, {
                encoding: 'utf8'
            });

            stream.on('open', function(fd) {
                console.log('opened file: ' + file_path);
                deferred.resolve(file_path);
            });

            stream.on('error', function(err) {
                deferred.reject(err);
            });

            stream.on('drain', function() {
                console.log('Data drained from buffer to file:' + file_path);
                stream.end();
            });

            stream.on('close', function() {
                console.log('Closing file stream.');
            });

            return deferred.promise;
        }
    }
}

var createDir = function(file_dir) {
    if (!fs.existsSync(file_dir)) {
        console.log('Target directory doesn\'t exist: ' + file_dir);
        return Q.nfcall(fs.mkdir, file_dir);
    }
}

module.exports = function() {
    return Q.all([createDir(argv.d), createStream()])
        .then(function() {
            console.log('Using query: ' + argv.q);
            return retrieveSRWResult(srw_config);
        }).then(function(result) {
            return parse(result);
        }).catch(function(err) {
            console.error(err);
        }).done();
}();
