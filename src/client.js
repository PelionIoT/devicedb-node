'use strict'

/*
 * Copyright (c) 2018, Arm Limited and affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const https = require('https')
const http = require('http')
const request = require('request')
const split = require('split')
const url = require('url')
const MAX_SOCKETS = 4

class DeviceDB {
    constructor(options) {
        var self = this;
        this.serverURI = options.uri
        this.https = options.https
        if(this.https) {
            self.agent = options.agent = new https.Agent({
                keepAlive: true,
                maxSockets: MAX_SOCKETS,
                checkServerIdentity: function(servername, cert) {
                }
            })
        } else {
            self.agent = options.agent = new http.Agent({
                keepAlive: true,
                maxSockets: MAX_SOCKETS
            })
        }
        
        this.lww = new bucket('lww', options)
        this.default = new bucket('default', options)
        this.shared = this.default
        this.cloud = new bucket('cloud', options)
        this.local = new bucket('local', options)
        options.category = 'events'
        this.history = new history(options)
        options.category = 'alerts'
        this.alerts = new alertLog(options)
    }
    
    _newRequestOptions(options) {
        var _options = { };
        
        if(this.https) {
            for(let k in this.https) {
                _options[k] = this.https[k]
            }
        }
        
        for(var k in options) {
            _options[k] = options[k]
        }
        
        _options.agent = this.agent
        _options.agentOptions = {
            checkServerIdentity: function(servername, cert) {
            }
        }
        
        return _options
    }
    
    requiresAuth() {
        return Promise.resolve(false)
    }
    
    addPeer(peerID, peerAddress) {
        let parsedAddress = url.parse(peerAddress)
        
        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/peers/' + peerID),
                method: 'PUT',
                json: true,
                body: {
                    id: peerID,
                    host: parsedAddress.hostname,
                    port: parseInt(parsedAddress.port) || 443
                }
            }), function(error, response, responseBody) {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    resolve()
                }
            })
        })
    }
    
    removePeer(peerID) {
        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/peers/' + peerID),
                method: 'DELETE',
                json: true
            }), function(error, response, responseBody) {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    resolve()
                }
            })
        })
    }
    
    listPeers() {
        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/peers'),
                method: 'GET',
                json: true
            }), function(error, response, responseBody) {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    resolve(responseBody)
                }
            })
        })
    }
    
    getMerkleRoot() {
        return this.default.getMerkleRoot()
    }
        
    put(key, value, context) {
        return this.default.put(key, value, context)
    }
    
    delete(key, context) {
        return this.default.delete(key, context)
    }
    
    batch(ops) {
        return this.default.batch(ops)
    }
    
    get(key) {
        return this.default.get(key)
    }
    
    getMatches(key, next) {
        return this.default.getMatches(key, next)
    }
}

class alertLog {
    constructor(options) {
        this.history = new history(options)
    }

    raiseAlert(name, level, metadata) {
        if(typeof name !== 'string' || name.trim() == '') {
            return Promise.reject(new Error('Invalid alert name (argument 1) specified'))
        }

        return this.history.log({
            source: name,
            type: level,
            data: {
                status: true,
                metadata: metadata
            }
        })
    }

    lowerAlert(name, level, metadata) {
        if(typeof name !== 'string' || name.trim() == '') {
            return Promise.reject(new Error('Invalid alert name (argument 1) specified'))
        }

        return this.history.log({
            source: name,
            type: level,
            data: {
                status: false,
                metadata: metadata
            }
        })
    }
}

function encodeGroups(groups) {
    if(groups.length == 0) {
        return ''
    }

    let query = '&'

    for(let group of groups) {
        query += '&group=' + encodeURIComponent(group)
    }

    return query
}

class history {
    constructor(options) {
        this.serverURI = options.uri
        this.https = options.https
        this.agent = options.agent
        this.category = options.category
    }
    
    _newRequestOptions(options) {
        var _options = { };
        
        if(this.https) {
            for(let k in this.https) {
                _options[k] = this.https[k]
            }
        }
        
        for(var k in options) {
            _options[k] = options[k]
        }
        
        _options.agent = this.agent
        _options.agentOptions = {
            checkServerIdentity: function(servername, cert) {
            }
        }
        
        return _options
    }
    
    log(event) {
        if(typeof event !== 'object' || event === null) {
            return Promise.reject(new Error('No event specified'))
        }
        
        if(typeof event.source !== 'string' || event.source.length == 0) {
            return Promise.reject(new Error('event.source is empty'))
        }
        
        if(typeof event.type !== 'string' || event.type.length == 0) {
            return Promise.reject(new Error('event.type is empty'))
        }
        
        //if(typeof event.data !== 'string' && this.category != 'alerts') {
        //    event.data = ''
        //}

        if(!Array.isArray(event.groups)) {
            event.groups = [ ]
        }
        
        for(let group of event.groups) {
            if(typeof group !== 'string') {
                return Promise.reject(new Error('event.groups is not a valid string array'))
            }
        }
        
        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/events/' + event.source + '/' + event.type) + '?category=' + encodeURIComponent(this.category) + encodeGroups(event.groups),
                method: 'PUT',
                body: JSON.stringify(event.data)
            }), function(error, response, responseBody) {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    resolve()
                }
            })
        })
    }
    
    query(historyQuery, next) {
        let queryString = ''
        
        if(typeof historyQuery !== 'object' || historyQuery === null) {
            historyQuery = { }
        }
        
        if(Array.isArray(historyQuery.sources)) {
            for(let source of historyQuery.sources) {
                queryString += 'source=' + source + '&'
            }
        }
        
        if('limit' in historyQuery) {
            queryString += 'limit=' + historyQuery.limit + '&'
        }
        
        if('sortOrder' in historyQuery) {
            queryString += 'sortOrder=' + historyQuery.sortOrder + '&'
        }
        
        if('data' in historyQuery) {
            queryString += 'data=' + historyQuery.data + '&'
        }
        
        if('maxAge' in historyQuery) {
            queryString += 'maxAge=' + historyQuery.maxAge + '&'
        }
        
        if('afterTime' in historyQuery) {
            queryString += 'afterTime=' + historyQuery.afterTime + '&'
        }
        
        if('beforeTime' in historyQuery) {
            queryString += 'beforeTime=' + historyQuery.beforeTime + '&'
        }
        
        if(queryString.endsWith('&')) {
            queryString = queryString.substring(0, queryString.length - 1)
        }
        
        if(typeof next !== 'function') {
            next = function(error, result) {
                if(error) {
                    console.error(error.stack)
                }
                else {
                    console.log(result)
                }
            }
        }

        let parseNext = (line) => {
            if(line.trim() == '') {
                return
            }
            
            let event = JSON.parse(line)
            
            next(null, event)
        }
        
        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/events') + '?' + queryString,
                method: 'GET'
            }), function(error, response, responseBody) {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    resolve()
                }
            }).pipe(split(parseNext)).on('error', (error) => {
                next(error, null)
            })
        })
    }
    
    purge(purgeQuery) {
        let queryString = ''
        
        if(typeof purgeQuery !== 'object' || purgeQuery === null) {
            purgeQuery = { }
        }
        
        if('maxAge' in purgeQuery) {
            queryString += 'maxAge=' + purgeQuery.maxAge + '&'
        }
        
        if('afterTime' in purgeQuery) {
            queryString += 'afterTime=' + purgeQuery.afterTime + '&'
        }
        
        if('beforeTime' in purgeQuery) {
            queryString += 'beforeTime=' + purgeQuery.beforeTime + '&'
        }
        
        if(queryString.endsWith('&')) {
            queryString = queryString.substring(0, queryString.length - 1)
        }
        
        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/events') + '?' + queryString,
                method: 'DELETE'
            }), function(error, response, responseBody) {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    resolve()
                }
            })
        })
    }
}

class bucket {
    constructor(name, options) {
        this.bucketName = name
        this.serverURI = options.uri
        this.https = options.https
        this.agent = options.agent
    }
    
    _newRequestOptions(options) {
        var _options = { };
        
        if(this.https) {
            for(let k in this.https) {
                _options[k] = this.https[k]
            }
        }
        
        for(var k in options) {
            _options[k] = options[k]
        }
        
        _options.agent = this.agent
        _options.agentOptions = {
            checkServerIdentity: function(servername, cert) {
            }
        }
        
        return _options
    }
    
    put(key, value, context) {
        return this.batch([
            {
                type: 'put',
                key: key,
                value: value,
                context: context || ''
            }
        ])
    }
    
    delete(key, context) {
        return this.batch([
            {
                type: 'delete',
                key: key,
                context: context || ''
            }
        ])
    }
    
    getMerkleRoot() {
        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/' + this.bucketName + '/merkleRoot'),
                method: 'GET',
                json: true
            }), function(error, response, responseBody) {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    resolve(responseBody)
                }
            })
        })
    }
    
    batch(ops) {
        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/' + this.bucketName + '/batch'),
                method: 'POST',
                json: true,
                body: ops
            }), function(error, response, responseBody) {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    resolve()
                }
            })
        })
    }
    
    _fillInDBObjectValue(dbobject) {
        if(dbobject == null) {
            return
        }
        
        if(!Array.isArray(dbobject.siblings)) {
            return
        }
        
        if(dbobject.siblings.length == 1) {
            dbobject.value = dbobject.siblings[0]
        }
        else {
            dbobject.value = null
        }
    }
    
    get(key) {
        let query = [ ]
        
        if(!Array.isArray(key)) {
            query = [ key ]
        }
        else {
            query = key
        }
        
        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/' + this.bucketName + '/values'),
                method: 'POST',
                json: true,
                body: query
            }), (error, response, responseBody) => {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    for(let response of responseBody) {
                        this._fillInDBObjectValue(response)
                    }
                    
                    if(Array.isArray(key)) {
                        resolve(responseBody)
                    }
                    else {
                        resolve(responseBody[0])
                    }
                }
            });
        });
    }
    
    getMatches(key, next) {
        let query = [ ]

        if(!Array.isArray(key)) {
            query = [ key ]
        }
        else {
            query = key
        }
        
        if(typeof next !== 'function') {
            next = function(error, result) {
                if(error) {
                    console.error(error)
                }
                else {
                    console.log(result)
                }
            }
        }

        let nextSiblingSet = { }
        let stateMachine = {
            'prefix': (line) => {
                nextSiblingSet.prefix = line
                nextState = stateMachine['key']
            },
            'key': (line) => {
                nextSiblingSet.key = line
                nextState = stateMachine['siblings']
            },
            'siblings': (line) => {
                let dbobject = JSON.parse(line)
                
                if(!Array.isArray(dbobject.siblings)) {
                    throw new Error("Siblings is not an array")
                }
                
                this._fillInDBObjectValue(dbobject)
                
                nextSiblingSet.siblings = dbobject.siblings
                nextSiblingSet.value = dbobject.value
                nextSiblingSet.context = dbobject.context
                
                next(null, nextSiblingSet)
                
                nextSiblingSet = { }
                
                nextState = stateMachine['prefix']
            }
        }
        let nextState = stateMachine['prefix']
        let parseNext = (line) => {
            nextState(line)
        }

        return new Promise((resolve, reject) => {
            request(this._newRequestOptions({
                uri: url.resolve(this.serverURI, '/' + this.bucketName + '/matches'),
                method: 'POST',
                json: true,
                body: query
            }), function(error, response, responseBody) {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    resolve()
                }
            }).pipe(split(parseNext)).on('error', (error) => {
                next(error, null)
            })
        })
    }
}

module.exports = (defaults) => {
    let defaultClient = new DeviceDB(defaults);

    defaultClient.createClient = (options) => {
        if(typeof options !== 'object') {
            options = defaults
        }

        return new DeviceDB(options)
    };

    defaultClient.encodeKey = (utf8Key) => {
        return (new Buffer(utf8Key)).toString('base64')
    }

    defaultClient.decodeKey = (base64Key) => {
        return (new Buffer(base64Key, 'base64')).toString('utf8');
    }

    defaultClient.ClusterClient = require('./cloud-client')

    return defaultClient
}
