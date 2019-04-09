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

const request = require('request')
const url = require('url')

class DeviceDB {
    constructor(options) {
        this.serverURI = options.uri
    }

    _server() {
        if(!Array.isArray(this.serverURI)) {
            return this.serverURI
        }

        return this.serverURI[parseInt(this.serverURI.length * Math.random())]
    }

    addRelay(relayID) {
        return new Promise((resolve, reject) => {
            request({
                uri: url.resolve(this._server(), '/relays/' + relayID),
                method: 'PUT',
                json: true,
            }, function(error, response, responseBody) {
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

    removeRelay(relayID) {
        return new Promise((resolve, reject) => {
            request({
                uri: url.resolve(this._server(), '/relays/' + relayID),
                method: 'DELETE',
                json: true,
            }, function(error, response, responseBody) {
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

    moveRelay(relayID, siteID) {
        return new Promise((resolve, reject) => {
            request({
                uri: url.resolve(this._server(), '/relays/' + relayID),
                method: 'PATCH',
                json: true,
                body: { site: siteID }
            }, function(error, response, responseBody) {
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

    addSite(siteID) {
        return new Promise((resolve, reject) => {
            request({
                uri: url.resolve(this._server(), '/sites/' + siteID),
                method: 'PUT',
                json: true,
            }, function(error, response, responseBody) {
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

    removeSite(siteID) {
        return new Promise((resolve, reject) => {
            request({
                uri: url.resolve(this._server(), '/sites/' + siteID),
                method: 'DELETE',
                json: true,
            }, function(error, response, responseBody) {
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

    site(siteID) {
        return new site({
            uri: this.serverURI,
            siteID: siteID
        })
    }
}

class site {
    constructor(options) {
        this.lww = new bucket('lww', options)
        this.default = new bucket('default', options)
        this.shared = this.default
        this.cloud = new bucket('cloud', options)
        this.local = new bucket('local', options)
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

class bucket {
    constructor(name, options) {
        this.bucketName = name
        this.serverURI = options.uri
        this.siteID = options.siteID
    }

    _server() {
        if(!Array.isArray(this.serverURI)) {
            return this.serverURI
        }

        return this.serverURI[parseInt(this.serverURI.length * Math.random())]
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
    
    batch(ops) {
        return new Promise((resolve, reject) => {
            request({
                uri: url.resolve(this._server(), '/sites/' + this.siteID + '/buckets/' + this.bucketName + '/batches'),
                method: 'POST',
                json: true,
                body: ops
            }, function(error, response, responseBody) {
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

        let queryString = ""

        for(let key of query) {
            queryString += "key=" + encodeURIComponent(key) + "&"
        }
        
        return new Promise((resolve, reject) => {
            request({
                uri: url.resolve(this._server(), '/sites/' + this.siteID + '/buckets/' + this.bucketName + '/keys?' + queryString),
                method: 'GET',
                json: true,
            }, (error, response, responseBody) => {
                if(error) {
                    reject(error)
                }
                else if(response.statusCode != 200) {
                    reject(responseBody)
                }
                else {
                    for(let response of responseBody) {
                    }

                    responseBody = responseBody.map((response) => {
                        delete response.prefix
                        this._fillInDBObjectValue(response)
                        
                        if(response.siblings == null) {
                            return null
                        }
                        else {
                            return response
                        }
                    })
                    
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

        let queryString = ""

        for(let key of query) {
            queryString += "prefix=" + encodeURIComponent(key) + "&"
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

        return new Promise((resolve, reject) => {
            request({
                uri: url.resolve(this._server(), '/sites/' + this.siteID + '/buckets/' + this.bucketName + '/keys?' + queryString),
                method: 'GET',
                json: true,
            }, (error, response, responseBody) => {
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

                    for(let response of responseBody) {
                        next(null, response)
                    }

                    resolve()
                }
            })
        })
    }
}

module.exports = DeviceDB
