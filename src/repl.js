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

const repl = require('repl')
const os = require('os')
const vm = require('vm')
const devicedb = require('./client')({ })
const argv = require('minimist')(process.argv.slice(2))
const url = require('url')
const fs = require('fs')

let https = { }

if(argv.uri) {
    try {
        let parsedURI = url.parse(argv.uri)
    }
    catch(error) {
        console.log('Invalid uri specified')
        
        process.exit(1)
    }
}
else {
    console.log('No uri specified')
    
    process.exit(1)
}

if(argv.rootCA) {
    try {
        let rootCA = fs.readFileSync(argv.rootCA, 'utf8')
        
        https.ca = rootCA.split('-----BEGIN CERTIFICATE-----').map(c => c.trim()).filter(c => c.length != 0).map(c => '-----BEGIN CERTIFICATE-----\n' + c)
    }
    catch(error) {
        console.log('Invalid root CA file specified')
        
        process.exit(1)
    }
}

if(!argv.cluster) {
    global.ddb = devicedb.createClient({ uri: argv.uri, https: https })
    global.ddb.encodeKey = devicedb.encodeKey
    global.ddb.decodeKey = devicedb.decodeKey
}
else {
    global.ddb = new devicedb.ClusterClient({ uri: argv.uri })
    global.ddb.encodeKey = devicedb.encodeKey
    global.ddb.decodeKey = devicedb.decodeKey
}

startREPL()

function startREPL() {
    repl.start({
        prompt: 'devicedb> ',
        eval: function(code, context, file, cb) {
            let err, result
            
            try {
                if(repl.useGlobal) {
                    result = vm.runInThisContext(code, file)
                } 
                else {
                    result = vm.runInContext(code, context, file)
                }
            } 
            catch(e) {
                err = e
            }

            if(err && process.domain) {
                process.domain.emit('error', err)
                process.domain.exit()
            }
            else {
                Promise.all([result]).then((result) => {
                    cb(err, result[0]);
                }, function(error) {
                    cb(err, error)
                })
            }
        }
    }).on('exit', () => {
        process.exit(0)
    })
}