/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.fuseki.embedded;

import static org.apache.jena.fuseki.embedded.TestEmbeddedFuseki.dataset ;
import static org.apache.jena.fuseki.embedded.TestEmbeddedFuseki.query ;
import static org.junit.Assert.assertEquals ;
import static org.junit.Assert.assertTrue ;

import java.io.OutputStream ;

import org.apache.http.HttpEntity ;
import org.apache.http.entity.ContentProducer ;
import org.apache.http.entity.EntityTemplate ;
import org.apache.jena.atlas.web.ContentType ;
import org.apache.jena.fuseki.FusekiException ;
import org.apache.jena.graph.Graph ;
import org.apache.jena.query.ResultSet ;
import org.apache.jena.query.ResultSetFormatter ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.RDFFormat ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.junit.Test ;

public class TestMultipleEmbedded {
    
    static Quad q1 = SSE.parseQuad("(_ :s :p 1)") ;
    static Quad q2 = SSE.parseQuad("(_ :s :p 2)") ;
    
    @Test(expected=FusekiException.class)
    public void multiple_01() {
        DatasetGraph dsg = dataset() ;

        FusekiEmbeddedServer server1 = FusekiEmbeddedServer.create().setPort(9900).add("/ds1", dsg).build() ;
        // Bad.
        FusekiEmbeddedServer server2 = FusekiEmbeddedServer.create().setPort(9900).add("/ds2", dsg).build() ;
    
        server1.start();
        
        try {
            server2.start();
        } catch (FusekiException ex) {
            assertTrue(ex.getCause() instanceof java.net.BindException ) ;
            throw ex ;
        } finally {
            try { server1.stop() ; } catch (Exception ex) {}
            try { server2.stop() ; } catch (Exception ex) {}
        }
    }

    @Test
    public void multiple_02() {
        DatasetGraph dsg = dataset() ;
        FusekiEmbeddedServer server1 = FusekiEmbeddedServer.create().setPort(9900).add("/ds1", dsg).build() ;
        // Good
        FusekiEmbeddedServer server2 = FusekiEmbeddedServer.create().setPort(9901).add("/ds2", dsg).build() ;

        try {
            server1.start();
            server2.start();
        } finally {
            try { server1.stop() ; } catch (Exception ex) {}
            try { server2.stop() ; } catch (Exception ex) {}
        }
    }
    
    @Test
    public void multiple_03() {
        DatasetGraph dsg1 = dataset() ;
        DatasetGraph dsg2 = dataset() ;
        // Same name.
        FusekiEmbeddedServer server1 = FusekiEmbeddedServer.create().setPort(9900).add("/ds", dsg1).build().start() ;
        Txn.executeWrite(dsg1, ()->dsg1.add(q1));
        FusekiEmbeddedServer server2 = FusekiEmbeddedServer.create().setPort(9901).add("/ds", dsg2).build().start() ;
        Txn.executeWrite(dsg2, ()->dsg2.add(q2));
        query("http://localhost:9900/ds/", "SELECT * {?s ?p 1}", qExec->{
            ResultSet rs = qExec.execSelect() ; 
            int x = ResultSetFormatter.consume(rs) ;
            assertEquals(1, x) ;
        }) ;
        query("http://localhost:9901/ds/", "SELECT * {?s ?p 1}", qExec->{
            ResultSet rs = qExec.execSelect() ; 
            int x = ResultSetFormatter.consume(rs) ;
            assertEquals(0, x) ;
        }) ;
        server1.stop();
        
        query("http://localhost:9901/ds/", "SELECT * {?s ?p 2}", qExec->{
            ResultSet rs = qExec.execSelect() ; 
            int x = ResultSetFormatter.consume(rs) ;
            assertEquals(1, x) ;
        }) ;
        server2.stop();
    }
    
    @Test
    public void multiple_04() {
        DatasetGraph dsg = dataset() ;
        // Same name.
        FusekiEmbeddedServer server1 = FusekiEmbeddedServer.create().setPort(9900).add("/ds", dsg).build().start() ;
        Txn.executeWrite(dsg, ()->dsg.add(q1));
        FusekiEmbeddedServer server2 = FusekiEmbeddedServer.create().setPort(9901).add("/ds", dsg).build().start() ;
        Txn.executeWrite(dsg, ()->dsg.add(q2));
        
        query("http://localhost:9900/ds/", "SELECT * {?s ?p ?o}", qExec->{
            ResultSet rs = qExec.execSelect() ; 
            int x = ResultSetFormatter.consume(rs) ;
            assertEquals(2, x) ;
        }) ;
        server1.stop();
        server2.stop();
    }

    /** Create an HttpEntity for the graph */  
    protected static HttpEntity graphToHttpEntity(final Graph graph) {
        final RDFFormat syntax = RDFFormat.TURTLE_BLOCKS ;
        ContentProducer producer = new ContentProducer() {
            @Override
            public void writeTo(OutputStream out) {
                RDFDataMgr.write(out, graph, syntax) ;
            }
        } ;
        EntityTemplate entity = new EntityTemplate(producer) ;
        ContentType ct = syntax.getLang().getContentType() ;
        entity.setContentType(ct.getContentType()) ;
        return entity ;
    }
}

