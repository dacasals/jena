// 
// Decompiled by Procyon v0.5.36
// 

package cl.uc.ing.benchmark.tdb2;

import org.apache.jena.query.*;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb.TDBFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.TransactionalNull;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.tdb2.TDB2Factory;

public class Find_TI3
{
    private static final int TOTAL_QUERIES = 50;
    private static final int MINUTES_TIMEOUT = 5;
    private static Random random;
    private static int timeouts;
    
    static {
        Find_TI3.random = new Random();
        Find_TI3.timeouts = 0;
    }
    
    public static void main(final String[] args) {
        JenaSystem.init();
        ARQ.init();
        Location location = Location.create(args[0]);
final Dataset ds = TDB2Factory.connectDataset(location);
        final List<String> startProperties = new ArrayList<String>();
        try {
            Throwable t = null;
            try {
                final BufferedReader br = new BufferedReader(new FileReader(args[1]));
                try {
                    String line;
                    while ((line = br.readLine()) != null) {
                        startProperties.add(line.trim());
                    }
                }
                finally {
                    if (br != null) {
                        br.close();
                    }
                }
            }
            finally {
//                if (t == null) {
//                    final Throwable exception;
//                    t = exception;
//                }
//                else {
//                    final Throwable exception;
//                    if (t != exception) {
//                        t.addSuppressed(exception);
//                    }
//                }
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e2) {
            e2.printStackTrace();
        }
        for (int propertiesFound = 0; propertiesFound < Integer.parseInt(args[2]); ++propertiesFound) {
            final int randomIndex = Find_TI3.random.nextInt(startProperties.size());
            final String randomStartProperty = startProperties.get(randomIndex);
            Transactional transactional = (ds != null && ds.supportsTransactions()) ? ds : new TransactionalNull();
            Txn.executeRead(transactional, () -> {
                final String[] properties = tryGetTI3(ds, randomStartProperty);
                if (properties != null) {
                    String[] array;
                    for (int length = (array = properties).length, i = 0; i < length; ++i) {
                        final String prop = array[i];
                        System.out.print(String.valueOf(prop) + " ");
                    }
                    System.out.println("");
                }

            });

        }
        System.out.printf("timeouts: %s%n", Find_TI3.timeouts);
    }
    
    private static String[] tryGetTI3(final Dataset ds, final String startProperty) {
        final String queryStr1 = String.format("SELECT DISTINCT ?p1 WHERE {?y %s ?x . ?z ?p1 ?x . FILTER(%s != ?p1) }", startProperty, startProperty);
        final Query query1 = QueryFactory.create(queryStr1);
        final QueryExecution qexec1 = QueryExecutionFactory.create(query1, ds);
        qexec1.setTimeout(300000L);
        try {
            final List<String> p1List = new ArrayList<String>();
            final ResultSet rs1 = qexec1.execSelect();
            final Var p1 = Var.alloc("p1");
            while (rs1.hasNext()) {
                final Binding binding = rs1.nextBinding();
                final String p1Value = String.format("<%s>", binding.get(p1).toString());
                p1List.add(p1Value);
            }
            if (p1List.size() > 0) {
                final int randomP1Index = Find_TI3.random.nextInt(p1List.size());
                final String randomP1 = p1List.get(randomP1Index);
                final String queryStr2 = String.format("SELECT DISTINCT ?p2 WHERE {?y %s ?x . ?z %s ?x . ?u ?p2 ?x . FILTER(%s != ?p2) . FILTER(%s != ?p2) }", startProperty, randomP1, startProperty, randomP1);
                final Query query2 = QueryFactory.create(queryStr2);
                final QueryExecution qexec2 = QueryExecutionFactory.create(query2, ds);
                qexec2.setTimeout(300000L);
                try {
                    final List<String> p2List = new ArrayList<String>();
                    final ResultSet rs2 = qexec2.execSelect();
                    final Var p2 = Var.alloc("p2");
                    while (rs2.hasNext()) {
                        final Binding binding2 = rs2.nextBinding();
                        final String p2Value = String.format("<%s>", binding2.get(p2).toString());
                        p2List.add(p2Value);
                    }
                    if (p2List.size() > 0) {
                        final int randomP2Index = Find_TI3.random.nextInt(p2List.size());
                        final String randomP2 = p2List.get(randomP2Index);
                        final String[] res = { startProperty, randomP1, randomP2 };
                        System.out.println("SUCCESS\t"+ queryStr2);
                        return res;
                    }
                    System.err.printf("no results fase 2 (%s, %s)%n", startProperty, randomP1);
                    return null;
                }
                catch (QueryCancelledException e) {
                    ++Find_TI3.timeouts;
                    System.err.printf("timeout fase 2 (timeout numero %d)%n", Find_TI3.timeouts);
                    return null;
                }
                finally {
                    qexec2.close();
                }
            }
            System.err.printf("no results fase 1 (%s)%n", startProperty);
            return null;
        }
        catch (QueryCancelledException e2) {
            ++Find_TI3.timeouts;
            System.err.printf("timeout fase 1 (timeout numero %d)%n", Find_TI3.timeouts);
            return null;
        }
        finally {
            qexec1.close();
        }
    }
}
