package org.utfsm.utils;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.ThreadLib;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.DatabaseMgr;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.jena.atlas.lib.FileOps;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.ThreadLib;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Misc tests for TestBinaryTree. */
public class TestBinaryTree {
    // Safe on MS Windows - different directories for abort1 and abort2.
//    static String DIR1 = "target/queries_test.csv";
//    static String DIR2 = "target/tdb-testing/DB_2";

    @BeforeClass
    public static void beforeClass() {
//        FileOps.ensureDir(DIR1);
//        FileOps.ensureDir(DIR2);
    }

    @AfterClass
    public static void afterClass() {
//        try {
//            FileOps.clearAll(DIR1);
////            FileOps.clearAll(DIR2);
//            FileOps.deleteSilent(DIR1);
////            FileOps.deleteSilent(DIR2);
//        } catch (Exception ex) {}
    }

    // JENA-1746 : tests abort1 and abort2.
    // Inlines are not relevant.
    // Errors that can occur:
    //   One common term -> no conversion.
    //   Two common terms -> bad read.

    @Test
    public void dephTree() {
        BinaryTree<Integer> tree = new BinaryTree<Integer>() {
            @Override
            public void defineDataJoinNode(Node<Integer> node) {
                node.data = -1;
            }
            @Override
            public String printDataJoin(Node<Integer> node) {
                return node.data.toString();
            }
            @Override
            public String printLeafDataNode(Node<Integer> node) {
                return node.data.toString();
            }
        };
        ArrayList<Integer> lista = new ArrayList<>();
        lista.add(6);
        lista.add(7);
        lista.add(5);
        tree.addNodeList(lista);
        assertEquals(
                "[ \"-1\" ,\n  [ \"-1\" ,\n    [ \"6\" ] ,\n    [ \"7\" ]\n  ] ,\n  [ \"5\" ]\n]",
                tree.toString());

    }
    @Test
    public void singleNode() {
        BinaryTree<Integer> tree = new BinaryTree<Integer>() {
            @Override
            public void defineDataJoinNode(Node<Integer> node) {
                node.data = -1;
            }
            @Override
            public String printDataJoin(Node<Integer> node) {
                return node.data.toString();
            }
            @Override
            public String printLeafDataNode(Node<Integer> node) {
                return node.data.toString();
            }
        };
        ArrayList<Integer> lista = new ArrayList<>();
        lista.add(6);
        tree.addNodeList(lista);
        assertEquals("[ \"6\" ]", tree.toString());
//        System.out.println(tree);

    }
}
