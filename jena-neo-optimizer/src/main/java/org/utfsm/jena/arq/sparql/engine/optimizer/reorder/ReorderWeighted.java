package org.utfsm.jena.arq.sparql.engine.optimizer.reorder;

import org.apache.jena.sparql.engine.optimizer.StatsMatcher;
import org.apache.jena.sparql.engine.optimizer.reorder.PatternTriple;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformationSubstitution;

public class ReorderWeighted extends ReorderTransformationSubstitution {
    private StatsMatcher stats ;

    public ReorderWeighted(StatsMatcher stats)
    {
        this.stats = stats ;
    }

    @Override
    protected double weight(PatternTriple pTriple)
    {
        return stats.match(pTriple) ;
    }
}
