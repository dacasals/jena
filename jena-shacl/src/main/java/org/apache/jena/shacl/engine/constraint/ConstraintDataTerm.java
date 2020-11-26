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

package org.apache.jena.shacl.engine.constraint;

import java.util.Set;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.shacl.engine.ValidationContext;
import org.apache.jena.shacl.parser.Constraint;
import org.apache.jena.shacl.parser.Shape;
import org.apache.jena.shacl.validation.ReportItem;
import org.apache.jena.sparql.path.Path;

/** A restriction on a single RDF term which needs access to the data to check such as sh:class. */
public abstract class ConstraintDataTerm implements Constraint {

    @Override
    final
    public void validateNodeShape(ValidationContext vCxt, Graph data, Shape shape, Node focusNode) {
        applyConstraintDataTerm(vCxt, shape, data, focusNode, null, focusNode);
    }

    @Override
    final
    public void validatePropertyShape(ValidationContext vCxt, Graph data, Shape shape, Node focusNode, Path path, Set<Node> pathNodes) {
        pathNodes.forEach(x->applyConstraintDataTerm(vCxt, shape, data, focusNode, path, x));
    }

    private void applyConstraintDataTerm(ValidationContext vCxt, Shape shape, Graph data, Node focusNode, Path path, Node term) {
        ReportItem item = validate(vCxt, data, term);
        if ( item == null )
            return;
        vCxt.reportEntry(item.getMessage(), shape, focusNode, path, item.getValue(), this);
    }

    public abstract ReportItem validate(ValidationContext vCxt, Graph data, Node n) ;
}
