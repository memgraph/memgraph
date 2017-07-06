//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 23.02.17.
//

#pragma once

namespace traversal_template {

/**
 * Indicates the type of uniqueness used when matching data to a pattern.
 * "Uniqueness" means that a single Vertex or Edge in the result path
 * may not map to a single element in the pattern.
 *
 * This is equivalent to saying that a single Vertex or Edge may not occur
 * more then once in the resulting path.
 *
 * Often the "-morphism terminology is used, where Homomorphism means
 * there are not uniqueness constraints, "Cyphermorphism" (Neo4j's current
 * default) means Edge uniqueness, and Isomorphism means Vertex uniqueness
 * (which also implies Edge uniqueness).
 *
 * TODO: Look into the slides on uniqueness from OpenCypher implementor meeting,
 * it is mentioned that Vertex uniqueness can result in exponential performance
 * degradation. Figure out why and how.
 */
enum class Uniqueness { None, Vertex, Edge };

/**
 * Indicates how a path should be expanded using the traversal API. For the
 * given path (we ignore relationship directionality in the example because it
 * does not affect it):
 *
 *      p = (node_N)-[]-(node_N+1)-...-(node_M)
 *
 * the Expansion::Front enum means that the path is expanded from (node_N)
 * backwards, so the resulting path would be:
 *
 *      q = (node_N-1)-[]-(node_N)-[]-(node_N+1)-...-(node_M)
 *
 * The Expansion::Back enum has the opposite meaning, path p would get expanded
 * from node_M.
 *
 * Note that this implies that a Path has direction (start and finish).
 */
enum class Expansion { Front, Back };

/**
 * Indicates which relationships from the expansion vertex should be used to
 * expand
 * the path. Direction::In means that incoming relationships are used for
 * expansion.
 *
 * For example, for the given graph data:
 *
 *    (a)-[]->(b)<-[]-(c)
 *
 * And the given current path
 *
 *    p = (b)
 *
 * Expansion (let's assume Expansion::Back) in the Direction::In would result
 * in:
 *
 *    q = (b)<-[]-(a)
 */
enum class Direction { In, Out, Both };
}
