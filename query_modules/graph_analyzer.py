import sys
import mgp
from collections import OrderedDict
from itertools import chain, repeat
from inspect import cleandoc
from typing import List, Tuple
try:
    import networkx as nx
except ImportError as import_error:
    sys.stderr.write((
        '\n'
        'NOTE: Please install networkx to be able to use graph_analyzer '
        'module. Using Python:\n'
        + sys.version +
        '\n'))
    raise import_error
# Imported last because it also depends on networkx.
from mgp_networkx import MemgraphMultiDiGraph  # noqa E402


_MAX_LIST_SIZE = 10


@mgp.read_proc
def help() -> mgp.Record(name=str, value=str):
    '''Shows manual page for graph_analyzer.'''
    records = []

    def make_records(name, doc):
        return (mgp.Record(name=n, value=v) for n, v in
                zip(chain([name], repeat('')), cleandoc(doc).splitlines()))

    for func in (help, analyze, analyze_subgraph):
        records.extend(make_records("Procedure '{}'".format(func.__name__),
                                    func.__doc__))

    for m, v in _get_analysis_mapping().items():
        records.extend(make_records("Analysis '{}'".format(m), v.__doc__))

    return records


@mgp.read_proc
def analyze(context: mgp.ProcCtx,
            analyses: mgp.Nullable[List[str]] = None
            ) -> mgp.Record(name=str, value=str):
    '''
    Shows graph information.

    In case of multiple results, only the first 10 will be shown.

    The optional parameter is a list of graph analyses to run.
    If NULL, all available analyses are run.

    Example call (give all information):
        CALL graph_analyzer.analyze() YIELD *;

    Example call (with parameter):
        CALL graph_analyzer.analyze(['nodes', 'edges']) YIELD *;
    '''
    g = MemgraphMultiDiGraph(ctx=context)
    recs = _analyze_graph(context, g, analyses)
    return [mgp.Record(name=name, value=value) for name, value in recs]


@mgp.read_proc
def analyze_subgraph(context: mgp.ProcCtx,
                     vertices: mgp.List[mgp.Vertex],
                     edges: mgp.List[mgp.Edge],
                     analyses: mgp.Nullable[List[str]] = None
                     ) -> mgp.Record(name=str, value=str):
    '''
    Shows subgraph information.

    In case of multiple results, only the first 10 will be shown.

    The optional parameter is a list of graph analyses to run.
    If NULL, all available analyses are run.

    Example call (give all information):
        MATCH (n)-[e]->(m) WITH
        collect(n) AS nodes,
        collect(e) AS edges
        CALL graph_analyzer.analyze_subgraph(nodes, edges) YIELD *
        RETURN name, value;

    Example call (with parameter):
        MATCH (n)-[e]->(m) WITH
        collect(n) AS nodes,
        collect(e) AS edges
        CALL graph_analyzer.analyze_subgraph(nodes, edges, ['nodes', 'edges'])
        YIELD *
        RETURN name, value;
    '''
    vertices, edges = map(set, [vertices, edges])
    g = nx.subgraph_view(
        MemgraphMultiDiGraph(ctx=context),
        lambda n: n in vertices,
        lambda n1, n2, e: e in edges)
    recs = _analyze_graph(context, g, analyses)
    return [mgp.Record(name=name, value=value) for name, value in recs]


def _get_analysis_mapping():
    return OrderedDict([
                        ('nodes', _number_of_nodes),
                        ('edges', _number_of_edges),
                        ('bridges', _bridges),
                        ('articulation_points', _articulation_points),
                        ('avg_degree', _avg_degree),
                        ('sorted_nodes_degree', _sorted_nodes_degree),
                        ('self_loops', _self_loops),
                        ('is_bipartite', _is_bipartite),
                        ('is_planar', _is_planar),
                        ('is_biconnected: ', _is_biconnected),
                        ('is_weakly_connected', _is_weakly_connected),
                        ('number_of_weakly_components', _weakly_components),
                        ('is_strongly_connected', _is_strongly_connected),
                        ('strongly_components', _strongly_components),
                        ('is_dag', _is_dag),
                        ('is_eulerian', _is_eulerian),
                        ('is_forest', _is_forest),
                        ('is_tree', _is_tree)])


def _get_analysis_func(name: str):
    _name_to_proc = _get_analysis_mapping()
    return _name_to_proc.get(name.lower())


def _get_analysis_funcs():
    return _get_analysis_mapping().values()


def _analyze_graph(context: mgp.ProcCtx,
                   g: nx.MultiDiGraph,
                   analyses: List[str]
                   ) -> List[Tuple[str, str]]:

    functions = (_get_analysis_funcs() if analyses is None
                 else [_get_analysis_func(name) for name in analyses])

    records = []
    for index, f in enumerate(functions):
        context.check_must_abort()
        if f is None:
            raise KeyError('Graph analysis is not supported: ' +
                           analyses[index])
        name, value = f(g)
        if isinstance(value, (list, set, tuple)):
            value = list(value)[:_MAX_LIST_SIZE]
        records.append((name, str(value)))

    return records


def _number_of_nodes(g: nx.MultiDiGraph) -> Tuple[str, int]:
    '''Returns number of nodes.'''
    return 'Number of nodes', nx.number_of_nodes(g)


def _number_of_edges(g: nx.MultiDiGraph) -> Tuple[str, int]:
    '''Returns number of edges.'''
    return 'Number of edges', nx.number_of_edges(g)


def _avg_degree(g: nx.MultiDiGraph) -> Tuple[str, float]:
    '''Returns average degree.'''
    _, number_of_nodes = _number_of_nodes(g)
    _, number_of_edges = _number_of_edges(g)
    avg_degree = (0 if number_of_nodes == 0
                  else number_of_edges / number_of_nodes)
    return 'Average degree', avg_degree


def _sorted_nodes_degree(g: nx.MultiDiGraph) -> Tuple[str, List[int]]:
    '''Returns list of sorted nodes degree. [(node_id, degree), ...]'''
    nodes_degree = [(n, g.degree(n)) for n in g.nodes()]
    nodes_degree.sort(key=lambda x: x[1], reverse=True)
    return 'Sorted nodes degree', nodes_degree


def _self_loops(g: nx.MultiDiGraph) -> Tuple[str, int]:
    '''Returns number of self loops.'''
    return 'Self loops', sum((1 if e[0] == e[1] else 0 for e in g.edges()))


def _is_bipartite(g: nx.MultiDiGraph) -> Tuple[str, bool]:
    '''Checks if graph is bipartite.'''
    _, number_of_nodes = _number_of_nodes(g)
    ret = (False if number_of_nodes == 0
           else nx.algorithms.bipartite.basic.is_bipartite(g))
    return 'Is bipartite', ret


def _is_planar(g: nx.MultiDiGraph) -> Tuple[str, bool]:
    '''Checks if graph is planar.'''
    _, number_of_nodes = _number_of_nodes(g)
    ret = (False if number_of_nodes == 0
           else nx.algorithms.planarity.check_planarity(g)[0])
    return 'Is planar', ret


def _is_biconnected(g: nx.MultiDiGraph) -> Tuple[str, bool]:
    '''Check if graph is biconnected.'''
    _, number_of_nodes = _number_of_nodes(g)
    ret = (False if number_of_nodes == 0
           else nx.is_biconnected(nx.MultiDiGraph.to_undirected(g)))
    return 'Is biconnected', ret


def _is_weakly_connected(g: nx.MultiDiGraph) -> Tuple[str, bool]:
    '''Check if graph is weakly connected.'''
    _, number_of_nodes = _number_of_nodes(g)
    ret = False if number_of_nodes == 0 else nx.is_weakly_connected(g)
    return 'Is weakly connected', ret


def _is_strongly_connected(g: nx.MultiDiGraph) -> Tuple[str, bool]:
    '''Checks if graph is strongly connected.'''
    _, number_of_nodes = _number_of_nodes(g)
    ret = False if number_of_nodes == 0 else nx.is_strongly_connected(g)
    return 'Is strongly connected', ret


def _is_dag(g: nx.MultiDiGraph) -> Tuple[str, bool]:
    '''Check if graph is directed acyclic graph (DAG)'''
    _, number_of_nodes = _number_of_nodes(g)
    ret = (False if number_of_nodes == 0
           else nx.algorithms.dag.is_directed_acyclic_graph(g))
    return 'Is DAG', ret


def _is_eulerian(g: nx.MultiDiGraph) -> Tuple[str, bool]:
    '''Checks if graph is Eulerian.'''
    _, number_of_nodes = _number_of_nodes(g)
    ret = (False if number_of_nodes == 0
           else nx.algorithms.euler.is_eulerian(g))
    return 'Is eulerian', ret


def _is_forest(g: nx.MultiDiGraph) -> Tuple[str, bool]:
    '''Checks if graph is forest, all components must be trees.'''
    _, number_of_nodes = _number_of_nodes(g)
    ret = (False if number_of_nodes == 0
           else nx.algorithms.tree.recognition.is_forest(g))
    return 'Is forest', ret


def _is_tree(g: nx.MultiDiGraph) -> Tuple[str, bool]:
    '''Checks if graph is tree.'''
    _, number_of_nodes = _number_of_nodes(g)
    ret = (False if number_of_nodes == 0
           else nx.algorithms.tree.recognition.is_tree(g))
    return 'Is tree', ret


def _bridges(g: nx.MultiDiGraph) -> Tuple[str, int]:
    '''Returns number of bridges, multiple edges between same nodes are
       mapped to one edge.'''
    return 'Number of bridges', sum(1 for _ in nx.bridges(nx.Graph(g)))


def _articulation_points(g: nx.MultiDiGraph):
    '''Returns number of articulation points.'''
    undirected = nx.MultiDiGraph.to_undirected(g)
    return ('Number of articulation points',
            sum(1 for _ in nx.articulation_points(undirected)))


def _weakly_components(g: nx.MultiDiGraph):
    '''Returns number of weakly components.'''
    comps = nx.algorithms.components.number_weakly_connected_components(g)
    return 'Number of weakly connected components', comps


def _strongly_components(g: nx.MultiDiGraph):
    '''Returns number of strongly connected components.'''
    comps = nx.algorithms.components.number_strongly_connected_components(g)
    return 'Number of strongly connected components', comps
