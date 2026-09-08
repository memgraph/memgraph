from typing import Dict, Union

import mgp
from mage.llm_util.parameters import OutputType, Parameter


class SchemaGenerator(object):
    def __init__(self, context, output_type):
        self._type = output_type.lower()
        self._node_counter = 0
        self._all_node_properties_dict = {}
        self._all_relationship_properties_dict = {}
        self._all_relationships_list = []

        self._generate_schema(context)

    @property
    def type(self):
        return self._type

    @property
    def node_counter(self):
        return self._node_counter

    def get_schema(self) -> Union[mgp.Map, str]:
        if self._type == OutputType.RAW.value:
            return self._get_raw_schema()
        elif self._type == OutputType.PROMPT_READY.value:
            return self._get_prompt_ready_schema()
        else:
            raise Exception(
                f"Can't generate a graph schema since the provided output_type is not correct. Please choose {OutputType.RAW.value} or {OutputType.PROMPT_READY.value}."
            )

    def _generate_schema(self, context: mgp.ProcCtx):
        for node in context.graph.vertices:
            self._node_counter += 1

            labels = tuple(sorted(label.name for label in node.labels))

            for label in labels:
                self._update_properties_dict(node, self._all_node_properties_dict, label)

            for relationship in node.out_edges:
                target_labels = tuple(sorted(label.name for label in relationship.to_vertex.labels))

                self._update_all_relationships_list(labels, relationship, target_labels)

                self._update_properties_dict(
                    relationship,
                    self._all_relationship_properties_dict,
                    relationship.type.name,
                )

    def _update_all_relationships_list(
        self,
        start_labels: tuple[str],
        relationship: mgp.Edge,
        target_labels: tuple[str],
    ):
        for start_label in start_labels:
            for target_label in target_labels:
                full_relationship = {
                    Parameter.START.value: start_label,
                    Parameter.TYPE.value: relationship.type.name,
                    Parameter.END.value: target_label,
                }
                if full_relationship not in self._all_relationships_list:
                    self._all_relationships_list.append(full_relationship)

    def _get_raw_schema(self) -> mgp.Map:
        return {
            Parameter.NODE_PROPS.value: self._all_node_properties_dict,
            Parameter.REL_PROPS.value: self._all_relationship_properties_dict,
            Parameter.RELATIONSHIPS.value: self._all_relationships_list,
        }

    def _get_prompt_ready_schema(self) -> str:
        prompt_ready_schema = "Node properties are the following:\n"
        for label in self._all_node_properties_dict.keys():
            prompt_ready_schema += "Node name: '{label}', Node properties: {properties}\n".format(
                label=label,
                properties=sorted(
                    self._all_node_properties_dict.get(label),
                    key=lambda prop: prop[Parameter.PROPERTY.value],
                ),
            )

        prompt_ready_schema += "\nRelationship properties are the following:\n"
        for rel in self._all_relationship_properties_dict.keys():
            prompt_ready_schema += "Relationship name: '{name}', Relationship properties: {properties}\n".format(
                name=rel,
                properties=sorted(
                    self._all_relationship_properties_dict.get(rel),
                    key=lambda prop: prop[Parameter.PROPERTY.value],
                ),
            )

        prompt_ready_schema += "\nThe relationships are the following:\n"

        for relationship in self._all_relationships_list:
            prompt_ready_schema += f"['(:{relationship[Parameter.START.value]})-[:{relationship[Parameter.TYPE.value]}]->(:{relationship[Parameter.END.value]})']\n"

        return prompt_ready_schema

    def _update_properties_dict(
        self,
        graph_object: Union[mgp.Vertex, mgp.Edge],
        all_properties_dict: Dict[str, str],
        key: str,
    ):
        for property_name in graph_object.properties.keys():
            if not all_properties_dict.get(key):
                all_properties_dict[key] = [
                    {
                        Parameter.PROPERTY.value: property_name,
                        Parameter.TYPE.value: type(graph_object.properties.get(property_name)).__name__,
                    }
                ]
                continue

            if property_name in [d[Parameter.PROPERTY.value] for d in all_properties_dict[key]]:
                continue

            all_properties_dict[key].append(
                {
                    Parameter.PROPERTY.value: property_name,
                    Parameter.TYPE.value: type(graph_object.properties.get(property_name)).__name__,
                }
            )


@mgp.read_proc
def schema(
    context: mgp.ProcCtx,
    output_type: str = OutputType.PROMPT_READY.value,
) -> mgp.Record(schema=mgp.Any):
    """
    Procedure to generate the graph database schema in a prompt-ready or raw format.

    Args:
        context (mgp.ProcCtx): Reference to the context execution.
        output_type (str): By default (set to 'prompt_ready'), the graph schema will include additional context and it will be prompt-ready. If set to 'raw', it will produce a simpler version that can be adjusted for the prompt.

    Returns:
        schema (mgp.Any): `str` containing prompt-ready graph schema description in a format suitable for large language models (LLMs), or `mgp.List` containing information on graph schema in raw format which can customized for LLMs.

    Example:
        Get prompt-ready graph schema:
            `CALL llm_util.schema() YIELD schema RETURN schema;`
            or
            `CALL llm_util.schema('prompt_ready') YIELD schema RETURN schema;`
        Get raw graph schema:
            `CALL llm_util.schema('raw') YIELD schema RETURN schema;`
    """

    schema_generator = SchemaGenerator(context, output_type)

    if schema_generator.node_counter == 0:
        raise Exception("Can't generate a graph schema since there is no data in the database.")

    return mgp.Record(schema=schema_generator.get_schema())
