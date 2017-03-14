import steps.database

class GraphProperties: 
    """
    Class used to store changes(side effects of queries) 
    to graph parameters(nodes, relationships, labels and 
    properties) when executing queries.
    """
    def set_beginning_parameters(self):
        """
        Method sets parameters to empty lists.
        
        @param self:
            Instance of a class.
        """
        self.nodes = []
        self.relationships = []
        self.labels = []
        self.properties = []

    def __init__(self):
        """
        Method sets parameters to empty lists.
            
        @param self:
            Instance of a class.
        """
        self.nodes = []
        self.relationships = []
        self.labels = []
        self.properties = []

    def change_nodes(self, dif):
        """
        Method adds node side effect.

        @param self:
            Instance of a class.
        @param dif:
            Int, difference between number of nodes before
            and after executing query.
        """
        self.nodes.append(dif)

    def change_relationships(self, dif):
        """
        Method adds relationship side effect.

        @param self:
            Instance of a class.
        @param dif:
            Int, difference between number of relationships 
            before and after executing query.
        """
        self.relationships.append(dif)

    def change_labels(self, dif):
        """
        Method adds one label side effect.

        @param self:
            Instance of a class.
        @param dif:
            Int, difference between number of labels before
            and after executing query.
        """
        self.labels.append(dif)

    def change_properties(self, dif):
        """
        Method adds one property side effect.

        @param self:
            Instance of a class.
        @param dif:
            Int, number of properties set in query.
        """
        self.properties.append(dif)

    def compare(self, nodes_dif, relationships_dif, labels_dif, properties_dif):
        """
        Method used to compare side effects from executing
        queries and an expected result from a cucumber test.

        @param self:
            Instance of a class.
        @param nodes_dif:
            List of all expected node side effects in order
            when executing query.
        @param relationships_dif:
            List of all expected relationship side effects 
            in order when executing query.
        @param labels_dif:
            List of all expected label side effects in order
            when executing query.
        @param properties_dif:
            List of all expected property side effects in order
            when executing query.
        @return:
            True if all side effects are equal, else false.
        """
        if len(nodes_dif) != len(self.nodes):
            return False
        if len(relationships_dif) != len(self.relationships):
            return False
        if len(labels_dif) != len(self.labels):
            return False
        if len(properties_dif) != len(self.properties):
            return False

        for i in range(0, len(nodes_dif)):
            if nodes_dif[i] != self.nodes[i]:
                return False

        for i in range(0, len(relationships_dif)):
            if relationships_dif[i] != self.relationships[i]:
                return False

        for i in range(0, len(labels_dif)):
            if labels_dif[i] != self.labels[i]:
                return False

        for i in range(0, len(properties_dif)):
            if properties_dif[i] != self.properties[i]:
                return False

        return True
