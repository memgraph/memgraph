CREATE (sw:SWRelease {uid: "Release_1.0", name: "MyRelease"});
CREATE (comp:Component {uid: "Component_A", name: "ComponentA"});
CREATE (sub1:Component {uid: "SubComponent_1", name: "SubComponent1"});
CREATE (sub2:Component {uid: "SubComponent_2", name: "SubComponent2"});
CREATE (comp)-[:PACKAGED]->(sw);
CREATE (sub1)-[:HAS_PARENT]->(comp);
CREATE (sub2)-[:HAS_PARENT]->(comp);
