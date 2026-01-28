// Create hierarchy of categories from Library of Congress Subject Headings
CREATE (c:LCSHTopic { authoritativeLabel: "Crystallography", dbLabel: "Crystallography", identifier: "sh 85034498" })
CREATE (po:LCSHTopic { authoritativeLabel: "Physical optics", dbLabel: "PhysicalOptics", identifier: "sh 85095187" })
CREATE (s:LCSHTopic { authoritativeLabel: "Solids", dbLabel: "Solids", identifier: "sh 85124647" })
CREATE (co:LCSHTopic { authoritativeLabel: "Crystal optics", dbLabel: "CrystalOptics", identifier: "sh 85034488" })
CREATE (cr:LCSHTopic { authoritativeLabel: "Crystals", dbLabel: "Crystals", identifier: "sh 85034503" })
CREATE (d:LCSHTopic { authoritativeLabel: "Dimorphism (Crystallography)", dbLabel: "DimorphismCrystallography", identifier: "sh 2007001101" })
CREATE (i:LCSHTopic { authoritativeLabel: "Isomorphism (Crystallography)", dbLabel: "IsomorphismCrystallography", identifier: "sh 85068653" })

// Create hierarchy relationships
CREATE (c)<-[:NARROWER_THAN]-(co)-[:NARROWER_THAN]->(po)
CREATE (c)<-[:NARROWER_THAN]-(cr)-[:NARROWER_THAN]->(s)
CREATE (c)<-[:NARROWER_THAN]-(d)
CREATE (c)<-[:NARROWER_THAN]-(i)

// Create books with category relationships
CREATE (b1:Book { title: "Crystals and light", identifier: "2167673"})
CREATE (b2:Book { title: "Optical crystallography", identifier: "11916857"})
CREATE (b3:Book { title: "Isomorphism in minerals", identifier: "8096161"})
CREATE (b4:Book { title: "Crystals and life", identifier: "12873809"})
CREATE (b5:Book { title: "Highlights in applied mineralogy", identifier: "20234576"})

CREATE (b1)-[:HAS_SUBJECT]->(co)
CREATE (b2)-[:HAS_SUBJECT]->(co)
CREATE (b3)-[:HAS_SUBJECT]->(i)
CREATE (b4)-[:HAS_SUBJECT]->(cr)
CREATE (b5)-[:HAS_SUBJECT]->(cr)
