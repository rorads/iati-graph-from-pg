// Note that these queries operate better with the 'connect result nodes' option disabled in the
// Neo4j browser.

//FCDO Long Chain 
MATCH
p=((act)<-[:PUBLISHES]-(dfid:PublishedOrganisation {organisationidentifier:"GB-GOV-1"})),
q=((tier1)<-[:FUNDS*10..12]-(act)),
r=(()-[:PUBLISHES]-(tier1))
RETURN p,q,r LIMIT 40

//BIG FCDO Participation Network
MATCH p = (fcdo:PublishedOrganisation {organisationidentifier:'GB-GOV-1'})
          -[:PUBLISHES]->(a1:PublishedActivity)
          -[r1:ACTIVITY_PARTICIPATION]-(a2:PublishedActivity)
          -[r2:ACTIVITY_PARTICIPATION]-(a3:PublishedActivity)
MATCH q=(pub2:PublishedOrganisation)-[:PUBLISHES]->(a2)
MATCH r=(pub3:PublishedOrganisation)-[:PUBLISHES]->(a3)
WHERE a1 <> a2 AND a2 <> a3
RETURN p, q, r
LIMIT 400