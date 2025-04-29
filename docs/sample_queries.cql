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

//BIG FCDO Participation Network

// three tier participation chain
MATCH p = (fcdo:PublishedOrganisation {organisationidentifier:'GB-GOV-1'})
          -[:PUBLISHES]->(a1:PublishedActivity)
          -[r1:ACTIVITY_PARTICIPATION]-(a2:PublishedActivity|PhantomActivity)
          -[r2:ACTIVITY_PARTICIPATION]-(a3:PublishedActivity|PhantomActivity)

// tire 1 funding
OPTIONAL MATCH (a1)-[f1:FUNDS]-(a2)
OPTIONAL MATCH (a1)-[f1:FUNDS]-(a3)

// tier 2 publisher
MATCH q=(pub2:PublishedOrganisation)-[p2:PUBLISHES]->(a2:PublishedActivity|PhantomActivity)
// tier 2 funding nearest neighbour
OPTIONAL MATCH qf=((a2)-[f2:FUNDS]-(a2r:PublishedActivity|PhantomActivity))

// tier 3 publisher
MATCH r=(pub3:PublishedOrganisation)-[p3:PUBLISHES]->(a3:PublishedActivity|PhantomActivity)
// tier 3 funding nearest neighbour
OPTIONAL MATCH rf=(a3)-[f3:FUNDS]-(a3r:PublishedActivity|PhantomActivity)


WHERE a1 <> a2 AND a2 <> a3 
// ordering by length adds overhead
RETURN p, q, r, qf, rf order by length(q) desc

// without ordering we can go to 5k on local setup
LIMIT 1000
