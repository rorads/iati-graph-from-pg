// Note that these queries operate better with the 'connect result nodes' option disabled in the
// Neo4j browser.

//FCDO Long Chain 


MATCH
p=((act)<-[:PUBLISHES]-(dfid:PublishedOrganisation {organisationidentifier:"GB-GOV-1"})),
q=((tier1)<-[:FUNDS*10..12]-(act)),
r=(()-[:PUBLISHES]-(tier1))
RETURN p,q,r LIMIT 40