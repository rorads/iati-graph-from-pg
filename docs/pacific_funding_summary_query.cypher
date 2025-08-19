// High-level Pacific Funding Summary Graph Query
// Paste this into Neo4j Browser to visualize the summary

// Highly Connected Pacific Funding Network
// Shows major donors, their collaboration patterns, and shared focus areas

// Main network view - shows all connections
MATCH (d:MajorDonor)-[rel]-(other)
WHERE type(rel) IN ['FUNDS_COUNTRY', 'WORKS_IN_SECTOR', 'COLLABORATES_IN_REGION', 'COLLABORATES_IN_SECTORS']
RETURN d, rel, other

// Alternative views you can try:

// 1. Focus on collaboration relationships only:
// MATCH (d1:MajorDonor)-[collab]-(d2:MajorDonor)
// WHERE type(collab) IN ['COLLABORATES_IN_REGION', 'COLLABORATES_IN_SECTORS']
// RETURN d1, collab, d2

// 2. Show country-centered networks:
// MATCH (d:MajorDonor)-[f:FUNDS_COUNTRY]->(c:PacificCountry)
// WHERE f.activities >= 10
// RETURN d, f, c

// 3. Show sector collaboration hubs:
// MATCH (d:MajorDonor)-[w:WORKS_IN_SECTOR]->(s:MajorSector)
// WHERE w.activities >= 15
// RETURN d, w, s

// Alternative focused queries you can try:

// 1. Focus on major donors only (uncomment to use):
// MATCH (d:DonorSummary)-[f:FUNDS]->(r:RecipientSummary)
// WHERE d.name IN ['Australia', 'New Zealand', 'UN Agencies', 'United States', 'Japan']
// AND f.activities >= 3
// RETURN d, f, r

// 2. Focus on specific recipient areas (uncomment to use):
// MATCH (d:DonorSummary)-[f:FUNDS]->(r:RecipientSummary)-[rs:RECEIVES_IN]->(s:SectorSummary)
// WHERE r.name IN ['Vanuatu', 'Fiji', 'Solomon Islands']
// RETURN d, f, r, rs, s

// 3. Focus on collaboration patterns (uncomment to use):
// MATCH (d1:DonorSummary)-[:WORKS_IN]->(s:SectorSummary)<-[:WORKS_IN]-(d2:DonorSummary)
// WHERE d1 <> d2 AND s.name IN ['Climate/Disaster', 'Health', 'Governance']
// RETURN d1, s, d2

// Node legend:
// DonorSummary (blue): Aggregated donor countries/organisation types
// RecipientSummary (green): Pacific island nations and regions  
// SectorSummary (orange): Thematic areas of intervention

// Relationship legend:
// FUNDS: Financial flows from donors to recipients (with activity count)
// WORKS_IN: Donor engagement in sectors (with activity count)
// RECEIVES_IN: Recipient country activity in sectors (with activity count)