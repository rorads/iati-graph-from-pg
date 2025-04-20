# TODO

- ✅ **[COMPLETED]** ~~**Graph Structure Note:** The current Neo4j graph model does not contain direct `(:PublishedActivity)-[:FINANCIAL_TRANSACTION]->(:PublishedActivity)` relationships. Financial transactions appear to link Organisations (Published/Phantom) to the PublishedActivities involved, typically representing the recipient organisation receiving funds *to* the activity, or the provider organisation disbursing funds *to* the activity. This needs to be considered when defining and querying funding chains.~~ Implemented a new `FUNDS` relationship that creates direct connections between activities, aggregating all relevant transaction types into a single directional relationship.

- **Add Activity Hierarchy Relationships:** Create a new table and relationships in Neo4j that capture parent/child/sibling connections between activities. These need to be implemented efficiently with one row per relationship (parent-child, sibling-sibling). The edge should include attributes that record how the relationship was established (which activity declared the relationship). The model should account for both directional (parent-child) and reflexive (sibling) relationships, and should consider both published and phantom activities.

- **Add Activity Participation Relationships:** Implement a new "participating activity" relationship in Neo4j to capture where an activity is declared as the `activityid` in a participatingorg row. This will potentially duplicate some financial relationships but will serve as a useful checksum. Implementation should involve:
  1. Creating a new DBT model to identify these relationships
  2. Writing a separate Python script to load them into Neo4j
  3. Ensuring the relationship captures links between both published and phantom activities 