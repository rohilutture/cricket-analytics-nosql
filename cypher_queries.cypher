
// Neo4j: useful queries

// a) Bowler vs Batter record
MATCH (bat:Player {name:$batter})-[r:FACED]->(bow:Player {name:$bowler})
RETURN count(r) AS balls,
       sum(r.runs) AS runs,
       sum(CASE WHEN r.isWicket THEN 1 ELSE 0 END) AS outs;

// b) Toughest bowlers for a batter (min 30 balls)
MATCH (bat:Player {name:$batter})-[r:FACED]->(bow:Player)
WITH bow, count(r) AS balls, sum(r.runs) AS runs, sum(CASE WHEN r.isWicket THEN 1 ELSE 0 END) AS outs
WHERE balls >= 30
RETURN bow.name AS bowler, balls, runs, (toFloat(runs)/balls)*100 AS strikeRate, outs
ORDER BY strikeRate ASC, outs DESC
LIMIT 10;

// c) Partnership proxy: players frequently facing the same bowler (example pattern)
MATCH (a:Player)-[r:FACED]->(bow:Player)<-[s:FACED]-(b:Player)
WHERE a <> b AND r.team = $team AND s.team = $team
WITH a,b, count(*) AS co_appearances
WHERE co_appearances >= 20
RETURN a.name, b.name, co_appearances
ORDER BY co_appearances DESC
LIMIT 20;

// d) Graph Data Science setup
CALL gds.graph.project('duels','Player','FACED', {relationshipProperties:['runs','isWicket']});

// e) PageRank (central batters in duels network)
CALL gds.pageRank.stream('duels')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS player, score
ORDER BY score DESC LIMIT 20;
