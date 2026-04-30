# brainstorm_graperank_algorithm_java

docker build -t graperank .

docker run -d -e REDIS_HOST=host.docker.internal   -e REDIS_PORT=6379  -e NEO4J_URL=neo4j://host.docker.internal:7687 -e  NEO4J_USERNAME=neo4j -e NEO4J_PASSWORD=password  graperank

Default scoring parameters:

- `GRAPERANK_ATTENUATION_FACTOR=0.85`
- `GRAPERANK_RIGOR=0.5`
- `GRAPERANK_RELEVANT_MAX_HOPS=992`
- `GRAPERANK_DISPLAY_MAX_HOPS=8`
