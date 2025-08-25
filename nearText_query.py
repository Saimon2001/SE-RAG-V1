import weaviate, json
from weaviate.classes.query import MetadataQuery

with weaviate.connect_to_local() as client:
    docs = client.collections.get("Project")

    resp = docs.query.near_text(
        query="propietario del proyecto PMIS_GatesMRI_Implementación",
        limit=3,
        return_metadata=MetadataQuery(distance=True),
    )

    for obj in resp.objects:
        print(obj.properties["project_id"], obj.metadata.distance)
        print(obj.properties["text"][:400], "\n---\n")