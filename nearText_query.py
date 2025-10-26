import weaviate, json
from weaviate.classes.query import MetadataQuery

with weaviate.connect_to_local() as client:
    docs = client.collections.get("ProjectV4")

    resp = docs.query.near_text(
        query="variación de: -4.600000",
        limit=3,
        return_metadata=MetadataQuery(distance=True),
    )

    for obj in resp.objects:
        print(json.dumps(obj.properties, indent=2))