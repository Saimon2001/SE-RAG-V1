import weaviate, json
from weaviate.classes.query import Filter

PROJECT_ID = "d794cd22-2caa-ec11-826a-00090faa0001"

with weaviate.connect_to_local() as client:
    docs = client.collections.get("Project")

    resp = docs.query.near_text(
        query="la tarea con el task_id dd94cd22-2caa-ec11-826a-00090faa0001, es critica?",
        limit=5,
        filters=Filter.by_property("project_id").equal(PROJECT_ID),
    )

    for obj in resp.objects:
        print(obj.properties["task_id"], obj.properties["assignment_string"][:200])
