import weaviate
import json

client = weaviate.connect_to_local()

with open("Data/proj_data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

projects = client.collections.get("ProjectV4")

with projects.batch.fixed_size(batch_size=200) as batch:
    for d in data:
        batch.add_object(
            {
                "string_project": d["string_project"],
                "string_task": d["string_task"],
            },
            uuid=d["ProjectId"] #object's unique ID #not added as a string but as a id
        )
        if batch.number_errors > 10:
            print("Batch import stopped due to excessive errors.")
            break

failed_objects = projects.batch.failed_objects
if failed_objects:
    print(f"Number of failed imports: {len(failed_objects)}")
    print(f"First failed object: {failed_objects[0]}")

client.close()  # Free up resources