import json
import uuid
import weaviate
from weaviate.classes.config import Property, DataType, Configure

# ---------------- Config ----------------
JSON_PATH = "Data/new_nested_projects10.json"
COLLECTION_NAME = "Project"      # denormalized RAG collection
RESET_COLLECTION = True          # set False to keep existing data

OLLAMA_ENDPOINT = "http://host.docker.internal:11434"
EMBED_MODEL = "nomic-embed-text"
GEN_MODEL = "llama3.2"

def safe(s):
    return s if isinstance(s, str) else ("" if s is None else str(s))

def build_text(project_string, assignment_string, task_string, resource_string):
    parts = [
        "PROYECTO:\n",   safe(project_string),    "\n\n",
        "ASIGNACIÓN:\n", safe(assignment_string), "\n\n",
        "TAREA:\n",      safe(task_string),       "\n\n",
        "RECURSO:\n",    safe(resource_string)
    ]
    return "".join(parts).strip()

def deterministic_uuid(project_id, task_id, resource_id, assignment_string):
    base = f"{safe(project_id)}|{safe(task_id)}|{safe(resource_id)}|{safe(assignment_string)[:128]}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))

with weaviate.connect_to_local() as client:
    # list_all() can be list[str], list[objects], or dict-like; normalize to a set of names
    raw = client.collections.list_all()
    if isinstance(raw, dict):
        existing = set(raw.keys())
    else:
        existing = {getattr(x, "name", x) for x in raw}

    # Reset / create collection
    if RESET_COLLECTION and COLLECTION_NAME in existing:
        client.collections.delete(COLLECTION_NAME)
        existing.remove(COLLECTION_NAME)

    if COLLECTION_NAME not in existing:
        client.collections.create(
            name=COLLECTION_NAME,
            vector_config=Configure.Vectors.text2vec_ollama(
                api_endpoint=OLLAMA_ENDPOINT,
                model=EMBED_MODEL,
            ),
            generative_config=Configure.Generative.ollama(
                api_endpoint=OLLAMA_ENDPOINT,
                model=GEN_MODEL,
            ),
            properties=[
                Property(name="text",        data_type=DataType.TEXT),
                Property(name="project_id",  data_type=DataType.TEXT),
                Property(name="task_id",     data_type=DataType.TEXT),
                Property(name="resource_id", data_type=DataType.TEXT),
                Property(name="project_string",    data_type=DataType.TEXT),
                Property(name="assignment_string", data_type=DataType.TEXT),
                Property(name="task_string",       data_type=DataType.TEXT),
                Property(name="resource_string",   data_type=DataType.TEXT),
            ],
        )

    docs = client.collections.get(COLLECTION_NAME)

    # Load JSON
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        projects_json = json.load(f)

    # Ingest (batched)
    errors = 0
    with docs.batch.fixed_size(batch_size=200, concurrent_requests=2) as batch:
        for proj in projects_json:
            project_id = proj.get("project_id")
            project_string = proj.get("project_string", "")

            for a in proj.get("assignments", []):
                assignment_string = a.get("assignment_string", "")

                task = a.get("task") or {}
                resource = a.get("resource") or {}

                task_id = task.get("task_id")
                task_string = task.get("task_string", "")
                resource_id = resource.get("resource_id")
                resource_string = resource.get("resource_string", "")

                text = build_text(project_string, assignment_string, task_string, resource_string)
                uid = deterministic_uuid(project_id, task_id, resource_id, assignment_string)

                batch.add_object(
                    properties={
                        "text": text,
                        "project_id": safe(project_id),
                        "task_id": safe(task_id),
                        "resource_id": safe(resource_id),
                        "project_string": safe(project_string),
                        "assignment_string": safe(assignment_string),
                        "task_string": safe(task_string),
                        "resource_string": safe(resource_string),
                    },
                    uuid=uid,
                )

            if batch.number_errors > 0:
                errors += batch.number_errors
                if errors > 10:
                    print("Batch import stopped due to excessive errors.")
                    break

    failed = docs.batch.failed_objects
    if failed:
        print(f"Number of failed imports: {len(failed)}")
        print(f"First failed object: {failed[0]}")

#client.close()  # no need it was sent with a with style.